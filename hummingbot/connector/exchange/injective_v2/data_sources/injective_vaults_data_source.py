import asyncio
import base64
import json
import re
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Tuple

from bidict import bidict
from google.protobuf import any_pb2, json_format
from pyinjective import Transaction
from pyinjective.async_client import AsyncClient
from pyinjective.composer import Composer, injective_exchange_tx_pb
from pyinjective.constant import Network
from pyinjective.orderhash import OrderHashManager
from pyinjective.wallet import Address, PrivateKey

from hummingbot.connector.exchange.injective_v2 import injective_constants as CONSTANTS
from hummingbot.connector.exchange.injective_v2.data_sources.injective_data_source import InjectiveDataSource
from hummingbot.connector.exchange.injective_v2.injective_market import InjectiveSpotMarket, InjectiveToken
from hummingbot.connector.exchange.injective_v2.injective_query_executor import PythonSDKInjectiveQueryExecutor
from hummingbot.connector.gateway.common_types import PlaceOrderResult
from hummingbot.connector.gateway.gateway_in_flight_order import GatewayInFlightOrder
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.async_throttler_base import AsyncThrottlerBase
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate
from hummingbot.core.pubsub import PubSub
from hummingbot.logger import HummingbotLogger


class InjectiveVaultsDataSource(InjectiveDataSource):
    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            private_key: str,
            subaccount_index: int,
            vault_contract_address: str,
            vault_subaccount_index: int,
            network: Network,
            use_secure_connection: bool = True):
        self._network = network
        self._client = AsyncClient(
            network=self._network,
            insecure=not use_secure_connection,
            chain_cookie_location=self._chain_cookie_file_path(),
        )
        self._composer = Composer(network=self._network.string())
        self._query_executor = PythonSDKInjectiveQueryExecutor(sdk_client=self._client)

        self._private_key = None
        self._public_key = None
        self._vault_admin_address = ""
        self._vault_admin_subaccount_index = subaccount_index
        self._vault_admin_subaccount_id = ""
        if private_key:
            self._private_key = PrivateKey.from_hex(private_key)
            self._public_key = self._private_key.to_public_key()
            self._vault_admin_address = self._public_key.to_address()
            self._vault_admin_subaccount_id = self._vault_admin_address.get_subaccount_id(index=subaccount_index)

        self._vault_contract_address = None
        self._vault_subaccount_id = ""
        self._vault_subaccount_index = vault_subaccount_index
        if vault_contract_address:
            self._vault_contract_address = Address.from_acc_bech32(vault_contract_address)
            self._vault_subaccount_id = self._vault_contract_address.get_subaccount_id(index=vault_subaccount_index)

        self._order_hash_manager: Optional[OrderHashManager] = None
        self._publisher = PubSub()
        self._last_received_message_time = 0
        self._order_creation_lock = asyncio.Lock()
        # We create a throttler instance here just to have a fully valid instance from the first moment.
        # The connector using this data source should replace the throttler with the one used by the connector.
        self._throttler = AsyncThrottler(rate_limits=CONSTANTS.RATE_LIMITS)

        self._is_timeout_height_initialized = False
        self._is_trading_account_initialized = False
        self._markets_initialization_lock = asyncio.Lock()
        self._market_info_map: Optional[Dict[str, InjectiveSpotMarket]] = None
        self._market_and_trading_pair_map: Optional[Mapping[str, str]] = None
        self._tokens_map: Optional[Dict[str, InjectiveToken]] = None
        self._token_symbol_symbol_and_denom_map: Optional[Mapping[str, str]] = None

        self._events_listening_tasks: List[asyncio.Task] = []

    @property
    def publisher(self):
        return self._publisher

    @property
    def query_executor(self):
        return self._query_executor

    @property
    def composer(self) -> Composer:
        return self._composer

    @property
    def order_creation_lock(self) -> asyncio.Lock:
        return self._order_creation_lock

    @property
    def throttler(self):
        return self._throttler

    @property
    def portfolio_account_injective_address(self) -> str:
        return self._vault_contract_address.to_acc_bech32()

    @property
    def portfolio_account_subaccount_id(self) -> str:
        return self._vault_subaccount_id

    @property
    def trading_account_injective_address(self) -> str:
        return self._vault_admin_address.to_acc_bech32()

    @property
    def injective_chain_id(self) -> str:
        return self._network.chain_id

    @property
    def fee_denom(self) -> str:
        return self._network.fee_denom

    @property
    def portfolio_account_subaccount_index(self) -> int:
        return self._vault_subaccount_index

    @property
    def network_name(self) -> str:
        return self._network.string()

    def events_listening_tasks(self) -> List[asyncio.Task]:
        return self._events_listening_tasks.copy()

    def add_listening_task(self, task: asyncio.Task):
        self._events_listening_tasks.append(task)

    async def timeout_height(self) -> int:
        if not self._is_timeout_height_initialized:
            await self._initialize_timeout_height()
        return self._client.timeout_height

    async def market_and_trading_pair_map(self):
        if self._market_and_trading_pair_map is None:
            async with self._markets_initialization_lock:
                if self._market_and_trading_pair_map is None:
                    await self.update_markets()
        return self._market_and_trading_pair_map.copy()

    async def market_info_for_id(self, market_id: str):
        if self._market_info_map is None:
            async with self._markets_initialization_lock:
                if self._market_info_map is None:
                    await self.update_markets()

        return self._market_info_map[market_id]

    async def trading_pair_for_market(self, market_id: str):
        if self._market_and_trading_pair_map is None:
            async with self._markets_initialization_lock:
                if self._market_and_trading_pair_map is None:
                    await self.update_markets()

        return self._market_and_trading_pair_map[market_id]

    async def market_id_for_trading_pair(self, trading_pair: str) -> str:
        if self._market_and_trading_pair_map is None:
            async with self._markets_initialization_lock:
                if self._market_and_trading_pair_map is None:
                    await self.update_markets()

        return self._market_and_trading_pair_map.inverse[trading_pair]

    async def all_markets(self):
        if self._market_info_map is None:
            async with self._markets_initialization_lock:
                if self._market_info_map is None:
                    await self.update_markets()

        return list(self._market_info_map.values())

    async def token(self, denom: str) -> InjectiveToken:
        if self._tokens_map is None:
            async with self._markets_initialization_lock:
                if self._tokens_map is None:
                    await self.update_markets()

        return self._tokens_map.get(denom)

    def configure_throttler(self, throttler: AsyncThrottlerBase):
        self._throttler = throttler

    async def trading_account_sequence(self) -> int:
        if not self._is_trading_account_initialized:
            await self.initialize_trading_account()
        return self._client.get_sequence()

    async def trading_account_number(self) -> int:
        if not self._is_trading_account_initialized:
            await self.initialize_trading_account()
        return self._client.get_number()

    async def stop(self):
        await super().stop()
        self._events_listening_tasks = []

    async def initialize_trading_account(self):
        await self._client.get_account(address=self.trading_account_injective_address)
        self._is_trading_account_initialized = True

    async def update_markets(self):
        self._tokens_map = {}
        self._token_symbol_symbol_and_denom_map = bidict()
        markets = await self._query_executor.spot_markets(status="active")
        markets_map = {}
        market_id_to_trading_pair = bidict()

        for market_info in markets:
            try:
                ticker_base, ticker_quote = market_info["ticker"].split("/")
                base_token = self._token_from_market_info(
                    denom=market_info["baseDenom"],
                    token_meta=market_info["baseTokenMeta"],
                    candidate_symbol=ticker_base,
                )
                quote_token = self._token_from_market_info(
                    denom=market_info["quoteDenom"],
                    token_meta=market_info["quoteTokenMeta"],
                    candidate_symbol=ticker_quote,
                )
                market = InjectiveSpotMarket(
                    market_id=market_info["marketId"],
                    base_token=base_token,
                    quote_token=quote_token,
                    market_info=market_info
                )
                market_id_to_trading_pair[market.market_id] = market.trading_pair()
                markets_map[market.market_id] = market
            except KeyError:
                self.logger().debug(f"The market {market_info['marketId']} will be excluded because it could not be "
                                    f"parsed ({market_info})")
                continue

        self._market_info_map = markets_map
        self._market_and_trading_pair_map = market_id_to_trading_pair

    async def order_updates_for_transaction(
            self, transaction_hash: str, transaction_orders: List[GatewayInFlightOrder]
    ) -> List[OrderUpdate]:
        order_updates = []

        async with self.throttler.execute_task(limit_id=CONSTANTS.GET_TRANSACTION_LIMIT_ID):
            transaction_info = await self.query_executor.get_tx_by_hash(tx_hash=transaction_hash)

        transaction_messages = json.loads(base64.b64decode(transaction_info["data"]["messages"]).decode())
        transaction_spot_orders = transaction_messages[0]["value"]["msg"]["admin_execute_message"]["injective_message"]["custom"]["msg_data"]["batch_update_orders"]["spot_orders_to_create"]
        transaction_logs = json.loads(base64.b64decode(transaction_info["data"]["logs"]).decode())
        batch_orders_message_event = next(
            (event for event in transaction_logs[0].get("events", []) if event.get("type") == "wasm"),
            {}
        )
        response = next(
            (attribute.get("value", "")
             for attribute in batch_orders_message_event.get("attributes", [])
             if attribute.get("key") == "batch_update_orders_response"), "")
        spot_order_hashes_match = re.search(r"spot_order_hashes: (\[.*?\])", response)
        if spot_order_hashes_match is not None:
            spot_order_hashes_text = spot_order_hashes_match.group(1)
        else:
            spot_order_hashes_text = ""
        spot_order_hashes = re.findall(r"[\"'](0x\w+)[\"']", spot_order_hashes_text)

        for order_info, order_hash in zip(transaction_spot_orders, spot_order_hashes):
            market = await self.market_info_for_id(market_id=order_info["market_id"])
            price = market.price_from_chain_format(chain_price=Decimal(order_info["order_info"]["price"]))
            amount = market.quantity_from_chain_format(chain_quantity=Decimal(order_info["order_info"]["quantity"]))
            trade_type = TradeType.BUY if order_info["order_type"] in [1, 7, 9] else TradeType.SELL
            for transaction_order in transaction_orders:
                market_id = await self.market_id_for_trading_pair(trading_pair=transaction_order.trading_pair)
                if (market_id == order_info["market_id"]
                        and transaction_order.amount == amount
                        and transaction_order.price == price
                        and transaction_order.trade_type == trade_type):
                    new_state = OrderState.OPEN if transaction_order.is_pending_create else transaction_order.current_state
                    order_update = OrderUpdate(
                        trading_pair=transaction_order.trading_pair,
                        update_timestamp=self._time(),
                        new_state=new_state,
                        client_order_id=transaction_order.client_order_id,
                        exchange_order_id=order_hash,
                    )
                    transaction_orders.remove(transaction_order)
                    order_updates.append(order_update)
                    self.logger().debug(
                        f"Exchange order id found for order {transaction_order.client_order_id} ({order_update})"
                    )
                    break

        return order_updates

    def real_tokens_trading_pair(self, unique_trading_pair: str) -> str:
        resulting_trading_pair = unique_trading_pair
        if (self._market_and_trading_pair_map is not None
                and self._market_info_map is not None):
            market_id = self._market_and_trading_pair_map.inverse.get(unique_trading_pair)
            market = self._market_info_map.get(market_id)
            if market is not None:
                resulting_trading_pair = combine_to_hb_trading_pair(
                    base=market.base_token.symbol,
                    quote=market.quote_token.symbol,
                )

        return resulting_trading_pair

    async def _initialize_timeout_height(self):
        await self._client.sync_timeout_height()
        self._is_timeout_height_initialized = True

    def _reset_order_hash_manager(self):
        raise NotImplementedError

    def _sign_and_encode(self, transaction: Transaction) -> bytes:
        sign_doc = transaction.get_sign_doc(self._public_key)
        sig = self._private_key.sign(sign_doc.SerializeToString())
        tx_raw_bytes = transaction.get_tx_data(sig, self._public_key)
        return tx_raw_bytes

    def _uses_default_portfolio_subaccount(self) -> bool:
        return self._vault_subaccount_index == CONSTANTS.DEFAULT_SUBACCOUNT_INDEX

    def _token_from_market_info(self, denom: str, token_meta: Dict[str, Any], candidate_symbol: str) -> InjectiveToken:
        token = self._tokens_map.get(denom)
        if token is None:
            unique_symbol = token_meta["symbol"]
            if unique_symbol in self._token_symbol_symbol_and_denom_map:
                if candidate_symbol not in self._token_symbol_symbol_and_denom_map:
                    unique_symbol = candidate_symbol
                else:
                    unique_symbol = token_meta["name"]
            token = InjectiveToken(
                denom=denom,
                symbol=token_meta["symbol"],
                unique_symbol=unique_symbol,
                name=token_meta["name"],
                decimals=token_meta["decimals"]
            )
            self._tokens_map[denom] = token
            self._token_symbol_symbol_and_denom_map[unique_symbol] = denom

        return token

    async def _last_traded_price(self, market_id: str) -> Decimal:
        async with self.throttler.execute_task(limit_id=CONSTANTS.SPOT_TRADES_LIMIT_ID):
            trades_response = await self.query_executor.get_spot_trades(
                market_ids=[market_id],
                limit=1,
            )

        price = Decimal("nan")
        if len(trades_response["trades"]) > 0:
            market = await self.market_info_for_id(market_id=market_id)
            price = market.price_from_chain_format(chain_price=Decimal(trades_response["trades"][0]["price"]["price"]))

        return price

    def _calculate_order_hashes(self, orders: List[GatewayInFlightOrder]) -> List[str]:
        raise NotImplementedError

    def _order_book_updates_stream(self, market_ids: List[str]):
        stream = self._query_executor.spot_order_book_updates_stream(market_ids=market_ids)
        return stream

    def _public_trades_stream(self, market_ids: List[str]):
        stream = self._query_executor.public_spot_trades_stream(market_ids=market_ids)
        return stream

    def _subaccount_balance_stream(self):
        stream = self._query_executor.subaccount_balance_stream(subaccount_id=self.portfolio_account_subaccount_id)
        return stream

    def _subaccount_orders_stream(self, market_id: str):
        stream = self._query_executor.subaccount_historical_spot_orders_stream(
            market_id=market_id, subaccount_id=self.portfolio_account_subaccount_id
        )
        return stream

    def _transactions_stream(self):
        stream = self._query_executor.transactions_stream()
        return stream

    async def _order_creation_message(
            self, spot_orders_to_create: List[GatewayInFlightOrder]
    ) -> Tuple[any_pb2.Any, List[str]]:
        composer = self.composer
        order_definitions = []

        for order in spot_orders_to_create:
            order_definition = await self._create_spot_order_definition(order=order)
            order_definitions.append(order_definition)

        message = composer.MsgBatchUpdateOrders(
            sender=self.portfolio_account_injective_address,
            spot_orders_to_create=order_definitions,
        )

        message_as_dictionary = json_format.MessageToDict(
            message=message,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
            use_integers_for_enums=True,
        )
        del message_as_dictionary["subaccount_id"]

        execute_message_parameter = self._create_execute_contract_internal_message(batch_update_orders_params=message_as_dictionary)

        execute_contract_message = composer.MsgExecuteContract(
            sender=self._vault_admin_address.to_acc_bech32(),
            contract=self._vault_contract_address.to_acc_bech32(),
            msg=json.dumps(execute_message_parameter),
        )

        return execute_contract_message, []

    def _order_cancel_message(self, spot_orders_to_cancel: List[injective_exchange_tx_pb.OrderData]) -> any_pb2.Any:
        composer = self.composer

        message = composer.MsgBatchUpdateOrders(
            sender=self.portfolio_account_injective_address,
            spot_orders_to_cancel=spot_orders_to_cancel,
        )

        message_as_dictionary = json_format.MessageToDict(
            message=message,
            including_default_value_fields=True,
            preserving_proto_field_name=True,
            use_integers_for_enums=True,
        )
        del message_as_dictionary["subaccount_id"]

        execute_message_parameter = self._create_execute_contract_internal_message(batch_update_orders_params=message_as_dictionary)

        execute_contract_message = composer.MsgExecuteContract(
            sender=self._vault_admin_address.to_acc_bech32(),
            contract=self._vault_contract_address.to_acc_bech32(),
            msg=json.dumps(execute_message_parameter),
        )

        return execute_contract_message

    async def _generate_injective_order_data(self, order: GatewayInFlightOrder) -> injective_exchange_tx_pb.OrderData:
        market_id = await self.market_id_for_trading_pair(trading_pair=order.trading_pair)
        order_data = self.composer.OrderData(
            market_id=market_id,
            subaccount_id=str(self.portfolio_account_subaccount_index),
            order_hash=order.exchange_order_id,
            order_direction="buy" if order.trade_type == TradeType.BUY else "sell",
            order_type="market" if order.order_type == OrderType.MARKET else "limit",
        )

        return order_data

    async def _create_spot_order_definition(self, order: GatewayInFlightOrder):
        # Both price and quantity have to be adjusted because the vaults expect to receive those values without
        # the extra 18 zeros that the chain backend expectes for direct trading messages
        market_id = await self.market_id_for_trading_pair(order.trading_pair)
        definition = self.composer.SpotOrder(
            market_id=market_id,
            subaccount_id=str(self.portfolio_account_subaccount_index),
            fee_recipient=self.portfolio_account_injective_address,
            price=order.price,
            quantity=order.amount,
            is_buy=order.trade_type == TradeType.BUY,
            is_po=order.order_type == OrderType.LIMIT_MAKER
        )

        definition.order_info.quantity = f"{(Decimal(definition.order_info.quantity) * Decimal('1e-18')).normalize():f}"
        definition.order_info.price = f"{(Decimal(definition.order_info.price) * Decimal('1e-18')).normalize():f}"
        return definition

    def _place_order_results(
            self,
            orders_to_create: List[GatewayInFlightOrder],
            order_hashes: List[str],
            misc_updates: Dict[str, Any],
            exception: Optional[Exception] = None,
    ) -> List[PlaceOrderResult]:
        return [
            PlaceOrderResult(
                update_timestamp=self._time(),
                client_order_id=order.client_order_id,
                exchange_order_id=None,
                trading_pair=order.trading_pair,
                misc_updates=misc_updates,
                exception=exception
            ) for order in orders_to_create
        ]

    def _create_execute_contract_internal_message(self, batch_update_orders_params: Dict) -> Dict[str, Any]:
        return {
            "admin_execute_message": {
                "injective_message": {
                    "custom": {
                        "route": "exchange",
                        "msg_data": {
                            "batch_update_orders": batch_update_orders_params
                        }
                    }
                }
            }
        }