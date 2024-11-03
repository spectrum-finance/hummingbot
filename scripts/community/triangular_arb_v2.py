import os
from typing import Dict, List, Set

from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from hummingbot.strategy_v2.models.executor_actions import ExecutorAction


class TriangularArbV2Config(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []
    markets: Dict[str, Set[str]] = {}

    cex_connector_main: str = Field(
        default="kucoin",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter main CEX connector: ",
            prompt_on_new=True
        ))
    cex_connector_proxy: str = Field(
        default="binance",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter proxy CEX connector: ",
            prompt_on_new=True
        ))
    dex_connector: str = Field(
        default="splash",
        client_data=ClientFieldData(
            prompt=lambda e: "Enter DEX connector: ",
            prompt_on_new=True
        ))

    arb_asset: str = Field(default_factory="ERG")
    arb_asset_wrapped: str = Field(default_factory="rsERG")
    proxy_asset: str = Field(default_factory="ADA")
    stable_asset: str = Field(default_factory="USDT")


class TriangularArbV2(StrategyV2Base):

    @classmethod
    def init_markets(cls, config: TriangularArbV2Config):
        cls.markets = {config.cex_connector_main: {f"{config.arb_asset}-{config.stable_asset}"},
                       config.cex_connector_proxy: {f"{config.proxy_asset}-{config.stable_asset}"},
                       config.dex_connector: {f"{config.arb_asset_wrapped}-{config.proxy_asset}"}, }

    def __init__(self, connectors: Dict[str, ConnectorBase], config: TriangularArbV2Config):
        super().__init__(connectors, config)
        self.config = config

    def determine_executor_actions(self) -> List[ExecutorAction]:
        executor_actions = []
        return executor_actions
