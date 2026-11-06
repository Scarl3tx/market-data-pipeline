from abc import ABC, abstractmethod
import polars as pl


class HistoricalDataProvider(ABC):

    @abstractmethod
    def get_stock_history(
        self,
        symbol: str,
        start_date: str,
        end_date: str
    ) -> pl.DataFrame:
        pass