import numpy as np
from Shared.datasource import DataSource
from Dashboard.data.constants import SQL, Category
import Shared.enums as enum


class ChartData(DataSource):
    def __init__(self):
        super().__init__('', '', datasource=enum.DataSourceType.NO_SOURCE)
        self.sql = ''

    def bap_data(self, category, types=None):
        if types is None:
            if category == Category.DataSource:
                self.sql = SQL.bap_categorical.value.format('MDCDim.dbo.DimDataSource', 'DataSource')
            elif category == Category.Industry:
                self.sql = SQL.bap_categorical.value.format('MDCDim.dbo.DimIndustry', 'IndustrySector')
            elif category == Category.Stage:
                self.sql = SQL.bap_categorical.value.format('MDCDim.dbo.DimStage', 'Stage')
        else:
            if category == Category.DataSource and types == Category.Potential:
                self.sql = SQL.bap_types.value.format('HighPotential', 'MDCDim.dbo.DimDataSource', 'DataSource', 'HighPotential')
        self.data = self.db.pandas_read(self.sql)
        return self.data


if __name__ == '__main__':
    ch = ChartData()
    x = ch.bap_data(Category.DataSource, Category.Potential)
    print(x.head(25))


