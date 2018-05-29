import numpy as np
from Shared.datasource import DataSource
from Dashboard.data.constants import SQL, Category
import Shared.enums as enum


class ChartData(DataSource):
    def __init__(self):
        super().__init__('', '', datasource=enum.DataSourceType.NO_SOURCE)
        self.sql = ''

    def test_data(self):
        np.random.seed(56)
        x = np.linspace(1, 25, 25)
        y = np.random.randint(1,25,25)
        np.random.seed(43)
        z = np.linspace(1, 25, 25)
        l = np.random.randint(1,25,25)
        np.random.seed(29)
        m = np.linspace(1, 25, 25)
        n = np.random.randint(1,25,25)
        data = [x, y, z, l, m, n]
        return data

        # Annual Revenue | Number of Employees | Advisory Service | Volunteer Mentor Hours | Funding TO Date

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


