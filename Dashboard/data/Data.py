import numpy as np


class ChartData:
    def __init__(self):
        self.name = ''

    def test_data(self):
        np.random.seed(15)
        x = np.random.randint(1, 26, 25)
        y = np.random.randint(1, 26, 25)
        z = np.random.randint(1, 26, 25)
        l = np.random.randint(1, 26, 25)
        m = np.random.randint(1, 26, 25)
        n = np.random.randint(1, 26, 25)
        data = [x, y, z, l, m, n]
        return data

