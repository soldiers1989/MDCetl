import pandas as pd
import datetime

class misc_funcs():

    def __init__(self):
        pass

    @classmethod
    def write_to_xl(self, df, filename, out_path, sheetname="Sheet1"):

        # now = datetime.datetime.now()
        # timestamp = now.strftime("%b-%d-%Y-%I%M%p")
        timestamp = ''
        filename = out_path + filename + "-" + timestamp

        writer = pd.ExcelWriter(filename + ".xlsx", engine='xlsxwriter')
        df.to_excel(writer, sheet_name=sheetname, index=False)  # send df to writer
        worksheet = writer.sheets[sheetname]  # pull worksheet object
        for idx, col in enumerate(df):  # loop through all columns
            series = df[col]
            max_len = max((
                series.astype(str).map(len).max(),  # len of largest item
                len(str(series.name))  # len of column name/header
            ))
            worksheet.set_column(idx, idx, max_len)  # set column width
        writer.save()
