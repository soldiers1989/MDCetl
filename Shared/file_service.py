import os
import pandas as pd
from Shared.common import Common as COM
from Shared.enums import FileType as FT


class FileService:

    def __init__(self, path):
        self.path = path
        self.source_file = os.listdir(path)

    def show_source_file(self):
        files_list = []
        lst = [f for f in self.source_files]
        for l in lst:
            if l[0:2] != '~$':
                files_list.append(l)
        print('LIST OF AVAILABLE FILES IN {}: {}\n'.format(self.path, len(files_list)))
        print('\n'.join(files_list))

    def read_source_file(self, ftype):
        data_list = []
        file_list = [f for f in self.source_file]
        for fl in file_list:
            if fl[-3:] in ftype or fl[-4:] in ftype:
                if ftype == FT.SPREAD_SHEET:
                    excel = pd.ExcelFile(fl)
                    sheets = excel.sheet_names
                    for sh in sheets:
                        data_list.append(pd.DataFrame(excel.parse(sh)))
                elif ftype == FT.CSV:
                    data_list.append(pd.read_csv(fl))
                else:
                    data_list = []
        return data_list

    def save_as_excel(self, dfs, file_name, path_key):
        print(os.getcwd())
        print(len(dfs))
        path = COM.get_config('box_file_path', path_key)
        box_path = os.path.join(os.path.expanduser("~"), path)
        os.chdir(box_path)
        try:
            writer = pd.ExcelWriter(file_name)
            j = 0
            for df in dfs:
                j += j
                sheet_name = 'SHEET {}'.format(j)
                df.to_excel(writer, sheet_name, index=False)
            writer.save()
        except Exception as ex:
            print(ex)