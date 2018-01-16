import os
import uuid
import pandas as pd

from Shared.common import Common as COM
from Shared.enums import FileType as FT
from Shared.enums import DataSource as DS
from Shared.enums import WorkSheet as WS, SourceSystemType as SS, FileType


class FileService:

	def __init__(self, path):
		self.path = os.path.join(os.path.expanduser("~"), path)
		self.source_file = None
		self._set_folder()

	def _set_folder(self):
		os.chdir(self.path)
		self.source_file = os.listdir(self.path)

	def show_source_file(self):
		file_list = [f for f in self.source_file if f[0:2] != '~$' and (f[-3:] in FileType.SPREAD_SHEET.value or f[-4:] in FileType.SPREAD_SHEET.value)]
		COM.print_list(file_list)

	def get_source_file(self):
		return [f for f in self.source_file if f[0:2] != '~$' and (f[-3:] in FileType.SPREAD_SHEET.value or f[-4:] in FileType.SPREAD_SHEET.value)]

	def read_source_file(self, ftype, data_source):
		l_program = []
		l_program_youth = []
		l_company = []
		l_company_annual = []

		file_list = [f for f in self.source_file if f[0:2] != '~$' and (f[-3:] in FileType.SPREAD_SHEET.value or f[-4:] in FileType.SPREAD_SHEET.value)]
		if ftype == FT.SPREAD_SHEET.value:
			if data_source == DS.BAP:
				for fl in file_list:
					print(fl)
					prg = pd.read_excel(fl, WS.bap_program.value)
					prg_youth = pd.read_excel(fl, WS.bap_program_youth.value)
					com = pd.read_excel(fl, WS.bap_company.value)
					com_a = pd.read_excel(fl, WS.bap_company_annual.value)

					ds = COM.set_datasource(str(fl))
					FileService.data_system_source(prg, prg_youth, com, com_a, os.getcwd(), str(fl), ds)

					l_program.append(prg)
					l_program_youth.append(prg_youth)
					l_company.append(com)
					l_company_annual.append(com_a)

				bap_program = pd.concat(l_program)
				bap_program_youth = pd.concat(l_program_youth)
				bap_company = pd.concat(l_company)
				bap_company_annual = pd.concat(l_company_annual)

				return bap_program, bap_program_youth, bap_company, bap_company_annual

		elif ftype == FT.CSV:
			for fl in file_list:
				data_list = pd.read_csv(fl)
			return data_list

	def save_as_excel(self, dfs, file_name, path_key):
		print(os.getcwd())
		print(len(dfs))
		path = COM.get_config('config.ini', 'box_file_path', path_key)
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

	def save_as_csv(self, df, file_name, path, sheet_name='SheetI'):
		os.chdir(path)
		writer = pd.ExcelWriter(file_name)
		df.to_excel(writer, sheet_name, index=False)
		writer.save()

	@staticmethod
	def data_system_source(cv, cvy, cd, cda, path, file_name, datasource):

		cv.insert(0, 'SourceSystem', SS.RICPD_bap.value)
		cv.insert(0, 'DataSource', datasource)
		cv.insert(0, 'Path', path)
		cv.insert(0, 'FileName', file_name)
		cv.insert(0, 'FileID', str(uuid.uuid4()))
		cv.insert(0, 'BatchID', '-')

		cvy.insert(0, 'SourceSystem', SS.RICPD_bap.value)
		cvy.insert(0, 'DataSource', datasource)
		cvy.insert(0, 'Path', path)
		cvy.insert(0, 'FileName', file_name)
		cvy.insert(0, 'FileID', str(uuid.uuid4()))
		cvy.insert(0, 'BatchID', '-')

		cd.insert(0, 'SourceSystem', SS.RICCD_bap.value)
		cd.insert(0, 'DataSource', datasource)
		cd.insert(0, 'Path', path)
		cd.insert(0, 'FileName', file_name)
		cd.insert(0, 'FileID', str(uuid.uuid4()))
		cd.insert(0, 'BatchID', '-')
		cd.insert(0, 'CompanyID', '-')

		if len(cda) > 0:
			cda.insert(0, 'SourceSystem', SS.RICACD_bap.value)
			cda.insert(0, 'DataSource', datasource)
			cda.insert(0, 'Path', path)
			cda.insert(0, 'FileName', file_name)
			cda.insert(0, 'FileID', str(uuid.uuid4()))
			cda.insert(0, 'BatchID', '-')
			cda.insert(0, 'CompanyID', '-')

