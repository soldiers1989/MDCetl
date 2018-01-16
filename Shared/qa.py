import os
import pandas as pd
import datetime
import openpyxl
from openpyxl.styles import PatternFill
from Shared.file_service import FileService
from Shared.common import Common
from Shared.enums import FileType, WorkSheet as WS, PATH as p
import warnings


class BapQA:
	def __init__(self):
		box_path = BapQA.change_location(p.DATA)
		file = FileService(box_path)

		self.ric_files = file.get_source_file()

		self.missing = PatternFill(fgColor='F4B042', bgColor='C00000', fill_type='solid')
		self.amber = PatternFill(fgColor='F4B042', bgColor='C00000', fill_type='solid')
		self.okay = PatternFill(fgColor='E1F7DC', bgColor='C00000', fill_type='solid')
		self.wrong = PatternFill(fgColor='F44242', bgColor='C00000', fill_type='solid')

		warnings.filterwarnings("ignore")

		self.quarter = 'Q3'
		self.year = 2018
		self.youth = 'Youth'
		self.all_youth = 'ALL incl. youth'

	@staticmethod
	def change_location(loc):
		path = Common.get_config('config.ini', 'box_file_path', 'path_bap_source')
		path_qa = Common.get_config('config.ini', 'box_file_path', 'path_bap_qa')
		if loc == p.DATA:
			box_path = os.path.join(os.path.expanduser('~'), path)
			os.chdir(box_path)
			return box_path
		elif loc == p.QA:
			qa_path = os.path.join(os.path.expanduser('~'), path_qa)
			os.chdir(qa_path)
			return None

	def check_columns_completeness(self):
		dfps = pd.DataFrame()
		dfpys = pd.DataFrame()
		dfqc = pd.DataFrame()
		dfac = pd.DataFrame()

		for fl in self.ric_files:
			BapQA.change_location(p.DATA)
			wb = openpyxl.load_workbook(fl, data_only=True)
			file = fl[:-5]

			program_sheet = wb.get_sheet_by_name(WS.bap_program.value)
			df_ps = self.sheet_columns(program_sheet, file, WS.bap_program.value)
			program_youth_sheet = wb.get_sheet_by_name(WS.bap_program_youth.value)
			df_pys = self.sheet_columns(program_youth_sheet, file, WS.bap_program_youth.value)
			quarterly_company_sheet = wb.get_sheet_by_name(WS.bap_company.value)
			df_qc = self.sheet_columns(quarterly_company_sheet, file, WS.bap_company.value)
			annual_company_sheet = wb.get_sheet_by_name(WS.bap_company_annual.value)
			df_ac = self.sheet_columns(annual_company_sheet, file, WS.bap_company_annual.value)


			dfps = pd.concat([dfps, df_ps])
			dfpys = pd.concat([dfpys, df_pys])
			dfqc = pd.concat([dfqc, df_qc])
			dfac = pd.concat([dfac, df_ac])

		writer = pd.ExcelWriter('RIC_BAP_COLUMNS_FY18_Q3.xlsx')
		dfps.to_excel(writer, 'Program', index=False)
		dfpys.to_excel(writer, 'Program Youth', index=False)
		dfqc.to_excel(writer, 'Quarterly Company', index=False)
		dfac.to_excel(writer, 'Annual Company', index=False)

		BapQA.change_location(p.QA)
		writer.save()

	def check_rics_file(self):
		print('-' * 100, '\nCHECKING RIC FILES COLUMNS\n')
		self.check_columns_completeness()
		print('-' * 100, '\nPROCESSING RIC FILES\n')
		for fl in self.ric_files:
			print(fl)
			try:
				BapQA.change_location(p.DATA)
				wb = openpyxl.load_workbook(fl, data_only=True)

				print('\tProgram Data')
				program_sheet = wb.get_sheet_by_name(WS.bap_program.value)
				self.rics_sheet(program_sheet)
				self.program_sheet(program_sheet)
				print('\tProgram Youth Data')
				program_youth_sheet = wb.get_sheet_by_name(WS.bap_program_youth.value)
				self.rics_sheet(program_youth_sheet)
				self.program_youth_sheet(program_youth_sheet)
				print('\tQuarterly Company Data')
				quarterly_company_sheet = wb.get_sheet_by_name(WS.bap_company.value)
				self.rics_sheet(quarterly_company_sheet)
				self.quarterly_company_data_sheet(quarterly_company_sheet)
				print('\tAnnual Company Data')
				annual_company_sheet = wb.get_sheet_by_name(WS.bap_company_annual.value)
				self.rics_sheet(annual_company_sheet)
				self.annual_company_data_sheet(annual_company_sheet)

				BapQA.change_location(p.QA)
				wb.save('{}_QA.xlsx'.format(fl[:-5]))
				print('\t{}_QA IS SAVED.'.format(fl[:-5]))
			except BaseException as ex:
				print(ex)

	def rics_sheet(self, sheet):
		for cl in sheet.columns:
			for c in cl:
				if c.row > 1:
					if c.value == '' or c.value is None:
						c.fill = self.missing

	def program_youth_sheet(self, sheet):
		for cl in sheet.columns:
			for c in cl:
				if c.row > 1:
					if c.column in ['A', 'B', 'C']:
						if isinstance(c.value, int):
							c.fill = self.okay
						else:
							c.fill = self.missing
					if c.column == 'D':
						if c.value.lower() != self.quarter.lower():
							c.fill = self.wrong
					if c.column == 'E':
						if c.value != self.year:
							c.fill = self.wrong
					if c.column == 'F':
						if c.value.lower() != self.youth.lower():
							c.fill = self.wrong

	def program_sheet(self, sheet):
		for cl in sheet.columns:
			for c in cl:
				if c.row > 1:
					if c.column in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'I', 'J', 'K', 'L', 'M', 'N']:
						if isinstance(c.value, int):
							c.fill = self.okay
						else:
							c.fill = self.missing
					if c.column == 'O':
						if c.value.lower() != self.quarter.lower():
							c.fill = self.amber
					if c.column == 'P':
						if c.value != self.year:
							c.fill = self.amber
					if c.column == 'Q':
						if c.value.lower() != self.all_youth.lower():
							c.fill = self.amber

	def quarterly_company_data_sheet(self, sheet):
		for cl in sheet.columns:
			for c in cl:
				try:
					if c.value is not None:
						if c.row > 1:
							if c.column in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'V', 'Z', 'H', 'I', 'J', 'K', 'L', 'T', 'U', 'V', 'X', 'Y', 'AA']:
								if len(str(c.value)) > 0:
									c.fill = self.okay
								else:
									c.fill = self.wrong
							if c.column == 'H,I':
								pass
							if c.column in ['W',  'AB', 'AC', 'AD', 'AF']:
								if isinstance(float(c.value), float):
									c.fill = self.okay
								else:
									c.fill = self.wrong
							if c.column in ['M', 'N']:
								if isinstance(c.value, int) or isinstance(c.value, datetime.date):
									c.fill = self.okay
								else:
									c.fill = self.wrong
							if c.column == 'N':
								if isinstance(c.value, int):
									c.fill = self.okay
								else:
									c.fill = self.wrong
							if c.column == 'AG':
								if c.value != self.quarter:
									c.fill = self.wrong
							if c.column == 'AH':
								if c.value != self.year:
									c.fill = self.wrong
								else:
									c.fill = self.missing
				except Exception as ex:
					print('{} | {} | {} | {}'.format(c.column, c.row, c.value, ex))

	def annual_company_data_sheet(self, sheet):
		for cl in sheet.columns:
			for c in cl:
				if c.value is not None:
					if c.row > 1:
						if c.column in ['A', 'B', 'C', 'D']:
							if len(c.value) > 0:
								c.fill = self.okay
							else:
								c.fill = self.amber
						if c.column in ['E', 'F', 'K']:
							if isinstance(c.value, float):
								c.fill = self.okay
							else:
								c.fill = self.amber
						if c.column in ['G', 'H', 'I', 'J']:
							if isinstance(c.value, int):
								c.fill = self.okay
							else:
								c.fill = self.amber
				else:
					c.fill = self.missing

	def sheet_columns(self, sheet, file, sheet_name):
		lst = []
		for rw in sheet.rows:
			for i in range(len(rw)):
				if rw[i].row == 1:
					d = dict()
					d['RIC'] = file
					d['Sheet'] = sheet_name
					d['Letter'] = rw[i].column
					d['Header'] = rw[i].value
					print(d)
					lst.append(d)
		return pd.DataFrame(lst, columns=lst[0].keys())
