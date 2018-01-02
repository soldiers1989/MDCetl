import os
import openpyxl
from openpyxl.styles import PatternFill
from Shared.common import Common
from Shared.enums import FileType, WorkSheet as WS, PATH as p
import warnings


class BapQA:
	def __init__(self):
		box_path = BapQA.change_location(p.DATA)
		file_source = os.listdir(box_path)
		self.ric_files = [f for f in file_source if f[0:2] != '~$' and (f[-3:] in FileType.SPREAD_SHEET.value or f[-4:] in FileType.SPREAD_SHEET.value)]
		Common.print_list(self.ric_files, '\n')
		self.missing = PatternFill(fgColor='F44242', bgColor='C00000', fill_type='solid')
		self.amber = PatternFill(fgColor='F4b042', bgColor='C00000', fill_type='solid')
		self.okay = PatternFill(fgColor='FFFFCC', bgColor='000000', fill_type='solid')

		warnings.filterwarnings("ignore")

		self.quarter = 'Q3'
		self.year = 2018
		self.youth = 'Youth'
		self.all_youth = 'ALL incl. youth'

	@staticmethod
	def change_location(loc):
		path = Common.get_config('config.ini', 'box_file_path', 'path_bap')
		path_qa = Common.get_config('config.ini', 'box_file_path', 'path_bap_qa')
		if loc == p.DATA:
			box_path = os.path.join(os.path.expanduser('~'), path)
			os.chdir(box_path)
			return box_path
		elif loc == p.QA:
			qa_path = os.path.join(os.path.expanduser('~'), path_qa)
			os.chdir(qa_path)
			return None

	def check_rics_file(self):
		for fl in self.ric_files:
			print('-' * 100, '\nPROCESSING RIC FILES\n')
			print(fl)
			try:
				BapQA.change_location(p.DATA)
				wb = openpyxl.load_workbook(fl, data_only=True)

				print('\tProgram Data')
				program_sheet = wb.get_sheet_by_name(WS.bap_program.value)
				self.rics_sheet(program_sheet)
				print('\tProgram Youth Data')
				program_youth_sheet = wb.get_sheet_by_name(WS.bap_program_youth.value)
				self.rics_sheet(program_youth_sheet)
				self.program_youth_sheet(program_youth_sheet)
				print('\tQuarterly Company Data')
				quarterly_company_sheet = wb.get_sheet_by_name(WS.bap_company.value)
				self.rics_sheet(quarterly_company_sheet)
				print('\tAnnual Company Data')
				annual_company_sheet = wb.get_sheet_by_name(WS.bap_company_annual.value)
				self.rics_sheet(annual_company_sheet)

				BapQA.change_location(p.QA)
				wb.save('{}_QA.xlsx'.format(fl[:-5]))
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
							c.fill = self.amber
					if c.column == 'E':
						if c.value != self.year:
							c.fill = self.amber
					if c.column == 'F':
						if c.value.lower() != self.youth.lower():
							c.fill = self.amber

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
