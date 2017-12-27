import os
import openpyxl
from openpyxl.styles import PatternFill
from Shared.common import Common
from Shared.enums import FileType, WorkSheet as WS


class BapQA:
	def __init__(self):
		path = Common.get_config('config.ini', 'box_file_path', 'path_bap')
		box_path = os.path.join(os.path.expanduser('~'), path)
		os.chdir(box_path)
		file_source = os.listdir(box_path)
		self.ric_files = [f for f in file_source]
		self.missing = PatternFill(fgColor='99004C', bgColor='C00000', fill_type='solid')
		self.zero = PatternFill(fgColor='FF9933', bgColor='C00000', fill_type='solid')

	def check_rics_file(self):
		for fl in self.ric_files:
			if fl[0:2] != '~$':
				if fl[-3:] in FileType.SPREAD_SHEET.value or fl[-4:] in FileType.SPREAD_SHEET.value:
					wb = openpyxl.load_workbook(fl, data_only=True)
					print('Program Data')
					program_sheet = wb.get_sheet_by_name(WS.bap_program.value)
					self.rics_sheet(program_sheet)
					print('Program Youth Data')
					program_youth_sheet = wb.get_sheet_by_name(WS.bap_program_youth.value)
					self.rics_sheet(program_youth_sheet)
					print('Quarterly Company Data')
					quarterly_company_sheet = wb.get_sheet_by_name(WS.bap_company.value)
					self.rics_sheet(quarterly_company_sheet)
					print('Annual Company Data')
					annual_company_sheet = wb.get_sheet_by_name(WS.bap_company_annual.value)
					self.rics_sheet(annual_company_sheet)
					wb.save('{}_QA.xlsx'.format(fl[:-5]))

	def rics_sheet(self, sheet):
		for cl in sheet.columns:
			for c in cl:
				if c.row > 1:
					print('{}\t{}'.format(str(c.row), str(c.value)))
					if c.value == '' or c.value is None:
						c.fill = self.missing
					if c.value == 0:
						c.fill = self.zero