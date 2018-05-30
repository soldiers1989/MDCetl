from configparser import ConfigParser

import pyodbc

import pandas as pd
import os


class DAL:

        def __init__(self, guid):
            self.guid = guid

        @staticmethod
        def get_config(header, item):
            config = ConfigParser()
            config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'Config_SG.ini'))
            con_str = config.get(header, item)
            return con_str

        @staticmethod
        def connect():
            try:
                con_str = DAL.get_config('db_connect', 'conn')
                conn = pyodbc.connect(con_str)
                return conn
            except Exception as ex:
                print('Connection Exception: {}'.format(ex))
                return None

        @classmethod
        def execute(self, sql):
            try:
                con = self.connect()
                cursor = con.cursor()
                cursor.execute(sql)
                cursor.commit()
            except Exception as ex:
                print('BAP ETL EXCEPTION: {}'.format(ex))

        @classmethod
        def pandas_read(self, sql):
            try:
                conn = self.connect()
                data = pd.read_sql(sql, conn)
                return data
            except Exception as ex:
                print('PANDAS READ SQL EXCEPTION: {}'.format(ex))
                return None

        @classmethod
        def bulk_insert(self, sql, values):
            con = self.connect()
            cursor = con.cursor()
            try:
                cursor.executemany(sql, values)
                cursor.commit()
                print('DATA UPLOAD SUCCESSFUL !')
            except Exception as ex:
                print('DB ERROR: {}'.format(ex))



        def bulk_insert_csv_program_youth(self, dataframe):
            values = []
            for index, row in dataframe.iterrows():
                val = []
                val.append(row['UniqueID'])
                val.append(row['SourceSystem'])
                val.append(row['DataSource'])
                val.append(row['Path'])
                val.append(row['FileName'])
                val.append(row['FileID'])
                val.append(row['BatchID'])
                val.append('csv_program_youth')
                val.append(row['OutreachNumberOfClientReferralsReceivedFromPartners'])
                val.append(row['NumerOfPaidAdvisorsMentorsAnalystsAssistingYouthClients'])
                val.append(row['Quarter'])
                val.append(row['Year'])
                val.append(row['Youth'])
                values.append(val)

            sql = 'INSERT INTO[Config].[CompanyAggProgramYouth] Values (?,?,?,?,?,?,?,?,?,?,?,?,?)'
            self.bulk_insert(sql, values)


