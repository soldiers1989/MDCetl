
from Shared.db import DB as DAL
from Shared.common import Common
CM = Common()

class sg_get_tables:

    def __init__(self):
        pass

    @classmethod
    def schema_to_dfs(self, schema, which_tables="all"):
        """
        Takes name of schema and returns dict of dataframes, one entry for each table in schema.
        Key = table name
        Value = dataframe
        Dataframes will contain results from SELECT * FROM TABLE.
        :param schema:
        :return dict:
        """
        select_all = "SELECT * FROM "
        sql = str("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + str(schema) + "'")
        tables = DAL.pandas_read(sql)

        if which_tables == "all":
            table_names = tables["TABLE_NAME"]
        else:
            table_names = which_tables

        table_dict = {}
        sql_statements = []
        for table in table_names:
            statement = str(select_all + schema + "." + table)
            sql_statements.append(statement)
            table_dict[table] = 0

        for statement in sql_statements:
            df = DAL.pandas_read(statement)
            table_name = statement.split(".")[1]
            for key in table_dict.keys():
                if key.lower() == table_name.lower():
                    table_dict[key] = df

        return table_dict

    @classmethod
    def get_dependencies(self, schema):
        """
        Takes name of schema and queries DB for table dependencies.
        Returned df has 2 cols:
            Col 1. Table w FK
            Col 2. Table referenced by FK in Col 1.
        :param schema:
        :return dataframe:
        """

        schema_str = "'" + str(schema) + "'"
        sql_str = CM.get_config("config.ini", "dependency_query", "query")
        dependency_sql = sql_str + schema_str
        dependencies = DAL.pandas_read(dependency_sql)
        dependencies.columns = ["FKTable", "ReferencedTable"]

        return dependencies

    @classmethod
    def get_load_order(self, schema):
        """
        Takes name of schema, returns DataFrame
        with load order in first column.
        :param schema:
        :return DataFrame:
        """

        schema_str = "'" + str(schema) + "'"
        sql_str = CM.get_config("config.ini", "dependency_query", "load_order_query")
        sql_str = sql_str.replace("WHAT_SCHEMA", schema_str)
        load_order = DAL.pandas_read(sql_str)
        return load_order
