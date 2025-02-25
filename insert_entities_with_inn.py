from config.config import cards51_directory_path as dir_path
from os.path import join
from config.db_connection_config import host, port, user, password, db_name
import pandas as pd
from sqlalchemy import create_engine, text
from numpy import isnan


def insert_or_update_entities(df: pd.core.frame.DataFrame,
                table_name: str,
                if_exists='append') -> None:

    df = df[['Контрагент', 'ИНН', 'Полное наименование']]
    df.index_name = 'id'
    df.columns = ['entity_1C_name', 'inn', 'full_name']
    
    try:
        engine = create_engine("mysql+mysqlconnector://" +
            f"{user}:{password}@{host}/{db_name}")
        connection = engine.connect()

        # column_names = df.columns.to_list()
        for row_number in df.index:
            entity_1C_name = str(df.loc[row_number, 'entity_1C_name'])
            if type(entity_1C_name) == str:
                entity_1C_name = entity_1C_name.replace('"', '""')
            entity_1C_name = '"' + entity_1C_name + '"'
            
            inn = df.loc[row_number, 'inn']

            full_name = str(df.loc[row_number, 'full_name'])
            if type(full_name) == str:
                full_name = full_name.replace('"', '""')
            full_name = '"' + full_name + '"'

            if isnan(inn):
                sql_query = f'INSERT INTO `{table_name}` ' + \
                    '(`entity_1C_name`, `full_name`) ' + \
                    'VALUES (' + entity_1C_name + ', ' + full_name + ') ' + \
                    'ON DUPLICATE KEY UPDATE ' + \
                    f'full_name = {full_name};'
            else:
                sql_query = f'INSERT INTO `{table_name}` ' + \
                    '(`entity_1C_name`, `inn`, `full_name`) ' + \
                    'VALUES (' + entity_1C_name + ', ' + str(inn) + ', ' + \
                        full_name + ') ' + \
                    'ON DUPLICATE KEY UPDATE ' + \
                    f'inn = {inn}, ' + \
                    f'full_name = {full_name};'

            connection.execute(text(sql_query))

        connection.commit()
        connection.close()

    except Exception as ex:
        print(ex)
        

if __name__ == '__main__':
    df = pd.read_excel(join(dir_path, 'Контрагенты.xlsx'), skiprows=6)
    
    insert_or_update_entities(df, 'entities')
