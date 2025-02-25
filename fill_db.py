from config.config import cards51_directory_path as dir_path
from os import listdir
from os.path import isfile, join
from config.db_connection_config import host, port, user, password, db_name
from py.card51 import *
from py.mysql_db import *

# from db_connection_config import host, port, user, password, db_name
# from config import cards51_directory_path as dir_path
# from config import card_indexes
# from config import min_date_str, max_date_str
# import pandas as pd
# import pymysql
# import datetime
# from periods import insert_periods
# from insert_entities_with_inn import insert_or_update_entities


def get_cards51_file_names(
        name_parts: list[str] = [],
        include_hidden_files: bool = False) -> list[str]:

    cards51_file_names = []

    for file_name in listdir(dir_path):
        if not isfile(join(dir_path, file_name)):
            continue
        
        if not 'xls' in file_name.lower():
            continue

        if not include_hidden_files and file_name[0] == '.':
            continue

        is_file_name_contains_all_parts = True
        for name_part in name_parts:
            if not name_part.lower() in file_name.lower():
                is_file_name_contains_all_parts = False
                break

        if is_file_name_contains_all_parts:
            cards51_file_names.append(file_name)

    return cards51_file_names


def load_card51_from_excel_to_mysql_db(path_to_excel_card51: str) -> None:
    card51_file_name = path_to_excel_card51.split('/')[-1]
    print(f'Loading data from "{card51_file_name}" ...')

    card51_info = get_card51_info(path_to_excel_card51)
    # print(card51_info)

    df = get_card51_data(path_to_excel_card51)
    # print(df.head())

    df_periods = get_periods(df.period.min(), df.period.max())
    insert_periods(df_periods)

    entities = get_entites(df)
    insert_set(entities, 'entities', 'entity_1C_name')

    income_articles = get_income_articles(df)
    insert_set(income_articles, 'income_articles', 'article_name')

    outcome_articles = get_outcome_articles(df)
    insert_set(outcome_articles, 'outcome_articles', 'article_name')

    accounts = get_accounts(df)
    insert_set(accounts, 'accounts', 'id')

    card51_info = get_card51_info(path_to_excel_card51)
    insert_card51_info(card51_info)

    df_outcomes = get_outcomes(card51_info, df)
    # print(df_outcomes.info())
    insert_data(df_outcomes, 'outcomes')
    
    df_incomes = get_incomes(card51_info, df)
    insert_data(df_incomes, 'incomes')

    print('Done!\n')


if __name__ == '__main__':

    # df = pd.read_excel(join(dir_path, 'Контрагенты.xlsx'), skiprows=6)
    # insert_or_update_entities(df, 'entities')

    cards_name_parts = ['Карточка', '51']
    cards51_file_names = get_cards51_file_names(cards_name_parts)

    for card51_file_name in cards51_file_names:
        path_to_excel_card51 = join(dir_path, card51_file_name)
        load_card51_from_excel_to_mysql_db(path_to_excel_card51)

