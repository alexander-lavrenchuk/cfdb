from config.config import *
import pandas as pd
import datetime
from config.db_connection_config import host, port, user, password, db_name
from sqlalchemy import create_engine, text


def get_card51_info(path_to_excel_card51: str) -> dict:
    entity_row = card_rows['entity_row'] - 1
    title_row = card_rows['title_row'] - 1
    first_data_row = card_rows['first_data_row'] - 1

    entity_col = card_columns['entity_col'] - 1
    title_col = card_columns['title_col'] - 1
    period_col = card_columns['period_col'] - 1
    debit_amount_col = card_columns['debit_amount_col'] - 1
    credit_amount_col = card_columns['credit_amount_col'] - 1
    debit_total_col = card_columns['debit_total_col'] - 1
    credit_total_col = card_columns['credit_total_col'] - 1

    try:
        df = pd.read_excel(path_to_excel_card51, header=None)
    except Exception as ex:
        print(ex)
        return
    
    holder_name = df.iloc[entity_row, entity_col].strip()

    title = df.iloc[title_row, title_col].strip()
    
    debit_total_value = df.iloc[df.shape[0] - 1, debit_total_col]
    credit_total_value = df.iloc[df.shape[0] - 1, credit_total_col]
    
    df = df.iloc[first_data_row:df.shape[0] - 1]
    debit_sum = round(df.iloc[:, debit_amount_col].sum(), 2)
    credit_sum = round(df.iloc[:, credit_amount_col].sum(), 2)

    transaction_count = sum(~df.iloc[:, debit_amount_col].isna())
    transaction_count += sum(~df.iloc[:, credit_amount_col].isna())

    df.iloc[:, period_col] = \
            df.iloc[:, period_col].apply( \
            lambda x: datetime.datetime.strptime(x, '%d.%m.%Y').date())

    card51_info = dict()
    card51_info['account_no'] = '51'
    card51_info['holder_name'] = holder_name
    card51_info['title'] = title
    card51_info['card_file_name'] = path_to_excel_card51.rsplit('/')[-1]
    card51_info['beginning_date'] = df.iloc[:, period_col].min()
    card51_info['ending_date'] = df.iloc[:, period_col].max()
    card51_info['debit_total_value'] = debit_total_value
    card51_info['credit_total_value'] = credit_total_value
    card51_info['debit_sum'] = debit_sum
    card51_info['credit_sum'] = credit_sum
    card51_info['transactions_count'] = transaction_count
    
    return card51_info


def get_card51_data(path_to_excel_card51: str) -> pd.core.frame.DataFrame:
    first_data_row = card_rows['first_data_row'] - 1
    period_col = card_columns['period_col'] - 1
    document_col = card_columns['document_col'] - 1
    debit_analytics_col = card_columns['debit_analytics_col'] - 1
    credit_analytics_col = card_columns['credit_analytics_col'] - 1


    try:
        df = pd.read_excel(path_to_excel_card51, header=None)
    except Exception as ex:
        print(ex)
        return
    
    df.columns = field_names
    df = df.drop(['empty_1', 'empty_2'], axis=1)

    df = df.iloc[first_data_row:df.shape[0] - 1]
    df.iloc[:, period_col] = \
            df.iloc[:, period_col].apply( \
            lambda x: datetime.datetime.strptime(x, '%d.%m.%Y').date())

    document = df.iloc[:, document_col].str.split(pat='\n', n=1, expand=True)
    document_1 = document.iloc[:, 0].str.split(pat=' от ', n=1, expand=True)
    document_1.iloc[:, 1] = \
        pd.to_datetime(document_1.iloc[:, 1],
            dayfirst=True, utc=True)
    document_2 = document.iloc[:, 1]
    document_1['_'] = document_2
    document_1.columns = document_names
    df = df.join(document_1)

    debit_analytics = \
        df.iloc[:, debit_analytics_col].str \
            .split(pat='\n', n=len(debit_analytics_names), expand=True)
    debit_analytics.columns = debit_analytics_names
    df = df.join(debit_analytics)

    credit_analytics = \
        df.iloc[:, credit_analytics_col].str \
            .split(pat='\n', n=len(credit_analytics_names), expand=True)
    credit_analytics.columns = credit_analytics_names
    df = df.join(credit_analytics)

    df[period_month_field_name] = \
        pd.DatetimeIndex(df.period,
            dayfirst=True).year.astype(str) + \
            pd.Series(['-']*df.shape[0], index=df.index) + \
            pd.DatetimeIndex(df.period,
                dayfirst=True).month.astype(str).str.zfill(2)

    return df


def get_entites(df: pd.core.frame.DataFrame, fillna='n/a') -> set[str]:
    entities = set(df[df.debit_account == '51']. \
        credit_analytics_part_2.fillna(fillna))
    entities.update(df[df.credit_account == '51']. \
        debit_analytics_part_2.fillna(fillna))

    return entities


def get_income_articles(df: pd.core.frame.DataFrame) -> set[str]:
    articles = set(df[df.debit_account == '51'].debit_analytics_part_3)

    return articles


def get_outcome_articles(df: pd.core.frame.DataFrame) -> set[str]:
    articles = set(df[df.credit_account == '51'].credit_analytics_part_3)

    return articles


def get_accounts(df: pd.core.frame.DataFrame) -> set[str]:
    accounts = set(df.debit_account)
    accounts.update(df.credit_account)

    return accounts


def get_min_max_periods(df: pd.core.frame.DataFrame) -> tuple[datetime.date]:
    min_period = df.period_month.min()
    max_period = df.period_month.max()

    return min_period, max_period


def get_periods(min_date: datetime.date,
                max_date: datetime.date) \
                    -> pd.core.frame.DataFrame:

    df_periods = pd.DataFrame(
        columns=['period_name',
                 'beginning_date',
                 'ending_date'])

    year = min_date.year
    month = min_date.month
    td = datetime.timedelta(days=1)
    
    beg_date = min_date
    if month < 12:
        end_date = datetime.date(year, month + 1, 1) - td
    else:
        end_date = datetime.date(year + 1, 1, 1) - td

    while end_date <= max_date:
        row = {'period_name': f'{year}-{month:02d}',
               'beginning_date': beg_date,
               'ending_date': end_date}
        df_periods = df_periods._append(row, ignore_index=True)

        beg_date = end_date + td
        year = beg_date.year
        month = beg_date.month
        if month < 12:
            end_date = datetime.date(year, month + 1, 1) - td
        else:
            end_date = datetime.date(year + 1, 1, 1) - td

    return df_periods


def get_outcomes(card51_info: dict, \
    df: pd.core.frame.DataFrame) \
        -> pd.core.frame.DataFrame:

    df_outcomes = df[df['credit_account'] == '51']

    engine = create_engine("mysql+mysqlconnector://" +
        f"{user}:{password}@{host}/{db_name}")
   
    try:
        holder_name = card51_info['holder_name'].replace('"', '""')
        sql_query = 'SELECT holders.id AS holder_id ' + \
            'FROM holders ' + \
            'JOIN entities ON holders.entity_id = entities.id ' + \
            f'WHERE entities.entity_1C_name = "{holder_name}";'
        df_query = pd.read_sql_query(sql_query, engine)
        holder_id = df_query.holder_id[0]
        df_outcomes.insert(0, 'holder_id', holder_id)

        sql_query = 'SELECT id, entity_1C_name ' + \
            'FROM entities;'
        df_entities = pd.read_sql_query(sql_query, engine)
        df_outcomes = df_outcomes.merge(df_entities, how='left',
            left_on='debit_analytics_part_2',
            right_on='entity_1C_name')

        df_outcomes = df_outcomes.rename(columns={
            'debit_account': 'account_id',
            'period': 'date',
            'period_month': 'period_name',
            'credit_amount': 'amount',
            'id': 'entity_id'})
        
        df_outcomes = df_outcomes.drop(columns=[
            'document',
            'debit_analytics',
            'credit_analytics',
            'debit_amount',
            'credit_account',
            'saldo_type',
            'saldo_amount',
            'entity_1C_name'])

        sql_query = 'SELECT id, article_name ' + \
                'FROM outcome_articles;'
        df_articles = pd.read_sql_query(sql_query, engine)
        df_outcomes = df_outcomes.merge(df_articles, how='left',
            left_on='credit_analytics_part_3',
            right_on='article_name')

        df_outcomes = df_outcomes.rename(columns={
            'id': 'article_id'})
        
        df_outcomes = df_outcomes.drop(columns=[
            'article_name'])

    except Exception as ex:
        print(ex)

    return df_outcomes


def get_incomes(card51_info: dict, \
    df: pd.core.frame.DataFrame) \
        -> pd.core.frame.DataFrame:

    df_incomes = df[df['debit_account'] == '51']

    engine = create_engine("mysql+mysqlconnector://" +
        f"{user}:{password}@{host}/{db_name}")
   
    try:
        holder_name = card51_info['holder_name'].replace('"', '""')
        sql_query = 'SELECT holders.id AS holder_id ' + \
            'FROM holders ' + \
            'JOIN entities ON holders.entity_id = entities.id ' + \
            f'WHERE entities.entity_1C_name = "{holder_name}";'
        df_query = pd.read_sql_query(sql_query, engine)
        holder_id = df_query.holder_id[0]
        df_incomes.insert(0, 'holder_id', holder_id)

        sql_query = 'SELECT id, entity_1C_name ' + \
            'FROM entities;'
        df_entities = pd.read_sql_query(sql_query, engine)
        df_incomes = df_incomes.merge(df_entities, how='left',
            left_on='credit_analytics_part_2',
            right_on='entity_1C_name')

        df_incomes = df_incomes.rename(columns={
            'credit_account': 'account_id',
            'period': 'date',
            'period_month': 'period_name',
            'debit_amount': 'amount',
            'id': 'entity_id'})
        
        df_incomes = df_incomes.drop(columns=[
            'document',
            'debit_analytics',
            'credit_analytics',
            'credit_amount',
            'debit_account',
            'saldo_type',
            'saldo_amount',
            'entity_1C_name'])

        sql_query = 'SELECT id, article_name ' + \
                'FROM income_articles;'
        df_articles = pd.read_sql_query(sql_query, engine)
        df_incomes = df_incomes.merge(df_articles, how='left',
            left_on='debit_analytics_part_3',
            right_on='article_name')

        df_incomes = df_incomes.rename(columns={
            'id': 'article_id'})
        
        df_incomes = df_incomes.drop(columns=[
            'article_name'])

    except Exception as ex:
        print(ex)

    return df_incomes


