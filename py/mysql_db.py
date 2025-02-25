from config.db_connection_config import host, port, user, password, db_name
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
from py.card51 import get_periods


def select_data(sql_query: str) \
        -> pd.core.frame.DataFrame:

    engine = create_engine("mysql+mysqlconnector://" +
        f"{user}:{password}@{host}/{db_name}")
   
    try:
        df = pd.read_sql_query(sql_query, engine)
        return df

    except Exception as ex:
        print(ex)
        return None


def select_min_max_period() -> tuple[str]:
    sql_query = 'SELECT COUNT(period_name) FROM periods;'
    df_query = select_data(sql_query)
    if df_query.iloc[0, 0] == 0:
        return None

    sql_query = 'SELECT period_name ' + \
        'FROM periods ' + \
        'ORDER BY period_name ' + \
        'LIMIT 1;'
    df_query = select_data(sql_query)
    min_period_db = df_query.iloc[0, 0]

    sql_query = 'SELECT period_name ' + \
        'FROM periods ' + \
        'ORDER BY period_name DESC ' + \
        'LIMIT 1;'
    df_query = select_data(sql_query)
    max_period_db = df_query.iloc[0, 0]

    return min_period_db, max_period_db


def insert_data(df: pd.core.frame.DataFrame,
                table_name: str,
                if_exists='append') -> None:

    engine = create_engine("mysql+mysqlconnector://" +
        f"{user}:{password}@{host}/{db_name}")
    
    try:
        df.to_sql(table_name,
            con=engine,
            if_exists=if_exists,
            index=False)

    except Exception as ex:
        print(ex)


def insert_set(values: set, table_name: str, field_name: str) -> None:
    sql_query = f'SELECT {field_name} FROM {table_name};'
    df_values_db = select_data(sql_query)
    values_db = set(df_values_db[field_name])
    values_ = values - values_db
    df_values = pd.DataFrame({field_name: list(values_)})
    insert_data(df_values, table_name)


def insert_periods(df_periods: pd.core.frame.DataFrame) -> None:

    min_max_period_db = select_min_max_period()

    if not min_max_period_db:
        insert_data(df_periods, 'periods')
        return

    min_period = df_periods.period_name.min()
    max_period = df_periods.period_name.max()
    min_period_db, max_period_db = min_max_period_db

    td = datetime.timedelta(days=1)

    if min_period < min_period_db:
        beg_date = df_periods.beginning_date.min()
        sql_query = 'SELECT beginning_date ' + \
            'FROM periods ' + \
            'ORDER BY beginning_date ' + \
            'LIMIT 1;'
        df_query = select_data(sql_query)
        end_date = df_query.iloc[0, 0] - td
        df_periods_sub = get_periods(beg_date, end_date)
        insert_data(df_periods_sub, 'periods')

    if max_period > max_period_db:
        sql_query = 'SELECT ending_date ' + \
            'FROM periods ' + \
            'ORDER BY ending_date DESC ' + \
            'LIMIT 1;'
        df_query = select_data(sql_query)
        beg_date = df_query.iloc[0, 0] + td
        end_date = df_periods.ending_date.max()
        df_periods_sub = get_periods(beg_date, end_date)
        insert_data(df_periods_sub, 'periods')

    return


def insert_card51_info(card51_info: dict) -> None:
    account_no = card51_info['account_no']
    holder_name = card51_info['holder_name'].replace('"', '""')
    
    try:
        engine = create_engine("mysql+mysqlconnector://" +
            f"{user}:{password}@{host}/{db_name}")
        connection = engine.connect()
        
        sql_query = 'INSERT IGNORE INTO accounts ' + \
            '(id) ' + \
            f'VALUES ("{account_no}");'
        connection.execute(text(sql_query))
        connection.commit()
        
        sql_query = 'INSERT IGNORE INTO entities ' + \
            '(entity_1C_name) ' + \
            f'VALUES ("{holder_name}");'
        connection.execute(text(sql_query))
        connection.commit()
        
        sql_query = 'SELECT id FROM entities WHERE ' + \
            f'entity_1C_name = "{holder_name}";'
        df_query = pd.read_sql_query(sql_query, engine)
        entity_id = df_query.iloc[0, 0]

        sql_query = 'INSERT IGNORE INTO holders ' + \
            '(entity_id) ' + \
            f'VALUES ({entity_id});'
        connection.execute(text(sql_query))
        connection.commit()

        sql_query = 'SELECT id FROM holders WHERE ' + \
            f'entity_id = "{entity_id}";'
        df_query = pd.read_sql_query(sql_query, engine)
        holder_id = df_query.iloc[0, 0]

        card51_info_mod = dict(card51_info)
        del card51_info_mod['holder_name']
        card51_info_mod['holder_id'] = holder_id
        df_card51_info = pd.DataFrame(card51_info_mod, index=[0])

        df_card51_info.to_sql('cards',
            con=engine,
            if_exists='append',
            index=False)

        connection.close()

    except Exception as ex:
        print(ex)


def get_min_max_dates() -> tuple[datetime.date]:

    sql_query = '''
        SELECT beginning_date
        FROM periods
        WHERE period_name =
            (SELECT min(period_name)
            FROM outcomes);
        '''
    df = select_data(sql_query)
    beg_date = df.iloc[0, 0]

    sql_query = '''
        SELECT ending_date
        FROM periods
        WHERE period_name =
            (SELECT max(period_name)
            FROM outcomes);
        '''
    df = select_data(sql_query)
    end_date = df.iloc[0, 0]

    return beg_date, end_date


