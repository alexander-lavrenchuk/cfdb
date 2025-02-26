from config.db_connection_config import host, port, user, password, db_name
import pandas as pd
from py.mysql_db import select_data, get_min_max_dates
from py.card51 import get_periods
from config.config import abs_accurancy, reports_directory_path
from os.path import join, exists
from os import remove
import openpyxl
from openpyxl.utils import get_column_letter
from collections.abc import Callable


def select_sum_amount_group_by_entity(table_name: str) \
        -> pd.core.frame.DataFrame:

    sql_query = f'''
        (SELECT
            t1.period_name,
            t3.entity_1C_name,
            t3.inn,
            SUM(t1.amount) AS `{table_name}`
        FROM {table_name} AS t1
        JOIN
        (SELECT entity_id, SUM(amount) AS total
        FROM {table_name}
        GROUP BY entity_id
        ORDER BY total DESC) AS t2
        ON t2.entity_id = t1.entity_id
        JOIN entities AS t3 ON t3.id = t1.entity_id
        GROUP BY t1.period_name, t1.entity_id
        ORDER BY t1.period_name, t2.total DESC)
        UNION
        (SELECT
            period_name,
            'Not specified' AS entity_1C_name,
            'Not specified' AS inn,
            SUM(amount)
        FROM {table_name}
        WHERE entity_id IS NULL
        GROUP BY period_name);
        '''

    df = select_data(sql_query)
    return df


def select_sum_amount_group_by_article(table_name: str) \
        -> pd.core.frame.DataFrame:

    articles_table_name = 'income_articles' if 'ins_' in table_name \
        else 'outcome_articles'

    sql_query = f'''
        (SELECT
            t1.period_name,
            t3.article_name AS `article_name`,
            "",
            SUM(t1.amount) AS `{table_name}`
        FROM {table_name} AS t1
        JOIN
        (SELECT article_id, SUM(amount) AS total
        FROM {table_name}
        GROUP BY article_id
        ORDER BY total DESC) AS t2
        ON t2.article_id = t1.article_id
        JOIN {articles_table_name} AS t3 ON t3.id = t1.article_id
        GROUP BY t1.period_name, t1.article_id
        ORDER BY t1.period_name, t2.total DESC)
        UNION
        (SELECT
            period_name,
            'Not specified' AS article_name,
            "",
            SUM(amount)
        FROM {table_name}
        WHERE article_id IS NULL
        GROUP BY period_name);
        '''

    df = select_data(sql_query)
    return df


def select_sum_amount_group_by_account(table_name: str) \
        -> pd.core.frame.DataFrame:

    sql_query = f'''
        (SELECT
            t1.period_name,
            t1.account_id AS `account_id`,
            t3.account_name AS `account_name`,
            SUM(t1.amount) AS `{table_name}`
        FROM {table_name} AS t1
        JOIN
        (SELECT account_id, SUM(amount) AS total
        FROM {table_name}
        GROUP BY account_id
        ORDER BY total DESC) AS t2
        ON t2.account_id = t1.account_id
        JOIN accounts AS t3 ON t3.id = t1.account_id
        GROUP BY t1.period_name, t1.account_id
        ORDER BY t1.period_name, t2.total DESC)
        UNION
        (SELECT
            period_name,
            'Not specified' AS `account_id`,
            'Not specified' AS `account_id`,
            SUM(amount)
        FROM {table_name}
        WHERE account_id IS NULL
        GROUP BY period_name);
        '''

    df = select_data(sql_query)
    return df


# (SELECT
#     t1.period_name,
#     t1.account_id AS `account_id`,
#     t3.account_name AS `account_name`,
#     t4.entity_1C_name AS `entity_name`,
#     t4.inn AS `inn`,
#     SUM(t1.amount) AS `ins_op`
# FROM ins_op AS t1
# JOIN accounts AS t3 ON t3.id = t1.account_id
# JOIN entities AS t4 ON t4.id = t1.entity_id
# GROUP BY t1.period_name, t1.account_id, t1.entity_id)
# UNION
# (SELECT
#     period_name,
#     'Not specified' AS `account_id`,
#     'Not specified' AS `account_name`,
#     'Not specified' AS `entity_name`,
#     'Not specified' AS `inn`,
#     SUM(amount)
# FROM ins_op
# WHERE account_id IS NULL
# GROUP BY period_name);


def select_sum_amount_group_by_account_entity(table_name: str) \
        -> pd.core.frame.DataFrame:

    sql_query = f'''
        SELECT
            t0.period_name,
            t1.account_id,
            t1.entity_id,
            SUM(t1.amount) AS amount_sum,
            t2.amount_sum_by_account,
            t3.amount_sum_by_account_entity
        FROM periods AS t0
        LEFT JOIN {table_name} AS t1
        ON t0.period_name = t1.period_name
        LEFT JOIN (
            SELECT account_id, SUM(amount) AS amount_sum_by_account
            FROM {table_name}
            GROUP BY account_id) AS t2
        ON t1.account_id = t2.account_id
        LEFT JOIN (
            SELECT
                account_id,
                entity_id,
                SUM(amount) AS amount_sum_by_account_entity
            FROM {table_name}
            GROUP BY account_id, entity_id) AS t3
        ON t1.account_id = t3.account_id AND t1.entity_id = t3.entity_id
        GROUP BY t0.period_name, t1.account_id, t1.entity_id
        ORDER BY
            t2.amount_sum_by_account DESC,
            t3.amount_sum_by_account_entity DESC;
#         (SELECT
#             t1.period_name,
#             t1.account_id AS `account_id`,
#             t3.account_name AS `account_name`,
#             t4.entity_1C_name AS `entity_name`,
#             t4.inn AS `inn`,
#             SUM(t1.amount) AS `{table_name}`
#         FROM {table_name} AS t1
#         JOIN accounts AS t3 ON t3.id = t1.account_id
#         JOIN entities AS t4 ON t4.id = t1.entity_id
#         GROUP BY t1.period_name, t1.account_id, t1.entity_id
#         ORDER BY t1.account_id, t1.entity_id);
        '''

    df = select_data(sql_query)
    return df


def pivot_and_sort_data_frame_with_single_analytics(
        df: pd.core.frame.DataFrame) \
        -> pd.core.frame.DataFrame:

    beg_date, end_date = get_min_max_dates()
    df_periods = get_periods(beg_date, end_date)

    dfm = df_periods.merge(df, how='outer', on=['period_name'])
    column_numbers = dfm.shape[1]
    dfm.iloc[:, column_numbers - 3] = \
        dfm.iloc[:, column_numbers - 3].fillna('Not specified')
    dfm.iloc[:, column_numbers - 2] = \
        dfm.iloc[:, column_numbers - 2].fillna('Not specified')
    dfm.iloc[:, column_numbers - 1] = \
        dfm.iloc[:, column_numbers - 1].fillna(0)

    columns = dfm.columns.tolist()
    periods = columns[0]
    analytics_id = columns[3]
    analytics_name = columns[4]
    values = columns[5]

    dfp = dfm.pivot_table(
        index=[analytics_id, analytics_name],
        columns=periods,
        values=values,
        aggfunc='sum',
        fill_value=0,
        margins=True)
    dfp = dfp.sort_values('All', ascending=False, axis=0)
    # dfp = dfp.drop('All')

    # remove rows with only zeroes
    dfp = dfp[~(dfp == 0).all(axis=1)]

    return dfp


def pivot_and_sort_data_frame_with_two_analytics(
        df: pd.core.frame.DataFrame) \
        -> pd.core.frame.DataFrame:

    beg_date, end_date = get_min_max_dates()
    df_periods = get_periods(beg_date, end_date)

    dfm = df_periods.merge(df, how='outer', on=['period_name'])
    column_numbers = dfm.shape[1]
    dfm.iloc[:, column_numbers - 5] = \
        dfm.iloc[:, column_numbers - 5].fillna('Not specified')
    dfm.iloc[:, column_numbers - 4] = \
        dfm.iloc[:, column_numbers - 4].fillna('Not specified')
    dfm.iloc[:, column_numbers - 3] = \
        dfm.iloc[:, column_numbers - 3].fillna('Not specified')
    dfm.iloc[:, column_numbers - 2] = \
        dfm.iloc[:, column_numbers - 2].fillna('Not specified')
    dfm.iloc[:, column_numbers - 1] = \
        dfm.iloc[:, column_numbers - 1].fillna(0)

    columns = dfm.columns.tolist()
    periods = columns[0]
    analytics1_id = columns[3]
    analytics1_name = columns[4]
    analytics2_id = columns[5]
    analytics2_name = columns[6]
    values = columns[7]

    dfp = dfm.pivot_table(
        index=[analytics1_id,
            analytics1_name,
            analytics2_id,
            analytics2_name],
        columns=periods,
        values=values,
        aggfunc='sum',
        fill_value=0,
        margins=True)
    dfp = dfp.sort_values('All', ascending=False, axis=0)
    # dfp = dfp.drop('All')

    # remove rows with only zeroes
    dfp = dfp[~(dfp == 0).all(axis=1)]

    return dfp


def format_sheet(wb_path: str, sh_name: str, column_width: list[int]) -> None:
    wb = openpyxl.load_workbook(wb_path)
    sh = wb[sh_name]

    # df = pd.read_excel(wb_path, sheet_name=sh_name)
    # column_B_width = 16 if df.iloc[:, 1].count() else 0

    # sh.column_dimensions['A'].width = 36
    # sh.column_dimensions['B'].width = column_B_width

    for col in range(1, 3):
        width = column_width[col - 1]
        if width > 0:
            sh.column_dimensions[get_column_letter(col)].width = width
        else:
            sh.column_dimensions[get_column_letter(col)].hidden = True

    # sh.column_dimensions['A'].width = column_width[0]
    # sh.column_dimensions['B'].width = column_width[1]

    min_row = 1
    max_row = sh.max_row
    min_col = 3
    max_col = sh.max_column
    cell_range = sh.iter_cols(
        min_row=min_row,
        max_row=max_row,
        min_col=min_col,
        max_col=max_col)

    for row in range(min_row, max_row + 1):
        sh['A' + str(row)].alignment = \
            openpyxl.styles.Alignment(horizontal='left')

        sh['B' + str(row)].alignment = \
            openpyxl.styles.Alignment(horizontal='left')

    for col in range(min_col, max_col + 1):
        sh.column_dimensions[get_column_letter(col)].width = column_width[2]

    for row in cell_range:
        for cell in row:
            cell.number_format = '#,##0.00'

    wb.save(wb_path)


def cf_to_excel(
    select_function: Callable[[str], pd.core.frame.DataFrame],
    file_name: str, column_width: list[int]) -> None:

    file_path = join(reports_directory_path, file_name)
    if exists(file_path):
        remove(file_path)
     
    activities = ['op', 'inv', 'fin', 'equity', 'vgo']
    # activities = ['inv']

    for activity in activities:
        cf_ins = 'ins_' + activity
        df_ins = select_function(cf_ins)
        dfp_ins = pivot_and_sort_data_frame_with_single_analytics(df_ins)

        writer_mode = 'a' if exists(file_path) else 'w'
        with pd.ExcelWriter(file_path, mode=writer_mode) as writer:  
            dfp_ins.to_excel(
                writer,
                sheet_name=activity,
                freeze_panes=(1, 2))
        
        cf_outs = 'outs_' + activity
        df_outs = select_function(cf_outs)
        dfp_outs = pivot_and_sort_data_frame_with_single_analytics(df_outs)
        start_row = writer.sheets[activity].max_row + 1

        writer_mode = 'a'
        with pd.ExcelWriter(file_path, mode=writer_mode,
                if_sheet_exists='overlay') as writer:  
            dfp_outs.to_excel(
                writer,
                sheet_name=activity,
                header=False,
                startrow=start_row)

        format_sheet(wb_path=file_path,
            sh_name=activity,
            column_width=column_width)

    print(f'CF model saved to {file_path}')


def cf_to_excel2(
    select_function: Callable[[str], pd.core.frame.DataFrame],
    file_name: str, column_width: list[int]) -> None:

    file_path = join(reports_directory_path, file_name)
    if exists(file_path):
        remove(file_path)
     
    activities = ['op', 'inv', 'fin', 'equity', 'vgo']
    # activities = ['inv']

    for activity in activities:
        cf_ins = 'ins_' + activity
        df_ins = select_function(cf_ins)
        print(df_ins)
        break
        dfp_ins = pivot_and_sort_data_frame_with_two_analytics(df_ins)


        writer_mode = 'a' if exists(file_path) else 'w'
        with pd.ExcelWriter(file_path, mode=writer_mode) as writer:  
            dfp_ins.to_excel(
                writer,
                sheet_name=activity,
                freeze_panes=(1, 4))
        
        cf_outs = 'outs_' + activity
        df_outs = select_function(cf_outs)
        print(df_outs)
        dfp_outs = pivot_and_sort_data_frame_with_two_analytics(df_outs)
        start_row = writer.sheets[activity].max_row + 1

        writer_mode = 'a'
        with pd.ExcelWriter(file_path, mode=writer_mode,
                if_sheet_exists='overlay') as writer:  
            dfp_outs.to_excel(
                writer,
                sheet_name=activity,
                header=False,
                startrow=start_row)

        # format_sheet(wb_path=file_path,
        #     sh_name=activity,
        #     column_width=column_width)

    print(f'CF model saved to {file_path}')


if __name__ == '__main__':
    # file_name = 'cf_by_entity.xlsx'
    # select_function = select_sum_amount_group_by_entity
    # column_width = [32, 14, 18]
    # cf_to_excel(select_function, file_name, column_width)

    # file_name = 'cf_by_article.xlsx'
    # select_function = select_sum_amount_group_by_article
    # column_width = [32, 0, 18]
    # cf_to_excel(select_function, file_name, column_width)

    # file_name = 'cf_by_account.xlsx'
    # select_function = select_sum_amount_group_by_account
    # column_width = [8, 32, 18]
    # cf_to_excel(select_function, file_name, column_width)

    file_name = 'cf_by_account_entity.xlsx'
    select_function = select_sum_amount_group_by_account_entity
    column_width = [8, 32, 18]
    cf_to_excel2(select_function, file_name, column_width)

    # raise KeyboardInterrupt

