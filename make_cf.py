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


def move_column(data_frame: pd.core.frame.DataFrame,
    column_name: str, loc: int):

    col = data_frame.pop(column_name)
    data_frame.insert(
        loc=loc,
        column=column_name,
        value=col)


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
            id,
            account_name
        FROM accounts;
        '''
    df_accounts = select_data(sql_query)
    df_accounts.set_index(
        keys='id',
        inplace=True)
    
    sql_query = f'''
        SELECT
            id,
            entity_1C_name,
            inn
        FROM entities;
        '''
    df_entities = select_data(sql_query)
    # df_entities.set_index(
    #     keys='id',
    #     inplace=True)
    df_entities.inn = df_entities.inn.astype('str')
    df_entities.inn = df_entities.inn.str.replace('.0', '')
    df_entities.inn = df_entities.inn.str.replace('nan', '')
    # print(df_entities)
    
    sql_query = f'''
        SELECT
            period_name,
            account_id,
            entity_id,
            SUM(amount) AS amount_sum
        FROM {table_name}
        GROUP BY period_name, account_id, entity_id;
        '''
    df_amounts = select_data(sql_query)
    df_amounts.entity_id = df_amounts.entity_id.fillna(-1)
    
    sql_query = f'''
        SELECT
            account_id,
            SUM(amount) AS amount_sum_by_account
        FROM {table_name}
        GROUP BY account_id
        ORDER BY account_id;
        '''
    df_amounts_by_account = select_data(sql_query)
    df_amounts_by_account.set_index(
        keys='account_id',
        inplace=True)
    
    sql_query = f'''
        SELECT
            account_id,
            entity_id,
            SUM(amount) AS amount_sum_by_account_entity
        FROM {table_name}
        GROUP BY account_id, entity_id
        ORDER BY account_id, entity_id;
        '''
    df_amounts_by_account_entity = select_data(sql_query)
    df_amounts_by_account_entity.entity_id = \
        df_amounts_by_account_entity.entity_id.fillna(-1)

    df_amounts_by_account_entity.set_index(
        keys=['account_id', 'entity_id'],
        inplace=True)

    dfp = pd.pivot_table(
        data=df_amounts,
        values='amount_sum',
        index=['account_id', 'entity_id'],
        columns='period_name',
        aggfunc='sum',
        fill_value=0,
        margins=False,
        # dropna=False,
        sort=False)

    dfp = dfp.join(
        other=df_amounts_by_account,
        on='account_id',
        validate='m:1')

    dfp = dfp.join(
        other=df_amounts_by_account_entity,
        validate='m:1')

    dfp.sort_values(
        by=['amount_sum_by_account', 'amount_sum_by_account_entity'],
        ascending=[False, False],
        inplace=True)

    dfp.reset_index(inplace=True)
    dfp = dfp.merge(
        right=df_accounts,
        how='left',
        left_on='account_id',
        right_on='id',
        validate='m:1')
    dfp.entity_id = dfp.entity_id.astype('int')
    dfp = dfp.merge(
        right=df_entities,
        how='left',
        left_on='entity_id',
        right_on='id',
        validate='m:1')
    dfp.drop(columns=['entity_id', 'id', 'amount_sum_by_account'],
        inplace=True)

    dfp_totals = dfp.sum(axis=0,
        numeric_only=True).to_frame('All').T
    dfp = pd.concat([dfp_totals, dfp])
    dfp.loc['All'] = dfp.loc['All'].fillna('')

    df_periods = select_data('SELECT period_name FROM periods;')
    all_periods_set = set(df_periods.period_name)
    missing_periods_set = all_periods_set - set(dfp.columns)
    if len(missing_periods_set) > 0:
        for period in missing_periods_set:
            dfp.insert(loc=dfp.shape[1], column=period, value=0)

    dfp.rename(
        columns={'amount_sum_by_account_entity': 'All'},
        inplace=True)

    dfp.sort_index(
        axis=1,
        inplace=True)

    move_column(data_frame=dfp,
        column_name='account_id',
        loc=0)
    dfp.iloc[0, 0] = 'All'

    move_column(data_frame=dfp,
        column_name='account_name',
        loc=1)

    move_column(data_frame=dfp,
        column_name='entity_1C_name',
        loc=2)

    move_column(data_frame=dfp,
        column_name='inn',
        loc=3)

    return dfp


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


# def pivot_and_sort_data_frame_with_two_analytics(
#         df: pd.core.frame.DataFrame) \
#         -> pd.core.frame.DataFrame:
# 
#     beg_date, end_date = get_min_max_dates()
#     df_periods = get_periods(beg_date, end_date)
# 
#     dfm = df_periods.merge(df, how='outer', on=['period_name'])
#     column_numbers = dfm.shape[1]
#     dfm.iloc[:, column_numbers - 5] = \
#         dfm.iloc[:, column_numbers - 5].fillna('Not specified')
#     dfm.iloc[:, column_numbers - 4] = \
#         dfm.iloc[:, column_numbers - 4].fillna('Not specified')
#     dfm.iloc[:, column_numbers - 3] = \
#         dfm.iloc[:, column_numbers - 3].fillna('Not specified')
#     dfm.iloc[:, column_numbers - 2] = \
#         dfm.iloc[:, column_numbers - 2].fillna('Not specified')
#     dfm.iloc[:, column_numbers - 1] = \
#         dfm.iloc[:, column_numbers - 1].fillna(0)
# 
#     columns = dfm.columns.tolist()
#     periods = columns[0]
#     analytics1_id = columns[3]
#     analytics1_name = columns[4]
#     analytics2_id = columns[5]
#     analytics2_name = columns[6]
#     values = columns[7]
# 
#     dfp = dfm.pivot_table(
#         index=[analytics1_id,
#             analytics1_name,
#             analytics2_id,
#             analytics2_name],
#         columns=periods,
#         values=values,
#         aggfunc='sum',
#         fill_value=0,
#         margins=True)
#     dfp = dfp.sort_values('All', ascending=False, axis=0)
#     # dfp = dfp.drop('All')
# 
#     # remove rows with only zeroes
#     dfp = dfp[~(dfp == 0).all(axis=1)]
# 
#     return dfp


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


def format_sheet2(wb_path: str, sh_name: str, column_width: list[int]) -> None:
    wb = openpyxl.load_workbook(wb_path)
    sh = wb[sh_name]

    # df = pd.read_excel(wb_path, sheet_name=sh_name)
    # column_B_width = 16 if df.iloc[:, 1].count() else 0

    # sh.column_dimensions['A'].width = 36
    # sh.column_dimensions['B'].width = column_B_width

    for col in range(1, 5):
        width = column_width[col - 1]
        if width > 0:
            sh.column_dimensions[get_column_letter(col)].width = width
        else:
            sh.column_dimensions[get_column_letter(col)].hidden = True

    # sh.column_dimensions['A'].width = column_width[0]
    # sh.column_dimensions['B'].width = column_width[1]

    min_row = 1
    max_row = sh.max_row
    min_col = 5
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


        sh['C' + str(row)].alignment = \
            openpyxl.styles.Alignment(horizontal='left')

        sh['D' + str(row)].alignment = \
            openpyxl.styles.Alignment(horizontal='left')

    for col in range(min_col, max_col + 1):
        sh.column_dimensions[get_column_letter(col)].width = column_width[4]

    for row in cell_range:
        for cell in row:
            cell.number_format = '#,##0.00'

    wb.save(wb_path)
    wb.close()


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
    # activities = ['op']

    for activity in activities:
        cf_ins = 'ins_' + activity
        # df_ins = select_function(cf_ins)
        dfp_ins = select_function(cf_ins)
        # dfp_ins = pd.pivot_table(
        #         data=df_ins,
        #         values='amount_sum',
        #         index=['account_id', 'entity_id'],
        #         columns='period_name',
        #         aggfunc='sum',
        #         fill_value=0,
        #         margins=True,
        #         sort=False)
        # dfp_ins = pivot_and_sort_data_frame_with_two_analytics(df_ins)
        # dfp_ins.sort_index(axis=1, inplace=True)

        writer_mode = 'a' if exists(file_path) else 'w'
        with pd.ExcelWriter(file_path, mode=writer_mode) as writer:  
            dfp_ins.to_excel(
                writer,
                sheet_name=activity,
                float_format='%.2f',
                index=False,
                freeze_panes=(1, 4))
        
        cf_outs = 'outs_' + activity
        # df_outs = select_function(cf_outs)
        dfp_outs = select_function(cf_outs)
        # dfp_outs = pd.pivot_table(
        #         data=df_outs,
        #         values='amount_sum',
        #         index=['account_id', 'entity_id'],
        #         columns='period_name',
        #         aggfunc='sum',
        #         fill_value=0,
        #         margins=True,
        #         sort=False)
        # dfp_outs = pivot_and_sort_data_frame_with_two_analytics(df_outs)
        # dfp_outs.sort_index(axis=1, inplace=True)
        
        start_row = writer.sheets[activity].max_row + 1

        writer_mode = 'a'
        with pd.ExcelWriter(file_path, mode=writer_mode,
                if_sheet_exists='overlay') as writer:  
            dfp_outs.to_excel(
                writer,
                sheet_name=activity,
                float_format='%.2f',
                index=False,
                header=False,
                startrow=start_row)

        # print('Start formatting')
        format_sheet2(wb_path=file_path,
            sh_name=activity,
            column_width=column_width)
        # print('End formatting')

    print(f'CF model saved to {file_path}')


if __name__ == '__main__':
    file_name = 'cf_by_entity.xlsx'
    select_function = select_sum_amount_group_by_entity
    column_width = [32, 14, 18]
    cf_to_excel(select_function, file_name, column_width)

    file_name = 'cf_by_article.xlsx'
    select_function = select_sum_amount_group_by_article
    column_width = [32, 0, 18]
    cf_to_excel(select_function, file_name, column_width)

    file_name = 'cf_by_account.xlsx'
    select_function = select_sum_amount_group_by_account
    column_width = [8, 32, 18]
    cf_to_excel(select_function, file_name, column_width)

    file_name = 'cf_by_account_entity.xlsx'
    select_function = select_sum_amount_group_by_account_entity
    column_width = [8, 32, 34, 14, 18]
    cf_to_excel2(select_function, file_name, column_width)

    # raise KeyboardInterrupt

