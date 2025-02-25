# Directory path, containning cards of account 51 to be loaded to Data Base
cards51_directory_path = './loads'
reports_directory_path = './rpts'

# Absolute accurancy in currency units for compare values
abs_accurancy = 0.02

card_rows = {
    'entity_row': 1,
    'title_row': 2,
    'header_row': 6,
    'first_data_row': 9}

card_columns = {
    'entity_col': 1,
    'title_col': 1,
    'period_col': 1,
    'document_col': 2,
    'debit_analytics_col': 3,
    'credit_analytics_col': 4,
    'debit_account_col': 5,
    'debit_amount_col': 6,
    'empty_1_col': 7,
    'credit_account_col': 8,
    'credit_amount_col': 9,
    'empty_2_col': 10,
    'saldo_type_col': 11,
    'saldo_amount_col': 12,
    'debit_total_col': 5,
    'credit_total_col': 8}

field_names = [
    'period',
    'document',
    'debit_analytics',
    'credit_analytics',
    'debit_account',
    'debit_amount',
    'empty_1',
    'credit_account',
    'credit_amount',
    'empty_2',
    'saldo_type',
    'saldo_amount']

document_names = [
    'bank_transaction_no',
    'bank_transaction_datetime',
    'bank_transaction_purpose']

debit_analytics_names = [
    'debit_analytics_part_1',
    'debit_analytics_part_2',
    'debit_analytics_part_3',
    'debit_analytics_part_4']

credit_analytics_names = [
    'credit_analytics_part_1',
    'credit_analytics_part_2',
    'credit_analytics_part_3',
    'credit_analytics_part_4']

period_month_field_name = 'period_month'

