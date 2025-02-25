DROP TABLE IF EXISTS accounts_plan;
CREATE TABLE accounts_plan LIKE accounts;

LOAD DATA INFILE '/var/lib/mysql-files/accounts_plan.csv'
INTO TABLE accounts_plan
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

UPDATE accounts AS ac, accounts_plan AS ap
SET ac.account_name = ap.account_name
WHERE ac.id = ap.id;


-- См. путь к папке, из которой MySQL будет загружать файлы в этой переменной:
-- SHOW VARIABLES LIKE 'secure_file_priv';
