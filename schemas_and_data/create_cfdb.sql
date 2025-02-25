DROP DATABASE IF EXISTS cfdb;
CREATE DATABASE cfdb;
USE cfdb;

DROP TABLE IF EXISTS contours;
CREATE TABLE contours (
    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    description VARCHAR(255) NOT NULL UNIQUE);

DROP TABLE IF EXISTS entities;
CREATE TABLE entities (
    id BIGINT(12) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    entity_1C_name VARCHAR(255) NOT NULL UNIQUE);

DROP TABLE IF EXISTS contours_entities;
CREATE TABLE contours_entities (
    contour_id TINYINT UNSIGNED NOT NULL,
    entity_id BIGINT(12) UNSIGNED NOT NULL,
    PRIMARY KEY (contour_id, entity_id),
    CONSTRAINT `contours_entities_contour_id_fk` FOREIGN KEY (`contour_id`) REFERENCES `contours` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT `contours_entities_entity_id_fk` FOREIGN KEY (`entity_id`) REFERENCES `entities` (`id`) ON DELETE RESTRICT ON UPDATE RESTRICT);

DROP TABLE IF EXISTS holders;
CREATE TABLE holders (
    id BIGINT(12) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    entity_id BIGINT(255) UNSIGNED NOT NULL UNIQUE);

DROP TABLE IF EXISTS activities;
CREATE TABLE activities (
    id TINYINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    activity_name VARCHAR(255) NOT NULL UNIQUE);

DROP TABLE IF EXISTS income_articles;
CREATE TABLE income_articles (
    id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    article_name VARCHAR(255) NOT NULL UNIQUE,
    is_investment TINYINT(1));

DROP TABLE IF EXISTS outcome_articles;
CREATE TABLE outcome_articles (
    id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    article_name VARCHAR(255) NOT NULL UNIQUE,
    is_investment TINYINT(1));

DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
    id VARCHAR(16) NOT NULL PRIMARY KEY,
    account_name VARCHAR(255));

DROP TABLE IF EXISTS periods;
CREATE TABLE periods (
    period_name VARCHAR(64) NOT NULL PRIMARY KEY,
    beginning_date DATE NOT NULL,
    ending_date DATE NOT NULL);

DROP TABLE IF EXISTS cards;
CREATE TABLE cards (
    id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    account_no VARCHAR(16) NOT NULL,
    holder_id BIGINT(12) UNSIGNED NOT NULL,
    title VARCHAR(255) NOT NULL,
    beginning_date DATE NOT NULL,
    ending_date DATE NOT NULL,
    card_file_name VARCHAR(255) NOT NULL,
    debit_total_value DECIMAL(15, 2) NOT NULL,
    credit_total_value DECIMAL(15, 2) NOT NULL,
    debit_sum DECIMAL(15, 2) NOT NULL,
    credit_sum DECIMAL(15, 2) NOT NULL,
    transactions_count INT UNSIGNED NOT NULL);

DROP TABLE IF EXISTS outcomes;
CREATE TABLE outcomes (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    holder_id BIGINT(12) UNSIGNED NOT NULL,
    entity_id BIGINT(12) UNSIGNED,
    account_id VARCHAR(16) NOT NULL,
    date DATE NOT NULL,
    period_name CHAR(7) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    article_id SMALLINT UNSIGNED NOT NULL,
    bank_transaction_no VARCHAR(255),
    bank_transaction_datetime DATETIME,
    bank_transaction_purpose VARCHAR(255),
    debit_analytics_part_1 VARCHAR(255),
    debit_analytics_part_2 VARCHAR(255),
    debit_analytics_part_3 VARCHAR(255),
    debit_analytics_part_4 VARCHAR(255),
    credit_analytics_part_1 VARCHAR(255),
    credit_analytics_part_2 VARCHAR(255),
    credit_analytics_part_3 VARCHAR(255),
    credit_analytics_part_4 VARCHAR(255));

DROP TABLE IF EXISTS incomes;
CREATE TABLE incomes (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    holder_id BIGINT(12) UNSIGNED NOT NULL,
    entity_id BIGINT(12) UNSIGNED,
    account_id VARCHAR(16) NOT NULL,
    date DATE NOT NULL,
    period_name CHAR(7) NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    article_id SMALLINT UNSIGNED NOT NULL,
    bank_transaction_no VARCHAR(255),
    bank_transaction_datetime DATETIME,
    bank_transaction_purpose VARCHAR(255),
    debit_analytics_part_1 VARCHAR(255),
    debit_analytics_part_2 VARCHAR(255),
    debit_analytics_part_3 VARCHAR(255),
    debit_analytics_part_4 VARCHAR(255),
    credit_analytics_part_1 VARCHAR(255),
    credit_analytics_part_2 VARCHAR(255),
    credit_analytics_part_3 VARCHAR(255),
    credit_analytics_part_4 VARCHAR(255));

ALTER TABLE holders
    ADD CONSTRAINT holders_entity_id_fk FOREIGN KEY
    (entity_id)
    REFERENCES entities(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE cards
    ADD CONSTRAINT cards_holder_id_fk FOREIGN KEY
    (holder_id)
    REFERENCES holders(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE cards
    ADD CONSTRAINT cards_account_no_fk FOREIGN KEY
    (account_no)
    REFERENCES accounts(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE outcomes
    ADD CONSTRAINT outcomes_holder_id_fk FOREIGN KEY
    (holder_id)
    REFERENCES holders(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE outcomes
    ADD CONSTRAINT outcomes_entity_id_fk FOREIGN KEY
    (entity_id)
    REFERENCES entities(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE outcomes
    ADD CONSTRAINT outcomes_account_id_fk FOREIGN KEY
    (account_id)
    REFERENCES accounts(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE outcomes
    ADD CONSTRAINT outcomes_period_name_fk FOREIGN KEY
    (period_name)
    REFERENCES periods(period_name)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE outcomes
    ADD CONSTRAINT outcomes_article_id_fk FOREIGN KEY
    (article_id)
    REFERENCES outcome_articles(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE incomes
    ADD CONSTRAINT incomes_holder_id_fk FOREIGN KEY
    (holder_id)
    REFERENCES holders(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE incomes
    ADD CONSTRAINT incomes_entity_id_fk FOREIGN KEY
    (entity_id)
    REFERENCES entities(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE incomes
    ADD CONSTRAINT incomes_account_id_fk FOREIGN KEY
    (account_id)
    REFERENCES accounts(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE incomes
    ADD CONSTRAINT incomes_period_name_fk FOREIGN KEY
    (period_name)
    REFERENCES periods(period_name)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE incomes
    ADD CONSTRAINT incomes_article_id_fk FOREIGN KEY
    (article_id)
    REFERENCES income_articles(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT;

ALTER TABLE entities
ADD COLUMN inn BIGINT(12) UNSIGNED;

ALTER TABLE entities
ADD COLUMN full_name VARCHAR(255);

