SET @contour_id = 1;

-- OUTCOMES
-- only Equity funds
CREATE TABLE outs_equity AS
SELECT * FROM outcomes
WHERE account_id IN ('000', '51', '55.03', '57.01');

-- without Equity funds
CREATE TABLE outs_wo_equity AS
SELECT * FROM outcomes
WHERE account_id NOT IN ('000', '51', '55.03', '57.01');


-- without Equity funds
-- only VGO
CREATE TABLE outs_vgo AS
SELECT * FROM outs_wo_equity AS t1
WHERE EXISTS
(SELECT 1 FROM contours_entities AS t2
    WHERE t2.contour_id = @contour_id AND t2.entity_id = t1.entity_id);

-- without Equity funds
-- without VGO
CREATE TABLE outs_wo_vgo AS
SELECT * FROM outs_wo_equity AS t1
WHERE NOT EXISTS
(SELECT 1 FROM contours_entities AS t2
    WHERE t2.contour_id = @contour_id AND t2.entity_id = t1.entity_id);


-- without VGO,
-- without Equity funds,
-- only financial activities
CREATE TABLE outs_fin AS
SELECT * FROM outs_wo_vgo
WHERE account_id IN ('66.01', '66.02', '66.03', '66.04',
                     '67.01', '67.02', '67.03', '67.04',
                     '75.01', '75.02', '84.01', '86.01');

-- without VGO,
-- without Equity funds,
-- without financial activities
CREATE TABLE outs_wo_fin AS
SELECT * FROM outs_wo_vgo
WHERE account_id NOT IN ('66.01', '66.02', '66.03', '66.04',
                         '67.01', '67.02', '67.03', '67.04',
                         '75.01', '75.02', '84.01', '86.01');


-- without VGO,
-- without Equity funds,
-- without financial activities
-- only investment activities
CREATE TABLE outs_inv_1 AS
SELECT * FROM outs_wo_fin
WHERE account_id IN ('07', '08.03', '58.03', '68.02');

CREATE TABLE outs_inv_2 AS
SELECT * FROM outs_wo_fin AS t1
WHERE account_id IN ('60.01', '60.02', '62.01', '62.02')
AND (SELECT is_investment FROM outcome_articles AS t2 WHERE t2.id = t1.article_id);

CREATE TABLE outs_inv AS
SELECT * FROM outs_inv_1
UNION
SELECT * FROM outs_inv_2;

-- without VGO,
-- without Equity funds,
-- without financial activities
-- without investment activities
-- only operating activities
CREATE TABLE outs_op AS
SELECT * FROM outs_wo_fin
WHERE NOT EXISTS
(SELECT 1 FROM outs_inv WHERE outs_inv.id = outs_wo_fin.id);



-- DROP TABLE outs_vgo;
-- DROP TABLE outs_wo_vgo;
-- DROP TABLE outs_equity;
-- DROP TABLE outs_wo_equity;
-- DROP TABLE outs_fin;
-- DROP TABLE outs_wo_fin;
-- DROP TABLE outs_inv_1;
-- DROP TABLE outs_inv_2;
-- DROP TABLE outs_inv;
-- DROP TABLE outs_op;


-- INCOMES
-- only Equity funds
CREATE TABLE ins_equity AS
SELECT * FROM incomes
WHERE account_id IN ('000', '51', '55.03', '57.01');

-- without Equity funds
CREATE TABLE ins_wo_equity AS
SELECT * FROM incomes
WHERE account_id NOT IN ('000', '51', '55.03', '57.01');


-- without Equity funds
-- only VGO
CREATE TABLE ins_vgo AS
SELECT * FROM ins_wo_equity AS t1
WHERE EXISTS
(SELECT 1 FROM contours_entities AS t2
    WHERE t2.contour_id = @contour_id AND t2.entity_id = t1.entity_id);

-- without Equity funds
-- without VGO
CREATE TABLE ins_wo_vgo AS
SELECT * FROM ins_wo_equity AS t1
WHERE NOT EXISTS
(SELECT 1 FROM contours_entities AS t2
    WHERE t2.contour_id = @contour_id AND t2.entity_id = t1.entity_id);


-- without VGO,
-- without Equity funds,
-- only financial activities
CREATE TABLE ins_fin AS
SELECT * FROM ins_wo_vgo
WHERE account_id IN ('66.01', '66.02', '66.03', '66.04',
                     '67.01', '67.02', '67.03', '67.04',
                     '75.01', '75.02', '84.01', '86.01');

-- without VGO,
-- without Equity funds,
-- without financial activities
CREATE TABLE ins_wo_fin AS
SELECT * FROM ins_wo_vgo
WHERE account_id NOT IN ('66.01', '66.02', '66.03', '66.04',
                         '67.01', '67.02', '67.03', '67.04',
                         '75.01', '75.02', '84.01', '86.01');


-- without VGO,
-- without Equity funds,
-- without financial activities
-- only investment activities
CREATE TABLE ins_inv_1 AS
SELECT * FROM ins_wo_fin
WHERE account_id IN ('07', '08.03', '58.03', '68.02');

CREATE TABLE ins_inv_2 AS
SELECT * FROM ins_wo_fin AS t1
WHERE account_id IN ('60.01', '60.02', '62.01', '62.02')
AND (SELECT is_investment FROM income_articles AS t2 WHERE t2.id = t1.article_id);

CREATE TABLE ins_inv AS
SELECT * FROM ins_inv_1
UNION
SELECT * FROM ins_inv_2;

-- without VGO,
-- without Equity funds,
-- without financial activities
-- without investment activities
-- only operating activities
CREATE TABLE ins_op AS
SELECT * FROM ins_wo_fin
WHERE NOT EXISTS
(SELECT 1 FROM ins_inv WHERE ins_inv.id = ins_wo_fin.id);


-- DROP TABLE ins_vgo;
-- DROP TABLE ins_wo_vgo;
-- DROP TABLE ins_equity;
-- DROP TABLE ins_wo_equity;
-- DROP TABLE ins_fin;
-- DROP TABLE ins_wo_fin;
-- DROP TABLE ins_inv_1;
-- DROP TABLE ins_inv_2;
-- DROP TABLE ins_inv;
-- DROP TABLE ins_op;




-- PS
-- INTO OUTFILE '/var/lib/mysql-files/file_name.tsv'
-- FIELDS TERMINATED BY '\t' ENCLOSED BY '' LINES TERMINATED BY '\n';

