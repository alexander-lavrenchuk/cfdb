1. Склонировать репозиторий с GitHub:
git clone git@github.com:alexander-lavrenchuk/cfdb.git proj

2. Перейти в папку проекта:
cd proj
Далее в настоящей инструкции пути к файлам и папкам указаны относительно созданной папки проекта

3. Создать директории loads и rpts,
в которых будут размещены
карточки сч 51 и справочник контрагентов (директория loads) и
сгенерированные отчёты CF в эксель файлах (директория rpts)
mkdir loads rpts

4. Разместить в директорию loads карточки сч 51 и справочник контрагентов с ИНН

5. Переименовать файл config/db_connection_config_example.py в config/db_connection_config.py
Указать в файле config/db_connection_config.py наименование вновь создаваемой базы данных и
параметры подключения к ней.
Далее в настоящей инструкции предполагается наименование вновь создаваемой базы данных - cfdb.

6. Создать виртуальное окружение и активировать его:
python3 -m venv venv
source venv/bin/activate

7. Установить необходимые пакеты (из перечня ./notes/requirenments.txt):
pip3 install et_xmlfile greenlet mysql-connector-python numpy openpyxl pandas PyMySQL python-dateutil pytz six SQLAlchemy typing_extensions tzdata

8. Скопировать в папку loads эксель файлы с карточками по счетам 51 и спрвочником контрагентов:
cp <путь_к_эксель_файлам_с_карточками>/* loads/

9. Создать новую базуданных cfdb:
mysqladmin -u user -p create cfdb
У пользователя user должны быть права на создание базы данных и таблиц

или в клиенте mysql выполнит команду:
mysql> CREATE DATABASE cfdb;

10. Загрузить дамп базы данных из файла ./schemas_and_data/create_cfdb.sql
mysql -u user -p cfdb < ./schemas_and_data/create_cfdb.sql

11. Загрузить в базу данных cfdb данные из эксель файлов с карточками сч.51:
python3 fill_db.py

-- 12. Скопировать стандартный план счетов в папку с файлами mysql:
sudo cp ./schemas_and_data/accounts_plan.csv /var/lib/mysql-files/acconts_plan.csv

-- 13. Загрузить в базу данных план счетов и обновить наименования счетов в соответствии со стандартным планом счетов:
mysql -u user -p cfdb < ./schemas_and_data/load_accounts_plan_and_update_accounts.sql

-- 14. Заполнить таблицу activities:
mysql -u user -p cfdb < ./schemas_and_data/insert_activities.sql

-- 15. При необходимости обновить данные таблиц outcome_articles и income_articles, указав признак инвестиционных статей (см. пример ./schemas_and_data/update_cf_articles.sql)
mysql -u user -p cfdb < ./schemas_and_data/update_cf_articles.sql

16. Загрузить справочник контрагентов с ИНН из папки loads:
python3 ./insert_entities_with_inn.py

17. Ввести контуры в таблицу contours:
mysql -u user -p cfdb < ./schemas_and_data/insert_contours.sql

18. Указать отношение контрагентов к контурам:
mysql -u user -p cfdb < ./schemas_and_data/insert_contours_entities.sql

19. Создать временные таблицы для генерации листов с CF итогового отчёта:
mysql -u user -p cfdb < ./schemas_and_data/create_tables_by_activity.sql
Дождаться окончания создания таблиц

20. Запустить скрипт создания отчётов CF в эксель файлы в папку ./rpts:
python3 ./make_cf.py
Дождаться окончания создания отчётов

21. Удалить временные таблицы для генерации листов с CF итогового отчёта:
mysql -u user -p cfdb < ./schemas_and_data/drop_tables_by_activity.sql










Доработать скрипт создания отчётов CF, добавить генерацию двухфакторных отчётов:
по счетам учёта и по контрагентам,
по контрагентам и счетам учёта,
по статье учёта и контрагентам,
по контрагентам и статье учёта,
по счетам и статье учёта,
по статье учёта и счетам

Использовать в эксель-отчётах наименования видов деятельности из таблицы activities





Заменить соответствующие пункты инструкции на упрощённую процендуру:
У1. Создать новую базуданных cfdb:
CREATE DATABASE cfdb;

У2. Создать папку для дампов базы данных (внутри папки проекта):
mkdir dumps

У3. Перейти в папку dumps:
cd dumps

У4. Скопировать в папку dumps файл со схемой базы данных:
cp <путь_к_файлу_со_схемой_базы_данных>cfdb.sql cfdb.sql

У5. Создать схему базы данных с пустыми таблицами:
mysql -u user -p cfdb < cfdb.sql
где cfdb - имя вновь создаваемой базы данных

У6. В mysql выбрать новую базу данных:
USE cfdb;

