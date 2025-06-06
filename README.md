# factory-inspectors-db-etl
# ETL-скрипт для данных о фабричных инспекторах

## О проекте

Этот проект представляет собой Python-скрипт, предназначенный для извлечения (Extract), преобразования (Transform) и загрузки (Load) данных о личном составе фабричной инспекции Российской империи за период 1901-1913 гг. из набора HTML-файлов в структурированную базу данных PostgreSQL.

Целью проекта является создание просопографической базы данных для последующего анализа социального происхождения, образования, региональной принадлежности и карьерных перемещений инспекторов.

## Функциональность

*   **Подключение и настройка БД:** Автоматическое подключение к базе данных PostgreSQL и создание/пересоздание необходимой схемы таблиц при запуске.
*   **Парсинг HTML:** Чтение и разбор HTML-файлов с табличными данными с использованием библиотеки `BeautifulSoup4`.
*   **Стандартизация данных:**
    *   Приведение текстовых данных к единому формату (нижний регистр, замена дореформенных символов, унификация сокращений).
    *   Очистка и преобразование числовых значений.
*   **Извлечение сущностей:**
    *   Разбор сложных текстовых описаний персонала для выделения ФИО, чина, профессии, образования, дат службы, информации о вакансиях и исполнении обязанностей.
    *   Обработка ячеек с атрибутом `rowspan` для корректного соотнесения данных.
*   **Работа со справочниками:**
    *   Создание и пополнение справочных таблиц для инспекторов, местоположений, чинов, профессий и образований.
    *   Реализован механизм кэширования и проверки на дубликаты для оптимизации вставок и обеспечения уникальности записей в справочниках.
*   **Загрузка данных:** Формирование и вставка данных в основную таблицу назначений (`Assignments`), связывающую инспекторов с их должностями, местами службы и характеристиками по годам.
*   **Логирование:** Подробное логирование всего процесса обработки, включая этапы подключения, парсинга, извлечения данных, а также возникающие предупреждения и ошибки.

## Используемые технологии

*   **Python 3**
*   **psycopg2:** Драйвер для работы с PostgreSQL.
*   **BeautifulSoup4 (с парсером lxml):** Для парсинга HTML-документов.
*   **re (регулярные выражения):** Для сложного разбора текстовых строк.
*   **logging:** Стандартный модуль Python для логирования.
*   **os, html:** Стандартные модули Python для работы с файловой системой и HTML-сущностями.
*   **PostgreSQL:** В качестве целевой реляционной базы данных.

## Структура базы данных

База данных состоит из следующих основных таблиц:

*   `Inspectors`: Информация об инспекторах.
*   `Locations`: Справочник географических местоположений.
*   `Ranks`: Справочник чинов и званий.
*   `Professions`: Справочник профессий и специальностей.
*   `Educations`: Справочник учебных заведений и степеней.
*   `Assignments`: Основная таблица, связывающая инспекторов, их должности, места службы и другие атрибуты по годам.

## Результат

Разработанный ETL-скрипт позволяет автоматизировать процесс сбора и структурирования исторических данных из разрозненных HTML-источников, создавая ценную основу для дальнейших исследований в области исторической просопографии и анализа государственной службы.

## Использование

1.  **Настройка:**
    *   Убедитесь, что у вас установлен Python 3 и необходимые библиотеки (`psycopg2-binary`, `beautifulsoup4`, `lxml`).
    *   Настройте параметры подключения к базе данных PostgreSQL в начале скрипта (`DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`).
    *   Укажите путь к папке с HTML-файлами в переменной `HTML_FOLDER` и список файлов в `HTML_FILES`.
2.  **Запуск:**
    *   Запустите скрипт:
        ```bash
        python3 populate_db_ru_v1.py
        ```
    *   Скрипт создаст (или пересоздаст) таблицы в указанной базе данных и начнет обработку HTML-файлов.
    *   Прогресс и возможные ошибки будут выводиться в консоль благодаря системе логирования.
