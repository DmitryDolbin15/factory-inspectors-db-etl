import os
import re
import psycopg2
from bs4 import BeautifulSoup, NavigableString
import logging
import html

# --- Конфигурация ---
DB_NAME = "factory_inspectors_db"
DB_USER = "macbook"
DB_PASSWORD = ""
DB_HOST = "localhost"
DB_PORT = "5432"

HTML_FOLDER = "/Users/macbook/diplom"
HTML_FILES = [
    'fabric1901.html',
    'fabric1902.html', 'fabric1903.html', 'fabric1904.html',
    'fabric1905.html', 'fabric1906.html', 'fabric1907.html', 'fabric1909.html',
    'fabric1910.html', 'fabric1912.html', 'fabric1913.html'
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s')

location_cache = {}
rank_cache = {}
profession_cache = {}
education_cache = {}
inspector_cache = {}

# Глобальные переменные для rowspan
rowspan_personnel_content = None 
rowspan_personnel_counter = 0
rowspan_location_text = None    
rowspan_location_counter = 0


def standardize_text(text):
    if text is None: return None
    text_val = str(text).strip()
    if not text_val: return None
    text_val = text_val.lower()
    
    text_val = text_val.replace('с.-петербургъ', 'с.-петербург')
    text_val = text_val.replace('с.-петербургь', 'с.-петербург')
    text_val = text_val.replace('с. петербургъ', 'с.-петербург')
    text_val = text_val.replace('с. петербург', 'с.-петербург')
    text_val = text_val.replace('спб.', 'с.-петербург')
    text_val = text_val.replace('нижній-новгородъ', 'нижній-новгород')
    text_val = text_val.replace('нижній новгородъ', 'нижній-новгород')
    text_val = text_val.replace('нахичевань н/д.', 'нахичевань-на-дону')
    text_val = text_val.replace('нахичевань н/д', 'нахичевань-на-дону')
    text_val = text_val.replace('в.-волочокъ', 'вышній-волочек')
    text_val = text_val.replace('вышн.-волочокъ', 'вышній-волочек')
    text_val = text_val.replace('иваново-вознесенскь', 'иваново-вознесенск')
    text_val = text_val.replace('инж.-гех', 'инж.-тех') 

    replacements_char = {'ѣ': 'е', 'і': 'и', 'ѳ': 'ф', 'ї': 'и', 'ѵ': 'и'}
    text_list = list(text_val)
    for i, char_item in enumerate(text_list):
        if char_item in replacements_char:
            text_list[i] = replacements_char[char_item]
    text_val = "".join(text_list)
    
    text_val = re.sub(r'ъ(?=\s|$|[.,;:!?])', '', text_val)
    text_val = re.sub(r'ь(?=\s|$|[.,;:!?])', '', text_val)
    
    text_val = text_val.replace('инж. техн.', 'инж.-техн.')
    text_val = text_val.replace('инж. тех.', 'инж.-тех.')
    text_val = text_val.replace('инж. мех.', 'инж.-мех.')
    text_val = text_val.replace('д-ръ мед.', 'д-р мед') 
    text_val = text_val.replace('д-ръ мед', 'д-р мед')
    text_val = text_val.replace('уч. инж.-тех.', 'уч. инж.-тех.')

    text_val = re.sub(r'\s*\.\s*\.(?!\s*[а-яё])', '.', text_val)
    text_val = re.sub(r'\s*\.(?=\s|$)', '.', text_val) 
    text_val = re.sub(r'\s*,\s*', ', ', text_val) 
    text_val = re.sub(r'\s+', ' ', text_val).strip()
    
    # УДАЛЕНИЕ КОНЕЧНОЙ ТОЧКИ/ЗАПЯТОЙ
    if text_val.endswith('.') or text_val.endswith(','):
        if not (re.search(r'\b[а-яё]\.$', text_val) or
                re.search(r'\b[а-яё]\.\s*[а-яё]\.$', text_val) or
                re.fullmatch(r'[а-яё]{1,3}\.', text_val) or
                text_val in ['с.', 'г.', 'д.', 'у.', 'м.'] 
               ):
            text_val = text_val[:-1].strip()

    return text_val if text_val else None


KNOWN_RANKS = {
    standardize_text(rank): name for rank, name in {
        'д. с. с.': 'Дѣйствительный статскій совѣтникъ', 'д.ст.сов.': 'Дѣйствительный статскій совѣтникъ', 'д с с': 'Дѣйствительный статскій совѣтникъ',
        'с. с.': 'Статскій совѣтникъ', 'ст. сов.': 'Статскій совѣтникъ', 'с с': 'Статскій совѣтникъ', 'ст. с.': 'Статскій совѣтникъ', 'стат. сов.': 'Статскій совѣтникъ',
        'к. с.': 'Коллежскій совѣтникъ', 'колл. сов.': 'Коллежскій совѣтникъ', 'колл сов': 'Коллежскій совѣтникъ', 'к с': 'Коллежскій совѣтникъ', 'к. сов.': 'Коллежскій совѣтникъ', 'кол. сов.': 'Коллежскій совѣтникъ', 'колл сов.': 'Коллежскій совѣтникъ', 'колл. сов': 'Коллежскій совѣтникъ',
        'н. с.': 'Надворный совѣтникъ', 'н с': 'Надворный совѣтникъ', 'надв. сов.': 'Надворный совѣтникъ', 'надв сов': 'Надворный совѣтникъ', 'н. сов.': 'Надворный совѣтникъ',
        'к. а.': 'Коллежскій асессоръ', 'колл. асс.': 'Коллежскій асессоръ', 'колл асс': 'Коллежскій асессоръ', 'к. ас.': 'Коллежскій асессоръ', 'к а': 'Коллежскій асессоръ', 'к. асс.': 'Коллежскій асессоръ', 'кол. асс.': 'Коллежскій асессоръ',
        'тт. с.': 'Титулярный совѣтникъ', 'тт с': 'Титулярный совѣтникъ', 'тит. сов.': 'Титулярный совѣтникъ', 'тит сов': 'Титулярный совѣтникъ', 'тт. сов.': 'Титулярный совѣтникъ', 'тит. с.': 'Титулярный совѣтникъ', 'т. с.': 'Титулярный совѣтникъ',
        'тг. с.': 'Титулярный совѣтникъ', 'т1. с.': 'Титулярный совѣтникъ', 'тІ. с.': 'Титулярный совѣтникъ',
        'к. ск.': 'Коллежскій секретарь', 'к ск': 'Коллежскій секретарь', 'колл. секр.': 'Коллежскій секретарь', 'колл секр': 'Коллежскій секретарь', 'к. секр.': 'Коллежскій секретарь', 'к. скр.': 'Коллежскій секретарь', 'колл. скр.': 'Коллежскій секретарь', 'кол. секр.': 'Коллежскій секретарь',
        'г. с.': 'Губернскій секретарь', 'губ. секр.': 'Губернскій секретарь', 'г. ск.': 'Губернскій секретарь', 'губ секр': 'Губернскій секретарь', 'губ. скр.': 'Губернскій секретарь',
        'колл. рег.': 'Коллежскій регистраторъ', 'к. рег.': 'Коллежскій регистраторъ', 'к. р.': 'Коллежскій регистраторъ', 'кол. рег.': 'Коллежскій регистраторъ',
        'н. ч.': 'Неимѣющій чина', 'неим. чина': 'Неимѣющій чина',
        'чт. с.': 'Чиновникъ (требует уточнения)', 'чт с': 'Чиновникъ (требует уточнения)',
        'к. св.': 'Коллежскій совѣтникъ (предположительно)',
        'п. с.': 'Почетный совѣтникъ (предположительно)',
        '1г. с.': 'Губернскій секретарь (1-го разряда?)', '1r. c.': 'Губернскій секретарь (1-го разряда? лат.)',
        'г. с. к.': 'Губернскій секретарь (требует уточнения)',
        'гл. с.': 'Губернскій секретарь (предположительно)',
        'в. с.': 'Военный совѣтникъ (предположительно)',
    }.items()
}
KNOWN_PROFESSIONS = {
    standardize_text(prof): name for prof, name in {
        'уч. инж.-тех.': 'Ученый инженеръ-технологъ',
        'инж.-тех.': 'Инженеръ-технологъ', 'инж.-техн.': 'Инженеръ-технологъ', 'инж тех': 'Инженеръ-технологъ',
        'инж-тех.': 'Инженеръ-технологъ', 'инж техн': 'Инженеръ-технологъ', 'инж.тех.': 'Инженеръ-технологъ',
        'инж.-мех.': 'Инженеръ-механикъ', 'инж-мех.': 'Инженеръ-механикъ', 'инж мех': 'Инженеръ-механикъ', 'инж.-мех. флота': 'Инженеръ-механикъ',
        'тех.': 'Техникъ', 'техн.': 'Техникъ',
        'инж.-хим.': 'Инженеръ-химикъ',
        'горн. инж.': 'Горный инженеръ', 'горный инж.': 'Горный инженеръ', 'горн инж': 'Горный инженеръ', 'гор. инж.': 'Горный инженеръ',
        'д-р мед': 'Докторъ медицины', 'др. мед.': 'Докторъ медицины', 'д-ръ мед': 'Докторъ медицины',
        'врачъ': 'Врачъ',
        'мех.-стр.': 'Механикъ-строитель',
        'воен. инж.': 'Военный инженеръ', 'воен инж': 'Военный инженеръ',
        'кораб. инж.': 'Корабельный инженеръ',
        'рудн.-инж.': 'Рудничный инженеръ',
        'инж. стр.': 'Инженеръ-строитель',
        'технологъ': 'Технологъ',
        'инж.-металлургъ': 'Инженеръ-металлургъ', 'инж. металлургъ': 'Инженеръ-металлургъ', 'инж. металл.': 'Инженеръ-металлургъ',
        'лейт. зап. фл.': 'Лейтенантъ запаса флота', 'лейт зап фл': 'Лейтенантъ запаса флота',
        'инж.': 'Инженеръ (общее)', 
    }.items()
}
KNOWN_EDUCATIONS = {
    standardize_text(edu): name for edu, name in {
        'мих. арт. акад.': 'Михайловская Артиллерійская Академія',
        'канд. физ.-мат. наукъ': 'Кандидатъ физико-математическихъ наукъ',
        'канд. физ.-мат. факул.': 'Кандидатъ физико-математическаго факультета',
        'канд. физ. мат. наукъ': 'Кандидатъ физико-математическихъ наукъ',
        'канд. физ.-мат. фак.': 'Кандидатъ физико-математическаго факультета',
        'канд. унив.': 'Кандидатъ университета',
        'канд. матем. наукъ': 'Кандидатъ математическихъ наукъ',
        'канд. мат. наукъ': 'Кандидатъ математическихъ наукъ',
        'канд. естеств. наукъ': 'Кандидатъ естественныхъ наукъ',
        'канд. экон. наукъ': 'Кандидатъ экономическихъ наукъ', 'канд. эк. наукъ': 'Кандидатъ экономическихъ наукъ',
        'канд. эконом. наукъ': 'Кандидатъ экономическихъ наукъ',
        'дѣйст. студ. физико-мат. фак.': 'Дѣйствительный студентъ физико-математическаго факультета',
        'дѣйст. студ.': 'Дѣйствительный студентъ', 
        'оконч. спб. полит. инст.': 'Окончившій С.-Петербургскій Политехническій Институтъ', 'канд. С.-П. политехникума': 'Кандидатъ в С.-Петербургскій Политехническій Институтъ', 'канд. спб. политех.': 'Кандитат въ С.-Петербургскій Политехническій Институтъ',
        'оконч. с.-петерб. полит. институтъ': 'Окончившій С.-Петербургскій Политехническій Институтъ',
        'оконч. СПБ. Политехн. инст. по эконом. отд': 'Окончившій С.-Петербургскій Политехническій Институтъ по экономическому отделениюю',
        'оконч. моск. ун. по юрид. фак.': 'Окончившій Московскій Университетъ по юридическому факультету',
        'окон. курсъ Москов. Коммерч. инст.': 'Окончившій курсы Московского Комерческого института',
        'институтъ': 'Институтъ (общее)', 'инст.': 'Институтъ (сокр.)'
    }.items()
}

SQL_SCHEMA = """
DROP TABLE IF EXISTS Assignments CASCADE; DROP TABLE IF EXISTS Inspectors CASCADE; DROP TABLE IF EXISTS Locations CASCADE; DROP TABLE IF EXISTS Ranks CASCADE; DROP TABLE IF EXISTS Professions CASCADE; DROP TABLE IF EXISTS Educations CASCADE;
CREATE TABLE Inspectors ( InspectorID SERIAL PRIMARY KEY, FullName VARCHAR(255) NOT NULL UNIQUE, Notes TEXT NULL );
CREATE TABLE Locations ( LocationID SERIAL PRIMARY KEY, CityName VARCHAR(255) NOT NULL, GuberniaName VARCHAR(255) NULL, OkrugName VARCHAR(255) NULL, LocationType VARCHAR(50) NULL, UNIQUE (CityName, GuberniaName, OkrugName) );
CREATE TABLE Ranks ( RankID SERIAL PRIMARY KEY, Abbreviation VARCHAR(100) UNIQUE NOT NULL, FullName_RU VARCHAR(100) NULL, RankType VARCHAR(50) NULL );
CREATE TABLE Professions ( ProfessionID SERIAL PRIMARY KEY, Abbreviation VARCHAR(150) UNIQUE NOT NULL, FullName_RU VARCHAR(150) NULL );
CREATE TABLE Educations ( EducationID SERIAL PRIMARY KEY, Abbreviation VARCHAR(150) UNIQUE NOT NULL, FullName_RU VARCHAR(255) NULL );
CREATE TABLE Assignments ( AssignmentID SERIAL PRIMARY KEY, InspectorID INT NULL REFERENCES Inspectors(InspectorID), Year INT NOT NULL, SourceFile VARCHAR(50) NOT NULL, OkrugName VARCHAR(255) NOT NULL, GuberniaName VARCHAR(255) NOT NULL, PositionRole VARCHAR(100) NOT NULL, UchastokIdentifier VARCHAR(100) NULL, UchastokDescription TEXT NULL, InspectorLocationID INT NULL REFERENCES Locations(LocationID), PersonnelRawString TEXT NULL, RankID INT NULL REFERENCES Ranks(RankID), ProfessionID INT NULL REFERENCES Professions(ProfessionID), EducationID INT NULL REFERENCES Educations(EducationID), StartDateInYearRaw VARCHAR(50) NULL, EndDateInYearRaw VARCHAR(50) NULL, IsActing BOOLEAN DEFAULT FALSE, IsVacancy BOOLEAN DEFAULT FALSE, AssignmentNotes TEXT NULL, EstablishmentsCount INT NULL, WorkerCount INT NULL, BoilerCount INT NULL );
CREATE INDEX idx_assignments_year ON Assignments (Year); CREATE INDEX idx_assignments_inspector ON Assignments (InspectorID); CREATE INDEX idx_assignments_location ON Assignments (InspectorLocationID); CREATE INDEX idx_assignments_gubernia ON Assignments (GuberniaName); CREATE INDEX idx_assignments_okrug ON Assignments (OkrugName);
"""

def get_db_connection():
    try: conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT); logging.info("Соединение с БД установлено."); return conn
    except Exception as e: logging.error(f"Ошибка подключения к БД: {e}"); return None

def setup_database(conn):
    try:
        with conn.cursor() as cur: cur.execute(SQL_SCHEMA)
        conn.commit(); logging.info("Схема БД создана/пересоздана.")
    except psycopg2.Error as e: logging.error(f"Ошибка при создании схемы БД: {e}"); conn.rollback(); raise

def get_or_create_lookup_id(conn, table_name, column_name, lookup_value_raw, cache_dict, full_name_ru_dict=None):
    std_key = standardize_text(lookup_value_raw)
    if not std_key: return None
    if std_key in cache_dict: return cache_dict[std_key]
    full_name_val = None
    if full_name_ru_dict:
        full_name_val = full_name_ru_dict.get(std_key)
        if not full_name_val:
            if std_key.endswith('.') and std_key[:-1] in full_name_ru_dict:
                full_name_val = full_name_ru_dict[std_key[:-1]]
            elif not std_key.endswith('.') and std_key + '.' in full_name_ru_dict:
                 full_name_val = full_name_ru_dict[std_key + '.']
    
    if full_name_ru_dict and not full_name_val and lookup_value_raw: 
        logging.warning(f"Для '{lookup_value_raw}' (станд. '{std_key}') не найдено полное имя в словаре {table_name}.")
    pk_col = f"{table_name[:-1] if table_name.endswith('s') else table_name}ID"
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT {pk_col} FROM {table_name} WHERE {column_name} = %s", (std_key,))
        res = cur.fetchone()
        if res: id_val = res[0]
        else:
            cols_to_insert = [column_name]; vals_to_insert = [std_key]; placeholders_insert = ["%s"]
            cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name='{table_name.lower()}' AND column_name='fullname_ru';")
            if cur.fetchone(): cols_to_insert.append("FullName_RU"); vals_to_insert.append(full_name_val); placeholders_insert.append("%s")
            cur.execute(f"INSERT INTO {table_name} ({', '.join(cols_to_insert)}) VALUES ({', '.join(placeholders_insert)}) RETURNING {pk_col}", tuple(vals_to_insert))
            id_val = cur.fetchone()[0]
            logging.info(f"Вставлено в {table_name}: '{std_key}' (FullName: '{full_name_val}', ID: {id_val})")
        cache_dict[std_key] = id_val
        return id_val
    except psycopg2.Error as e: logging.error(f"Ошибка в get_or_create_lookup_id ({table_name}, '{std_key}' из '{lookup_value_raw}'): {e}"); conn.rollback(); return None
    finally: cur.close()

def get_or_create_rank_id(conn, abbr_raw): return get_or_create_lookup_id(conn, 'Ranks', 'Abbreviation', abbr_raw, rank_cache, KNOWN_RANKS)
def get_or_create_profession_id(conn, abbr_raw):
    std_abbr = standardize_text(abbr_raw)
    if std_abbr in KNOWN_EDUCATIONS: return None
    return get_or_create_lookup_id(conn, 'Professions', 'Abbreviation', abbr_raw, profession_cache, KNOWN_PROFESSIONS)
def get_or_create_education_id(conn, abbr_raw): return get_or_create_lookup_id(conn, 'Educations', 'Abbreviation', abbr_raw, education_cache, KNOWN_EDUCATIONS)

def get_or_create_location(conn, city, gubernia=None, okrug=None, loc_type=None):
    std_city_raw = standardize_text(city); std_gubernia = standardize_text(gubernia); std_okrug = standardize_text(okrug)
    if not std_city_raw: logging.warning(f"Пустой город (губ: {std_gubernia}, окр: {std_okrug})."); return None
    std_city = std_city_raw.strip(" .,:;")
    cache_key = (std_city, std_gubernia, std_okrug)
    if cache_key in location_cache: return location_cache[cache_key]
    cur = conn.cursor()
    try:
        query_parts = ["SELECT LocationID FROM Locations WHERE lower(CityName) = lower(%s)"]
        params = [std_city]
        if std_gubernia: query_parts.append("lower(GuberniaName) = lower(%s)"); params.append(std_gubernia)
        else: query_parts.append("GuberniaName IS NULL")
        if std_okrug: query_parts.append("lower(OkrugName) = lower(%s)"); params.append(std_okrug)
        else: query_parts.append("OkrugName IS NULL")
        query = " AND ".join(query_parts)
        cur.execute(query, tuple(params))
        result = cur.fetchone()
        if result: location_id = result[0]
        else:
            city_to_insert = city.strip() if city else None
            gubernia_to_insert = gubernia.strip() if gubernia else None
            okrug_to_insert = okrug.strip() if okrug else None
            cur.execute(""" INSERT INTO Locations (CityName, GuberniaName, OkrugName, LocationType)
                            VALUES (%s, %s, %s, %s) RETURNING LocationID """,
                        (city_to_insert, gubernia_to_insert, okrug_to_insert, loc_type))
            location_id = cur.fetchone()[0]
            logging.info(f"Вставлено Местоположение: Город='{city_to_insert}', Губ='{gubernia_to_insert}', Округ='{okrug_to_insert}' (Тип: {loc_type}) с ID {location_id}")
        location_cache[cache_key] = location_id
        return location_id
    except psycopg2.Error as e: logging.error(f"Ошибка Loc для Г='{city}', Губ='{gubernia}', Окр='{okrug}': {e}"); conn.rollback(); return None
    finally: cur.close()

def clean_number(num_str):
    if num_str is None or isinstance(num_str, (int, float)): return num_str
    text = str(num_str).strip()
    if text == '—' or text == '-' or not text: return None
    try:
        cleaned_str = re.sub(r'[.,](?=\d{3})', '', text.replace('\xa0', ''))
        cleaned_str = re.sub(r'[^\d]', '', cleaned_str) 
        if not cleaned_str: logging.debug(f"Нет цифр для '{text}' после очистки '{cleaned_str}'"); return None
        return int(cleaned_str)
    except ValueError: logging.warning(f"Не число: '{num_str}' (ориг: '{text}')"); return None

def get_or_create_inspector_id(conn, full_name_raw):
    if not full_name_raw: return None
    std_name_intermediate = standardize_text(full_name_raw)
    if not std_name_intermediate: return None

    parts = std_name_intermediate.split()
    initials = []
    surname_parts = []
    for part in parts:
        if re.fullmatch(r"[а-яё]\.", part): initials.append(part)
        elif re.fullmatch(r"[а-яё]\.[а-яё]\.", part): initials.extend([part[:2], part[2:]])
        elif part.endswith('.'):
            if len(part) == 2 and part[0].isalpha(): initials.append(part)
            else: surname_parts.append(part)
        else: surname_parts.append(part)
    
    final_surname = " ".join(surname_parts)
    final_initials_parts = sorted([i.replace('.', '') for i in initials])
    final_initials_str = "".join([i + "." for i in final_initials_parts]) 

    if final_surname and final_initials_str: std_name = f"{final_surname} {final_initials_str}"
    elif final_surname: std_name = final_surname
    elif final_initials_str: std_name = final_initials_str 
    else: return None

    if std_name in inspector_cache: return inspector_cache[std_name]
    cur = conn.cursor()
    try:
        cur.execute("SELECT InspectorID FROM Inspectors WHERE FullName = %s", (std_name,))
        result = cur.fetchone()
        if result: inspector_id = result[0]
        else:
            cur.execute("INSERT INTO Inspectors (FullName) VALUES (%s) RETURNING InspectorID", (std_name,))
            inspector_id = cur.fetchone()[0]
            logging.info(f"Вставлен Инспектор: '{std_name}' (из '{full_name_raw}') с ID {inspector_id}")
        inspector_cache[std_name] = inspector_id
        return inspector_id
    except psycopg2.Error as e: logging.error(f"Ошибка Inspector для '{std_name}' (из '{full_name_raw}'): {e}"); conn.rollback(); return None
    finally: cur.close()

def parse_personnel_string_v4(raw_string_html):
    final_results = []
    if not raw_string_html: return final_results

    delimiter_br = "||BR_DELIMITER||"
    unescaped_html = html.unescape(raw_string_html)
    raw_string_no_br = re.sub(r'<br\s*/?>', delimiter_br, unescaped_html, flags=re.IGNORECASE)
    br_separated_parts_raw = raw_string_no_br.split(delimiter_br)

    date_pattern_ru = r"(?:(?P<prefix_date>съ|с|до|по)\s+(?P<day>\d{1,2})\s+(?P<month>[а-яѣію]+(?:(?:\.|ъ|ь)\s*)?))"
    vacancy_pattern_ru = r"\bвакансія\b|\bвакансия\b"
    acting_pattern_ru = r"\bи\. ?д\."
    note_pattern_ru = r"\(([^)]+)\)"
    death_sign = '†'
    neim_china_pattern_ru = r"\bнеим\.? чина\b"
    
    name_pattern_ru = re.compile(
        r"([А-ЯЁѢІѲ][а-яёѣіѳ]+(?:[-][А-ЯЁѢІѲ][а-яёѣіѳ]+)?\s+[А-ЯЁѢІѲ]\.\s*(?:[А-ЯЁѢІѲ]\.)?)"  
        r"|((?:[А-ЯЁѢІѲ]\.\s*){1,2}\s*[А-ЯЁѢІѲ][а-яёѣіѳ]+(?:[-][А-ЯЁѢІѲ][а-яёѣіѳ]+)?)"  
        r"|([А-ЯЁѢІѲ][а-яёѣіѳ]+(?:[-][А-ЯЁѢІѲ][а-яёѣіѳ]+)?)"  
    , re.IGNORECASE)

    sr_inspector_marker_std = standardize_text("старшій инспекторъ")
    sr_fabr_inspector_marker_std = standardize_text("старшій фабричный инспекторъ")

    for individual_assignment_text_raw in br_separated_parts_raw:
        current_segment_raw_for_log = individual_assignment_text_raw.strip()
        std_check_for_skip = standardize_text(current_segment_raw_for_log)

        if not current_segment_raw_for_log or current_segment_raw_for_log == '—' or \
           std_check_for_skip == standardize_text('(нет данных)') or \
           std_check_for_skip == standardize_text('(нетъ данныхъ)'):
            logging.debug(f"  Пропуск BR-сегмента (пустой/тире/нет данных): '{current_segment_raw_for_log}'")
            continue
        
        logging.debug(f"  Обработка BR-сегмента: '{current_segment_raw_for_log}'")
        
        # Убираем разделение по запятой для упрощения, каждый BR-сегмент = 1 назначение
        actual_sub_segments_to_process = [current_segment_raw_for_log.strip(" ,.")]

        for segment_text_original_loop in actual_sub_segments_to_process:
            if not segment_text_original_loop.strip() or segment_text_original_loop.strip() == '—': continue

            processed_segment_text_loop = standardize_text(segment_text_original_loop)
            if not processed_segment_text_loop: continue

            # Проверка на "старший инспекторъ" как специальную роль
            if processed_segment_text_loop == sr_inspector_marker_std or \
               processed_segment_text_loop == sr_fabr_inspector_marker_std:
                assignment_sr_insp = {'name': None, 'rank_abbr':None,'prof_abbr':None,'edu_abbr':None,
                                      'start_date_raw':None,'end_date_raw':None,
                                      'is_vacancy':False,'is_acting':False,'notes':None, 
                                      'special_role': "старший инспектор"}
                final_results.append(assignment_sr_insp)
                logging.debug(f"      ---> Добавлено назначение (спец.роль старший инспектор): {assignment_sr_insp}")
                continue 

            logging.debug(f"      Парсинг суб-сегмента: '{segment_text_original_loop}' (станд. '{processed_segment_text_loop}')")
            
            assignment = {'name':None,'rank_abbr':None,'prof_abbr':None,'edu_abbr':None,
                          'start_date_raw':None,'end_date_raw':None,
                          'is_vacancy':False,'is_acting':False,'notes':None, 'special_role': None}
            text_to_parse_current = processed_segment_text_loop
            
            notes_list_local = []
            def note_replacer_local(match):
                note_content = match.group(1).strip()
                if note_content == death_sign: assignment['notes'] = ((assignment.get('notes') or "") + "; Умеръ (†)").lstrip("; ")
                elif "см. выше" in note_content or "см. ниже" in note_content: assignment['notes'] = ((assignment.get('notes') or "") + f"; Ссылка: ({note_content})").lstrip("; ")
                else: notes_list_local.append(note_content)
                return "" 
            text_to_parse_current = re.sub(note_pattern_ru, note_replacer_local, text_to_parse_current).strip(" ,.")
            if notes_list_local: assignment['notes'] = ((assignment.get('notes') or "") + "; ".join(notes_list_local)).lstrip("; ")

            acting_match = re.search(acting_pattern_ru, text_to_parse_current, re.IGNORECASE)
            if acting_match: assignment['is_acting'] = True; text_to_parse_current = text_to_parse_current.replace(acting_match.group(0), "", 1).strip(" ,.")
            
            date_matches = list(re.finditer(date_pattern_ru, text_to_parse_current, re.IGNORECASE))
            date_spans_to_remove = []
            for dm in sorted(date_matches, key=lambda m: m.start()):
                prefix, day, month_raw = dm.group('prefix_date'), dm.group('day'), dm.group('month')
                month_std = standardize_text(month_raw.strip(' .ьъ'))
                date_str = f"{prefix} {day} {month_std}"
                if prefix in ["съ", "с"] and not assignment['start_date_raw']: assignment['start_date_raw'] = date_str; date_spans_to_remove.append(dm.span())
                elif prefix in ["до", "по"] and not assignment['end_date_raw']: assignment['end_date_raw'] = date_str; date_spans_to_remove.append(dm.span())
            
            temp_text_list = list(text_to_parse_current)
            for start, end in sorted(date_spans_to_remove, reverse=True):
                for i in range(start, end): temp_text_list[i] = ''
            text_to_parse_current = "".join(temp_text_list).strip(" ,.")
            
            vacancy_match = re.search(vacancy_pattern_ru, text_to_parse_current, re.IGNORECASE)
            if vacancy_match:
                assignment['is_vacancy'] = True; assignment['name'] = "вакансия"
                remaining_after_vacancy = text_to_parse_current.replace(vacancy_match.group(0), "", 1).strip(" ,.")
                if remaining_after_vacancy: assignment['notes'] = ((assignment.get('notes') or "") + f"; Доп. о вакансии: {remaining_after_vacancy}").lstrip("; ")
                text_to_parse_current = ""
            
            if not assignment['is_vacancy']:
                work_string_for_entities = text_to_parse_current
                
                neim_china_match = re.search(neim_china_pattern_ru, work_string_for_entities, re.IGNORECASE)
                if neim_china_match:
                    assignment['notes'] = ((assignment.get('notes') or "") + "; Неимѣющій чина").lstrip("; ")
                    work_string_for_entities = work_string_for_entities.replace(neim_china_match.group(0), "", 1).strip(" ,.")

                extracted_entities_log = []
                entity_extraction_order = [
                    (KNOWN_PROFESSIONS, 'prof_abbr', 'профессия'),
                    (KNOWN_EDUCATIONS, 'edu_abbr', 'образование'),
                    (KNOWN_RANKS, 'rank_abbr', 'чин') 
                ]

                for current_dict, category_key, log_name_cat in entity_extraction_order:
                    if assignment[category_key]: continue 
                    for dict_key_std in sorted(current_dict.keys(), key=len, reverse=True):
                        if not dict_key_std: continue
                        pattern_base = re.escape(dict_key_std)
                        if '-' in dict_key_std: pattern_base = pattern_base.replace(r'\-', r'[-\s]?')
                        patterns_to_try_entity = [r'(?<![а-яё0-9])\b' + pattern_base + r'\b(?![а-яё0-9])']
                        if dict_key_std.endswith('.'): patterns_to_try_entity.append(r'(?<![а-яё0-9])\b' + pattern_base[:-2] + r'\b(?![а-яё0-9])')
                        elif not dict_key_std.endswith('.'): patterns_to_try_entity.append(r'(?<![а-яё0-9])\b' + pattern_base + r'\.(?![а-яё0-9])')
                        
                        for p_str in patterns_to_try_entity:
                            entity_match_candidate = re.search(p_str, work_string_for_entities, re.IGNORECASE)
                            if entity_match_candidate:
                                assignment[category_key] = dict_key_std
                                extracted_entities_log.append(f"{log_name_cat}: '{dict_key_std}' (из '{entity_match_candidate.group(0)}')")
                                start_idx, end_idx = entity_match_candidate.span()
                                work_string_for_entities = work_string_for_entities[:start_idx] + work_string_for_entities[end_idx:]
                                work_string_for_entities = work_string_for_entities.strip(" ,.;")
                                break
                        if assignment[category_key]: break
                
                if extracted_entities_log: logging.debug(f"Извлеченные сущности: {'; '.join(extracted_entities_log)}. Остаток для имени: '{work_string_for_entities}'")
                
                text_for_name_extraction = work_string_for_entities.strip(" ,.;")
                if text_for_name_extraction:
                    name_match_obj = name_pattern_ru.search(text_for_name_extraction) 
                    if name_match_obj:
                        extracted_name_candidate = next((g for g in name_match_obj.groups() if g is not None), None)
                        if extracted_name_candidate:
                            assignment['name'] = standardize_text(extracted_name_candidate.strip(" ,."))
                            text_to_parse_current = text_for_name_extraction.replace(extracted_name_candidate, "", 1).strip(" ,.;") # Удаляем из *остатка для имени*
                            logging.debug(f"        Извлечено имя: '{assignment['name']}'. Оставшийся текст (после имени): '{text_to_parse_current}'")
                        else: text_to_parse_current = text_for_name_extraction
                    elif len(text_for_name_extraction.split()) <= 4 and len(text_for_name_extraction) > 1 and re.search(r'[а-яё]', text_for_name_extraction):
                         assignment['name'] = standardize_text(text_for_name_extraction)
                         text_to_parse_current = ""
                         logging.debug(f"        Имя (эвристика, остаток <=4 слов): {assignment['name']}")
                    else:
                        text_to_parse_current = text_for_name_extraction
                        if text_to_parse_current:
                            logging.warning(f"        Не удалось извлечь имя из остатка (после сущностей): '{text_to_parse_current}' для суб-сегмента: '{segment_text_original_loop}'")
                else: text_to_parse_current = "" 
            
            if not assignment['name'] and not assignment['is_vacancy'] and not assignment.get('special_role') and (assignment['start_date_raw'] or assignment['end_date_raw']):
                assignment['is_vacancy'] = True; assignment['name'] = "вакансия (предп. по датам)"
                logging.debug(f"      Предполагаем вакансию на основе дат для '{segment_text_original_loop}'")
            
            if text_to_parse_current.strip() == death_sign :
                assignment['notes'] = ((assignment.get('notes') or "") + "; Умеръ (†)").lstrip("; ")
                text_to_parse_current = ""

            if text_to_parse_current:
                final_note_prefix = "Доп. инфо"
                if not assignment['name'] and not assignment['is_vacancy'] and not assignment.get('special_role'):
                    logging.warning(f"        Финальный остаток без имени/вакансии: '{text_to_parse_current}' для суб-сегмента: '{segment_text_original_loop}'")
                    final_note_prefix = "Неразобр. остаток"
                new_note_part = f"{final_note_prefix}: {text_to_parse_current}"
                if new_note_part.strip() != final_note_prefix + ":":
                    assignment['notes'] = ((assignment.get('notes') or "") + "; " + new_note_part).lstrip("; ")

            # Добавляем результат, если есть имя, вакансия ИЛИ специальная роль "старший инспектор"
            if assignment['name'] or assignment['is_vacancy'] or assignment.get('special_role') == "старший инспектор":
                # Если это спец.роль "старший инспектор", но имя было случайно установлено как "старший инспекторъ", сбрасываем его, 
                # чтобы ID искался для реального старшего инспектора, а не для этой строки.
                if assignment.get('special_role') == "старший инспектор" and \
                   standardize_text(assignment.get('name')) in [sr_inspector_marker_std, sr_fabr_inspector_marker_std]:
                    assignment['name'] = None 
                
                final_results.append(assignment)
                logging.debug(f"      ---> Добавлено назначение: {assignment}")
            else: # Остальные случаи - логируем как пропущенные, если есть что логировать
                is_only_dates_or_handled_notes = (assignment['start_date_raw'] or assignment['end_date_raw'] or \
                                                 (assignment['notes'] and "Неразобр. остаток" not in str(assignment['notes']))) and \
                                                 not assignment['rank_abbr'] and not assignment['prof_abbr'] and \
                                                 not assignment['edu_abbr']
                
                if segment_text_original_loop and not is_only_dates_or_handled_notes and \
                   standardize_text(segment_text_original_loop) != standardize_text('(нет данных)') and \
                   'замѣщалъ кандидатъ' not in segment_text_original_loop.lower() and \
                   '(см. выше)' not in segment_text_original_loop.lower() and \
                   standardize_text(segment_text_original_loop) != '»':
                    logging.warning(f"  Не удалось извлечь имя/вакансию из суб-сегмента '{segment_text_original_loop}' (детали: { {k:v for k,v in assignment.items() if v} }), запись проигнорирована.")
                elif segment_text_original_loop: 
                     logging.debug(f"  Суб-сегмент '{segment_text_original_loop}' не содержит имени/вакансии (только даты/примечания или уже обработан, или это '»'), пропущен.")
    return final_results

def process_html_file(conn, filepath):
    global rowspan_personnel_content, rowspan_personnel_counter, rowspan_location_text, rowspan_location_counter

    year_match = re.search(r'fabric(\d{4})\.html', os.path.basename(filepath))
    if not year_match: logging.warning(f"Не год: {filepath}"); return
    year = int(year_match.group(1)); logging.info(f"Обработка {filepath} за {year} год...")
    try:
        with open(filepath, 'r', encoding='utf-8') as f: soup = BeautifulSoup(f, 'lxml')
    except Exception as e: logging.error(f"Ошибка чтения {filepath}: {e}"); return
    
    table = soup.find('table')
    if not table: logging.warning(f"Нет таблицы в {filepath}"); return
    
    tbody = table.find('tbody')
    rows = tbody.find_all('tr', recursive=False) if tbody else table.find_all('tr', recursive=False)
    if not rows: logging.warning(f"В файле {filepath} не найдены строки <tr>"); return

    current_okrug, current_gubernia, last_location_city_std, last_valid_gubernia_for_ditto_std = "Неизвестно", "Неизвестно", None, "Неизвестно"
    last_assigned_personnel_data_for_ditto = None
    current_senior_inspector_id_for_gubernia = None
    
    rowspan_personnel_content = None; rowspan_personnel_counter = 0
    rowspan_location_text = None; rowspan_location_counter = 0
    
    cur = conn.cursor()
    start_row_index = 0 
    header_rows_count = 0
    if table.find('thead'): header_rows_count += len(table.find('thead').find_all('tr', recursive=False))
    temp_rows_for_header_check = tbody.find_all('tr', recursive=False) if tbody else table.find_all('tr', recursive=False)
    initial_data_row_offset = 0
    for i, row_check in enumerate(temp_rows_for_header_check):
        if i < header_rows_count: continue
        th_in_row = row_check.find_all('th', recursive=False)
        td_in_row = row_check.find_all('td', recursive=False)
        if th_in_row: initial_data_row_offset = i + 1 - header_rows_count; continue
        if td_in_row:
            is_special_header = False
            row_class_list = row_check.get('class', [])
            # Проверяем первую реальную ячейку в строке на colspan
            first_td_is_colspan_header = False
            if td_in_row[0].name == 'td' and td_in_row[0].get('colspan') and len(td_in_row) == 1:
                first_td_is_colspan_header = True

            if any(cls in row_class_list for cls in ['section-header', 'district-header', 'governorate-header', 'okrug-header', 'oblast-header']) or first_td_is_colspan_header: 
                is_special_header = True
            
            if is_special_header: initial_data_row_offset = i + 1 - header_rows_count; continue
        break 
    start_row_index = header_rows_count + initial_data_row_offset
    logging.info(f"Старт строк данных с индекса {start_row_index} в {os.path.basename(filepath)}")

    for row_idx, row in enumerate(rows[start_row_index:]):
        current_row_log_num = row_idx + start_row_index + 1
        raw_cells_from_html = row.find_all('td', recursive=False) 
        first_cell_text_raw_header_check = raw_cells_from_html[0].get_text(strip=True) if raw_cells_from_html else ""
        header_text_for_std = re.sub(r'\[\*.*?\]', '', first_cell_text_raw_header_check).strip()
        header_cell_text_std = standardize_text(header_text_for_std)
        
        row_classes = row.get('class', [])
        is_header_row = False
        first_raw_cell_is_colspan = len(raw_cells_from_html) > 0 and raw_cells_from_html[0].name == 'td' and raw_cells_from_html[0].get('colspan') and len(raw_cells_from_html) == 1

        if 'okrug-header' in row_classes or 'district-header' in row_classes or \
           (header_cell_text_std and ('округъ' in header_cell_text_std or 'округа' in header_cell_text_std) and first_raw_cell_is_colspan):
            current_okrug = header_cell_text_std or "Неизвестно"; current_gubernia = "Неизвестно"; last_valid_gubernia_for_ditto_std = "Неизвестно"; last_assigned_personnel_data_for_ditto = None; current_senior_inspector_id_for_gubernia = None
            logging.info(f"--- Строка {current_row_log_num}: Округ: {current_okrug} ---"); is_header_row = True
        elif 'gubernia-header' in row_classes or 'oblast-header' in row_classes or \
             (header_cell_text_std and ('губернія' in header_cell_text_std or 'область' in header_cell_text_std) and first_raw_cell_is_colspan):
            gubernia_text_clean = header_cell_text_std.replace('губернія','').replace('область','').replace('губ.','').strip().rstrip('.')
            if gubernia_text_clean: current_gubernia = gubernia_text_clean; last_valid_gubernia_for_ditto_std = current_gubernia; last_assigned_personnel_data_for_ditto = None; current_senior_inspector_id_for_gubernia = None; logging.info(f"--- Стр {current_row_log_num}: Губ/Обл: {current_gubernia} ---")
            else: logging.warning(f"Стр {current_row_log_num}: Не извлечена Губ из: {first_cell_text_raw_header_check}")
            is_header_row = True
        elif year == 1901 and 'section-header' in row_classes: # Только для 1901, т.к. там section-header для всего
             if header_cell_text_std and ('округъ' in header_cell_text_std): current_okrug = header_cell_text_std; current_gubernia = "Неизвестно"; last_valid_gubernia_for_ditto_std = "Неизвестно"; last_assigned_personnel_data_for_ditto = None; current_senior_inspector_id_for_gubernia = None; logging.info(f"--- Строка {current_row_log_num}: Округ (1901): {current_okrug} ---"); is_header_row = True
             elif header_cell_text_std and ('губернія' in header_cell_text_std):
                 gubernia_text_clean = header_cell_text_std.replace('губернія','').strip().rstrip('.'); 
                 if gubernia_text_clean: current_gubernia = gubernia_text_clean; last_valid_gubernia_for_ditto_std = current_gubernia; last_assigned_personnel_data_for_ditto = None; current_senior_inspector_id_for_gubernia = None; logging.info(f"--- Стр {current_row_log_num}: Губ/Обл (1901): {current_gubernia} ---"); is_header_row = True
                 else: logging.warning(f"Стр {current_row_log_num}: Не извлечена Губ (1901) из: {first_cell_text_raw_header_check}")
             else: # Если section-header, но не округ/губерния - вероятно, пустая разделительная строка в 1901
                 logging.info(f"Строка {current_row_log_num}: Пропуск section-header (1901) без ключевых слов: {first_cell_text_raw_header_check[:100]}..."); is_header_row = True
        
        if is_header_row: continue
        effective_cells_content = []
        col_counter_for_raw_cells = 0
        expected_cols_for_schema = 4 if year == 1901 else 6
        
        if year == 1901: desc_cell_idx_schema, loc_cell_idx_schema, pers_cell_idx_schema = 1, 2, 3; stat_start_idx_schema = -1
        else: desc_cell_idx_schema, stat_start_idx_schema, loc_cell_idx_schema, pers_cell_idx_schema = 0, 1, 4, 5

        for current_col_schema_idx in range(expected_cols_for_schema):
            cell_content_to_add = None
            use_raw_cell = True

            if current_col_schema_idx == pers_cell_idx_schema and rowspan_personnel_counter > 0:
                if rowspan_personnel_content is not None:
                    cell_content_to_add = rowspan_personnel_content 
                    use_raw_cell = False
                    logging.debug(f"Стр {current_row_log_num}, кол {current_col_schema_idx}: Используем rowspan персонал: {str(cell_content_to_add)[:30]}")
            elif current_col_schema_idx == loc_cell_idx_schema and rowspan_location_counter > 0:
                if rowspan_location_text is not None:
                    cell_content_to_add = rowspan_location_text 
                    use_raw_cell = False
                    logging.debug(f"Стр {current_row_log_num}, кол {current_col_schema_idx}: Используем rowspan локацию: {str(cell_content_to_add)[:30]}")

            if use_raw_cell:
                if col_counter_for_raw_cells < len(raw_cells_from_html):
                    current_raw_cell_obj = raw_cells_from_html[col_counter_for_raw_cells]
                    cell_content_to_add = current_raw_cell_obj.decode_contents(formatter="html") if current_col_schema_idx == pers_cell_idx_schema else current_raw_cell_obj.get_text(strip=True)
                    
                    if current_raw_cell_obj.name == 'td' and current_raw_cell_obj.get('rowspan'):
                        try:
                            span_val = int(current_raw_cell_obj['rowspan'])
                            if span_val > 1:
                                if current_col_schema_idx == pers_cell_idx_schema: 
                                    rowspan_personnel_content = cell_content_to_add 
                                    rowspan_personnel_counter = span_val 
                                    logging.debug(f"Стр {current_row_log_num}: Установлен rowspan={span_val} для ПЕРСОНАЛА.")
                                elif current_col_schema_idx == loc_cell_idx_schema: 
                                    rowspan_location_text = cell_content_to_add 
                                    rowspan_location_counter = span_val
                                    logging.debug(f"Стр {current_row_log_num}: Установлен rowspan={span_val} для МЕСТОПОЛОЖЕНИЯ.")
                        except ValueError: logging.warning(f"Стр {current_row_log_num}: Некорректный rowspan='{current_raw_cell_obj['rowspan']}' для кол {current_col_schema_idx}")
                    col_counter_for_raw_cells += 1
                else: 
                    cell_content_to_add = "" 
                    logging.debug(f"Стр {current_row_log_num}, схем.кол {current_col_schema_idx}: Добавлена пустышка (не хватило реальных ячеек HTML).")
            
            effective_cells_content.append(cell_content_to_add)
        
        if rowspan_personnel_counter > 0: rowspan_personnel_counter -=1
        if rowspan_location_counter > 0: rowspan_location_counter -=1
        if rowspan_personnel_counter == 0: rowspan_personnel_content = None
        if rowspan_location_counter == 0: rowspan_location_text = None

        if len(effective_cells_content) < expected_cols_for_schema:
             if any(str(c).strip() for c in effective_cells_content): 
                 logging.warning(f"Строка {current_row_log_num}: Пропуск (мало ячеек {len(effective_cells_content)}/{expected_cols_for_schema}): {[str(c)[:30] for c in effective_cells_content]}")
             continue
        
        # Логика пропуска строк-примечаний (снова, т.к. первая ячейка теперь из effective_cells_content)
        first_cell_text_raw_data = effective_cells_content[0] if effective_cells_content else ""
        if isinstance(first_cell_text_raw_data, NavigableString): first_cell_text_raw_data = str(first_cell_text_raw_data)

        if first_cell_text_raw_data.startswith(('*', ')', '1)', '*)', '**)', '***)', '****)')) or \
           ("примечание:" in first_cell_text_raw_data.lower()) or \
           ("въ пензенской губерніи:" in first_cell_text_raw_data.lower()) or \
           ("въ черноморской губ." in first_cell_text_raw_data.lower()) or \
           (len(raw_cells_from_html) > 0 and raw_cells_from_html[0].name == 'td' and len(raw_cells_from_html) == 1 and not any(char.isdigit() for char in first_cell_text_raw_data if char not in '1234) ') and len(first_cell_text_raw_data) > 30 and 'участокъ' not in first_cell_text_raw_data.lower() and 'инспекторъ' not in first_cell_text_raw_data.lower()):
             logging.info(f"Строка {current_row_log_num}: Пропуск строки, похожей на примечание/подзаголовок/сноску (после rowspan): {first_cell_text_raw_data[:100]}..."); continue
        
        est_count, work_count, boil_count = None, None, None
        gubernia_name_for_db, okrug_name_for_db = current_gubernia, current_okrug
        
        if year != 1901:
            est_count = clean_number(str(effective_cells_content[stat_start_idx_schema]).strip())
            work_count = clean_number(str(effective_cells_content[stat_start_idx_schema+1]).strip())
            boil_count = clean_number(str(effective_cells_content[stat_start_idx_schema+2]).strip())
        
        gubernia_candidate_raw_from_cell0 = str(effective_cells_content[0]).strip() if year == 1901 else None
        if gubernia_candidate_raw_from_cell0:
            gubernia_candidate_std = standardize_text(gubernia_candidate_raw_from_cell0)
            if gubernia_candidate_std and ('губернія' in gubernia_candidate_std or 'область' in gubernia_candidate_std):
                current_gubernia = gubernia_candidate_std.replace('губернія','').replace('область','').strip()
                gubernia_name_for_db = current_gubernia
                last_valid_gubernia_for_ditto_std = current_gubernia
                last_assigned_personnel_data_for_ditto = None
                current_senior_inspector_id_for_gubernia = None
            elif not gubernia_candidate_std and gubernia_name_for_db == "Неизвестно": gubernia_name_for_db = last_valid_gubernia_for_ditto_std
        elif gubernia_name_for_db == "Неизвестно" and last_valid_gubernia_for_ditto_std != "Неизвестно": gubernia_name_for_db = last_valid_gubernia_for_ditto_std

        uchastok_desc_raw = str(effective_cells_content[desc_cell_idx_schema]).strip()
        location_raw = str(effective_cells_content[loc_cell_idx_schema]).strip()
        personnel_html_content = effective_cells_content[pers_cell_idx_schema]
        
        location_city_current_std = standardize_text(location_raw)
        if location_city_current_std == '»' or not location_city_current_std:
            if last_location_city_std: location_city_current_std = last_location_city_std
            else: logging.warning(f"Стр {current_row_log_num}: Нет города для '»' и нет предыдущего. Уч: '{uchastok_desc_raw}'."); continue
        elif location_city_current_std: last_location_city_std = location_city_current_std
        
        role_val, uch_id_val, uch_desc_text_val = "Не определена", None, uchastok_desc_raw
        role_map_val = {'окружный фабричный инспекторъ':'Окружный инспектор','старшій фабричный инспекторъ':'Старший инспектор','кандидатъ на должность фабричнаго инспектора':'Кандидат','помощникъ старшаго инспектора':'Помощник','состоящій въ распоряженіи окружного фабричнаго инспектора':'В распоряжении','состоящій въ распоряженіи окружного инспектора':'В распоряжении','состоящій въ распоряженіи':'В распоряжении'}
        
        desc_lower_std_role_val = standardize_text(uchastok_desc_raw)
        role_found_val = False
        if desc_lower_std_role_val:
            for key_std_r_map, val_r_map in role_map_val.items():
                if standardize_text(key_std_r_map) == desc_lower_std_role_val: 
                    role_val=val_r_map; uch_desc_text_val=None; role_found_val=True; break
        
        if not role_found_val and uchastok_desc_raw.strip():
            role_val = "Инспектор участка"
            uch_match = re.match(r"(\d+)\s*(?:-?й|-?ый|-? участокъ|-? участокь|-? уч\.)?", uchastok_desc_raw, re.IGNORECASE)
            if uch_match: uch_id_val = uch_match.group(1)
            elif "вся губернія составляетъ одинъ участокъ" in uchastok_desc_raw.lower(): uch_id_val = "Вся губернія"
            elif "(должность не указана)" in uchastok_desc_raw.lower(): role_val = "Должность не указана"; uch_desc_text_val = None
        
        parsed_assignments_list = []
        pers_html_std_for_ditto = standardize_text(html.unescape(personnel_html_content).strip())
        if pers_html_std_for_ditto == '»' and last_assigned_personnel_data_for_ditto:
            parsed_assignments_list.append(last_assigned_personnel_data_for_ditto.copy())
            logging.debug(f"Стр {current_row_log_num}: Используем данные предыдущего инспектора для '»': {last_assigned_personnel_data_for_ditto}")
        else:
            parsed_assignments_list = parse_personnel_string_v4(personnel_html_content)
        
        if not parsed_assignments_list:
            temp_pers_text_check = html.unescape(personnel_html_content).strip()
            std_pers_text_check = standardize_text(temp_pers_text_check)
            if not (not temp_pers_text_check or temp_pers_text_check == '—' or \
                   std_pers_text_check == standardize_text('(нет данных)') or \
                   std_pers_text_check == standardize_text('(нетъ данныхъ)')):
                logging.warning(f"Строка {current_row_log_num}: Не удалось разобрать строку персонала (v4): '{html.unescape(personnel_html_content)[:100]}...' для '{uchastok_desc_raw}'")
            continue

        location_id_db = get_or_create_location(conn, location_city_current_std, gubernia_name_for_db, okrug_name_for_db, 'Город')
        if not location_id_db and location_city_current_std : 
             logging.error(f"Стр {current_row_log_num}: КРИТ ОШИБКА: Нет ID местоположения для Г='{location_city_current_std}', Губ='{gubernia_name_for_db}', Окр='{okrug_name_for_db}'. Пропуск."); continue

        for assign_data in parsed_assignments_list:
            inspector_id_db = None
            
            if not assign_data['is_vacancy'] and (assign_data['name'] or assign_data.get('special_role')):
                if assign_data.get('special_role') == "старший инспектор":
                    if current_senior_inspector_id_for_gubernia:
                        inspector_id_db = current_senior_inspector_id_for_gubernia
                        logging.info(f"Стр {current_row_log_num}: Участок '{uchastok_desc_raw}' -> Ст. инсп. (ID: {inspector_id_db} из кэша Губернии)")
                    else: 
                        cur.execute(""" SELECT InspectorID, RankID, ProfessionID, EducationID FROM Assignments 
                                        WHERE Year = %s AND OkrugName = %s AND GuberniaName = %s 
                                              AND PositionRole = 'Старший инспектор' AND InspectorID IS NOT NULL 
                                        ORDER BY AssignmentID DESC LIMIT 1 """, (year, okrug_name_for_db, gubernia_name_for_db))
                        sr_insp_res_db = cur.fetchone()
                        if sr_insp_res_db: 
                            inspector_id_db = sr_insp_res_db[0]
                            rank_id_from_db = sr_insp_res_db[1]; prof_id_from_db = sr_insp_res_db[2]; edu_id_from_db = sr_insp_res_db[3]
                            assign_data['rank_abbr'] = next((abbr for abbr, r_id_cache in rank_cache.items() if r_id_cache == rank_id_from_db), assign_data.get('rank_abbr')) if rank_id_from_db else assign_data.get('rank_abbr')
                            assign_data['prof_abbr'] = next((abbr for abbr, p_id_cache in profession_cache.items() if p_id_cache == prof_id_from_db), assign_data.get('prof_abbr')) if prof_id_from_db else assign_data.get('prof_abbr')
                            assign_data['edu_abbr'] = next((abbr for abbr, e_id_cache in education_cache.items() if e_id_cache == edu_id_from_db), assign_data.get('edu_abbr')) if edu_id_from_db else assign_data.get('edu_abbr')
                            logging.info(f"Стр {current_row_log_num}: Участок '{uchastok_desc_raw}' -> Ст. инсп. (ID: {inspector_id_db} из БД). Детали обновлены: R:{assign_data['rank_abbr']}, P:{assign_data['prof_abbr']}, E:{assign_data['edu_abbr']}")
                        else: 
                            logging.warning(f"Стр {current_row_log_num}: Не найден ID для 'Старшій инспекторъ' в {okrug_name_for_db}/{gubernia_name_for_db}, год {year}.")
                            assign_data['notes'] = ((assign_data.get('notes') or "") + "; Обслуж. ст.инсп.(ID не найден)").lstrip("; ")
                elif assign_data['name']: 
                    inspector_id_db = get_or_create_inspector_id(conn, assign_data['name'])
                
                if not inspector_id_db and not assign_data['is_vacancy'] and not assign_data.get('special_role') and assign_data['name']:
                    logging.warning(f"Стр {current_row_log_num}: Нет ID для инсп. '{assign_data['name']}'. Пропуск назначения."); continue
            
            if role_val == 'Старший инспектор' and inspector_id_db:
                current_senior_inspector_id_for_gubernia = inspector_id_db
            
            if assign_data['name'] and not assign_data['is_vacancy'] and not assign_data.get('special_role'):
                last_assigned_personnel_data_for_ditto = assign_data.copy()

            rank_id_db = get_or_create_rank_id(conn, assign_data['rank_abbr'])
            prof_id_db = get_or_create_profession_id(conn, assign_data['prof_abbr'])
            edu_id_db = get_or_create_education_id(conn, assign_data['edu_abbr'])
            
            if prof_id_db is None and assign_data['prof_abbr'] and standardize_text(assign_data['prof_abbr']) in KNOWN_EDUCATIONS:
                edu_id_db = get_or_create_education_id(conn, assign_data['prof_abbr'])

            try:
                cur.execute(""" INSERT INTO Assignments (InspectorID,Year,SourceFile,OkrugName,GuberniaName,PositionRole,UchastokIdentifier,UchastokDescription,InspectorLocationID,PersonnelRawString,RankID,ProfessionID,EducationID,StartDateInYearRaw,EndDateInYearRaw,IsActing,IsVacancy,EstablishmentsCount,WorkerCount,BoilerCount,AssignmentNotes)
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """,
                            (inspector_id_db,year,os.path.basename(filepath),okrug_name_for_db,gubernia_name_for_db,role_val,uch_id_val,uch_desc_text_val,location_id_db,html.unescape(personnel_html_content.strip()),rank_id_db,prof_id_db,edu_id_db,assign_data['start_date_raw'],assign_data['end_date_raw'],assign_data['is_acting'],assign_data['is_vacancy'],est_count,work_count,boil_count,assign_data['notes']))
            except psycopg2.Error as e: logging.error(f"Стр {current_row_log_num}: Ошибка вставки в БД для '{html.unescape(personnel_html_content)[:50]}...': {e}"); conn.rollback()
    conn.commit()
    cur.close()
    logging.info(f"Завершена обработка {filepath}")


if __name__ == "__main__":
    conn = get_db_connection()
    if conn:
        try:
            setup_database(conn) 
            inspector_cache.clear(); location_cache.clear(); rank_cache.clear(); profession_cache.clear(); education_cache.clear()

            for filename in HTML_FILES:
                filepath = os.path.join(HTML_FOLDER, filename)
                if os.path.exists(filepath): process_html_file(conn, filepath)
                else: logging.warning(f"Файл не найден: {filepath}")
        except Exception as e: logging.exception("Произошла непредвиденная ошибка во время обработки файлов.")
        finally: conn.close(); logging.info("Соединение с базой данных закрыто.")
    else: logging.error("Не удалось подключиться к базе данных. Выход.")
