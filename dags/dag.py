from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
import requests as req
import pandas as pd
import xml.etree.ElementTree as etree
from clickhouse_driver import Client
import os
# Константы

URL = 'http://www.cbr.ru/scripts/XML_daily.asp'
DAG_START_DATE = datetime(2025, 6, 19)
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': DAG_START_DATE,
    'end_date' : datetime(2025,6,28)
}

dag = DAG(
    'cbr',
    default_args=default_args,
    schedule='@daily',
    catchup=True,
    max_active_runs=1,
    description='DAG для загрузки курсов валют с сайта ЦБ РФ в ClickHouse'

)
client = Client(host='your_host',
    user= 'your_user',
    password= 'your_pass',
    database= 'sandbox')

table_name = "toMO_data"

def create_table(**kwargs):
    try:
        client.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                num_code UInt32,
                char_code FixedString(3),
                nominal UInt32,
                name String,
                value Float64,
                date Date
            ) ENGINE = MergeTree()
            ORDER BY (date, char_code)
            PARTITION BY toYYYYMM(date)
        ''')
        print(f"Таблица {table_name} успешно создана/проверена")
    except Exception as e:
        print(f"Ошибка при создании таблицы: {e}")
        raise

def extract_data(**kwargs):
    ds = kwargs['ds']
    # Преобразуем в datetime объект
    exec_date = datetime.strptime(ds, '%Y-%m-%d')

    # Форматируем для URL (DD/MM/YYYY)
    cbr_date = exec_date.strftime('%d/%m/%Y')
    try:
        response = req.get(f"{URL}?date_req={cbr_date}", timeout=10)
        response.raise_for_status()

        with open("/tmp/currency.xml", "w", encoding="utf-8") as tmp_file:
            tmp_file.write(response.text)
    except Exception as e:
        print(f"Ошибка при получении данных за {cbr_date}: {e}")
        raise


 

def transform_data(**kwargs):
    try:
        xml_path = "/tmp/currency.xml"
        if not os.path.exists(xml_path):
            raise FileNotFoundError(f"Файл {xml_path} не найден")
        if os.path.getsize(xml_path) == 0:
            raise ValueError("XML файл пустой")

        # Используем lxml с восстановлением после ошибок
        parser = etree.XMLParser( encoding='utf-8')
        tree = etree.parse(xml_path, parser=parser)
        root = tree.getroot()
        if 'Date' not in root.attrib:
            raise ValueError("Отсутствует атрибут Date в XML")
        xml_date_str = root.attrib['Date']
        parsed_date = datetime.strptime(xml_date_str, '%d.%m.%Y').date()
        rows = []
        for valute in root.findall('Valute'):
            try:
                rows.append({
                    'num_code': int(valute.find('NumCode').text),
                    'char_code': valute.find('CharCode').text.strip(),
                    'nominal': int(valute.find('Nominal').text),
                    'name': valute.find('Name').text.strip(),
                    'value': float(valute.find('Value').text.replace(',', '.')),
                    'date': parsed_date.strftime('%Y-%m-%d')
                })
            except Exception as ve:
                print(f"Ошибка обработки валюты: {str(ve)}")
                continue

        df = pd.DataFrame(rows)
        df.to_csv("/tmp/currency.csv", index=False)
        print(f"Успешно обработано {len(df)} записей")
    except Exception as e:
        print(f"Критическая ошибка: {str(e)}")
        try:
            with open("/tmp/currency.xml", "r", encoding='utf-8') as f:
                print("Содержимое XML:", f.read(500))
        except:
            print("Не удалось прочитать XML файл")
        raise

def check_and_delete(**kwargs):
    ds = kwargs['ds']
    # Преобразуем в datetime объект
    exec_date = datetime.strptime(ds, '%Y-%m-%d')
   # Приводим к нужному формату: 'YYYY-MM-DD'
    cbr_date = exec_date.strftime('%Y-%m-%d')
    try:
        # Используем toDate() в запросе, чтобы избежать ошибок типов
        count = client.execute(f"""
            SELECT count() FROM {table_name}
            WHERE date = toDate('{cbr_date}')
        """)[0][0]
        print(f"Найдено {count} записей за {cbr_date}")
        if count > 0:
            client.execute(f"""
                ALTER TABLE {table_name}
                DELETE WHERE date = toDate('{cbr_date}')
            """)
            print(f"Удалено {count} старых записей")
    except Exception as e:
        print(f"Ошибка очистки данных: {e}")
        raise
def upload_to_clickhouse(**kwargs):
    csv_file = "/tmp/currency.csv"
    try:
        df = pd.read_csv(csv_file)
        # Преобразуем строковую дату в datetime.date
        df['date'] = pd.to_datetime(df['date']).dt.date
        records = df.to_dict('records')
        client.execute(f"INSERT INTO {table_name} VALUES", records)
        print(f"Успешно загружено {len(records)} записей")
    except Exception as e:
        print(f"Ошибка загрузки в ClickHouse: {e}")
        raise

 

# Определение задач

 

create_table_task = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=dag,
)

 

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

 

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

 

clean_task = PythonOperator(
    task_id='check',
    python_callable=check_and_delete,
    dag=dag,
)

 

load_task = PythonOperator(
    task_id='upload_to_clickhouse',
    python_callable=upload_to_clickhouse,
    dag=dag,
)

# Поток выполнения
create_table_task >> extract_task >> transform_task >> clean_task >> load_task
