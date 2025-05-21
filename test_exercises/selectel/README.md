
# Python

# Задание: Разработать Airflow DAG для сбора данных из публичного API (CoinGecko)

## Контекст

# Представь, что ты реализуешь ежедневный пайплайн, который собирает информацию о криптовалютах из открытого API и загружает в DWH или S3.

# В этом задании тебе нужно:

# Написать Airflow DAG, который:
#    - Ходит в API CoinGecko за данными о криптовалютах
#    - Сохраняет данные в БД или S3 (raw или трансформированные — на твой выбор)

## API endpoint (текущая информация о топ-10 монетах):
# https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1&sparkline=false


import request
import json
from airflow import Dag
from airflow python operator

#
URL = 'https://api.coingecko.com/api/v3/coins/markets'
S3_Bucket_Name = ''

default_args = {
  
}

def ingest_data_api(**contex):

  params = {
    'vs_currency': = 'usd',
    'order': = 'market_cap_desc',
    'per_page': = 10,    
  }
  
   try:        
     response = request.get(URL, params=params)
     data = response.json() 
     return data
     
   except Exception as e:
     logging.error(f'Ошибка при получении данных')
     raise    
  
def save_to_s3(**contex):  
    data.to_s3()

dag = Dag(
  'ingest_data_api',
  schedule_interval = '@daily'
)

  ingest_data_api = PythonOperator(
  task_id = 'ingest_data_api',
  python_callable=ingest_data_api,
  dag=dag
  )

  save_to_s3 = PythonOperator(
  task_id = 'save_to_s3',
  python_callable=save_to_s3,
  dag=dag
  )

ingest_data_api >> save_to_s3

# SQL

# CREATE TABLE "accounts" (
# 	"id" SERIAL NOT NULL,
# 	"currency" TEXT NOT NULL,
# 	"legal_entity" TEXT NOT NULL,
# 	"client_id" INTEGER NOT NULL,
# 	PRIMARY KEY("id")
# );

# CREATE TABLE "clients" (
# 	"id" SERIAL NOT NULL,
# 	"name" TEXT NOT NULL,
# 	"age" INTEGER NOT NULL,
# 	"country" TEXT NOT NULL,
# 	PRIMARY KEY("id")
# );

# CREATE TABLE "payments" (
# 	"id" SERIAL NOT NULL,
# 	"created_at" TIMESTAMP NOT NULL,
# 	"account_id" INTEGER NOT NULL,
# 	"amount" INTEGER NOT NULL,
# 	"product_id" INTEGER NOT NULL,
# 	PRIMARY KEY("id")
# );

# CREATE TABLE "products" (
# 	"id" SERIAL NOT NULL,
# 	"name" TEXT NOT NULL,
# 	PRIMARY KEY("id")
# );

# ALTER TABLE "accounts"
# ADD FOREIGN KEY("client_id") REFERENCES "clients"("id");

# ALTER TABLE "payments"
# ADD FOREIGN KEY("account_id") REFERENCES "accounts"("id");

# ALTER TABLE "payments"
# ADD FOREIGN KEY("product_id") REFERENCES "products"("id")
# 

# Спроектировать core-слой на основе вышеуказанных таблиц. Подразумевается использование классического подхода с разделением на dim и fct-таблицы (звезда или снежинка).
# Также необходимо построить витрину — таблицу, с помощью которой можно будет отвечать на основные аналитические вопросы бизнеса (по странам, возрастам, продуктам, клиентам и т.д.).

# Особенности: наименования продуктов в таблице products может меняться со временем


1. DimProduct
Select
MD5(id) as id,
id as product_id,
name
From public.product

2. DimAccount
Select
MD5(b1.Id) as Id,
b1.id as account_id,
b1.currency,
b1.legal_entity,
b1.client_id,
b2.name,
b2.age,
b2.country
From public.account as b1
Inner join public.clients as b2 on b2.id = b1.client_id

3. FactPayments

Select
b1.id,
b1.created_at,
b2.Id as Account_Id,
b3.id as Product_Id,
b1.amount
From public.payments as b1
Left join DimAccount as b2 on b2.account_id = b1.account_id
Left join DimProduct as b3 on b3.product_id = b1.product_id

