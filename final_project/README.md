## Описание задачи: 

Компания по прокату велосипедов. Есть маршруты поездок от станции А до станции Б на внешнем сервере. 
Необходимо спроектировать систему доставки и хранения данных и их последующего анализа, 
чтобы оценить влияние сезонных факторов.

## Декомпозиция задачи.

0. Создать аккаунт в Google Cloud Services. подключить APIs: Composer/ BigQuery / Data Studio 
1. **Настроить планировщик задач** ( Airflow ), где будем запускать скрипты по расписанию.
    - Создать Composer Server
    - установить нужные Python зависимости на сервер 
    - подключить мониторинг. метрики для сервера
    - настроить Pools/settings
    - закинуть папку с DAGs, как будет готово.
    
2. **Написать DAG для Airflow** (python скрипт), который будет содержать последовательность шагов:
    - *PythonOperator*. скачивать файлы из [внешнего источника](https://cycling.data.tfl.gov.uk/) 
    - *BranchPythonOperator* если нет скаченных данных, то пропускаем все следующие шаги, а если есть, то идем далее
    - *GoogleCloudStorageToBigQueryOperator*. сохранять raw csv data в Google Cloud Storage (GCS)
    - *BigQueryOperator*. merge new data from GCS to BigQuery, 
        - используем таблицу. 1) partition by start_date 2) clustered by start_station_id, end_station_id, rental_id
        - предварительно очищая данные и исправляя типизацию.
        - используем merge insert, если часть данных уже существует.
    
3. **Создать репорт в Data Studio** (BI), для последующего анализа.
    - создать View с нужными агрегатами, чтобы легче было подключать источники в BI. 
    - если будут много использовать view, то можно будет  создать отдельную таблицу, и пересчитывать раз в день, для экономии трафика 
    - подключить источники
    - построить графики для отчета

## Архитектура:
![Архитектура](https://github.com/achicha/otus-de-2020-02/blob/master/final_project/screenshots/architecture.png)

## Необходимые компоненты системы:

- Composer (managed Airflow)
- Google Cloud Storage
- BigQuery 
- Data Studio

## Итог

Существует сезонная зависимость в прокате. Спрос преобладает в теплые месяцы и в будние дни.
Возможно, люди используют велосипед, чтобы доезжать от метро до работы. 

#### Airflow. 

- [Run DAG](https://github.com/achicha/otus-de-2020-02/blob/master/final_project/dags/run_dag.py), 
- [Parse external data](https://github.com/achicha/otus-de-2020-02/blob/master/final_project/dags/fetch_data.py)

![metrics ](https://github.com/achicha/otus-de-2020-02/blob/master/final_project/screenshots/composer_metrics.png)
![webUI](https://github.com/achicha/otus-de-2020-02/blob/master/final_project/screenshots/airflow_webui.png)

#### Google Cloud Storage raw files

![gcs raw files](https://github.com/achicha/otus-de-2020-02/blob/master/final_project/screenshots/gcs_raw_files.png)

#### BigQuery

- [merge insert](https://github.com/achicha/otus-de-2020-02/blob/master/final_project/dags/merge_tables_sql.py)
- [aggregate views](https://github.com/achicha/otus-de-2020-02/tree/master/final_project/sql)

![gcs raw files](https://github.com/achicha/otus-de-2020-02/blob/master/final_project/screenshots/bigquery_schema.png)

#### Data Studio
![gcs raw files](https://github.com/achicha/otus-de-2020-02/blob/master/final_project/screenshots/datastudio.png)