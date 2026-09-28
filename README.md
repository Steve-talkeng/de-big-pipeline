# DE Big Pipeline

Локальный Data Engineering pipeline, построенный с использованием **Apache Airflow, PostgreSQL, MinIO и ClickHouse**.

Проект демонстрирует полный batch-процесс обработки данных: данные извлекаются из PostgreSQL, передаются в объектное хранилище MinIO, загружаются в ClickHouse и затем проверяются через аналитическую витрину.

## Архитектура

```text
                    ┌─────────────────┐
                    │   PostgreSQL    │
                    │                 │
                    │     mydb        │
                    │     orders      │
                    └────────┬────────┘
                             │
                       export_csv
                             │
                             ▼
                    ┌─────────────────┐
                    │      MinIO      │
                    │                 │
                    │ data/orders.csv │
                    └────────┬────────┘
                             │
                    load_to_clickhouse
                             │
                             ▼
                    ┌─────────────────┐
                    │    ClickHouse   │
                    │                 │
                    │ default.orders  │
                    └────────┬────────┘
                             │
                         агрегация
                             │
                             ▼
                 ┌──────────────────────┐
                 │       datamart       │
                 │   sales_by_product   │
                 └──────────────────────┘

                    Оркестрация
                 ┌───────────────────┐
                 │   Apache Airflow  │
                 │                   │
                 │ Scheduler         │
                 │ API Server        │
                 │ DAG Processor     │
                 └───────────────────┘
```

## Используемые технологии

* **Apache Airflow 3.2.2** — оркестрация pipeline
* **PostgreSQL 14** — исходная база данных и база метаданных Airflow
* **MinIO** — S3-совместимое объектное хранилище
* **ClickHouse 23.10** — аналитическая база данных
* **Docker / Docker Compose** — контейнеризация
* **Python** — создание Airflow DAG
* **Git / GitHub** — система контроля версий

## Структура проекта

```text
de-big-pipeline/
│
├── dags/
│   └── datamart_dag.py
│
├── docker-compose.yml
├── Dockerfile.airflow
├── .env.example
├── .gitignore
└── README.md
```

## Pipeline

Основной Airflow DAG называется:

```text
datamart_update
```

DAG запускается ежедневно и состоит из следующих задач:

```text
export_csv
    ↓
copy_to_minio
    ↓
truncate_clickhouse
    ↓
load_to_clickhouse
    ↓
check_data
    ↓
check_view
    ↓
notify_success
```

### 1. `export_csv`

Извлекает данные из таблицы `orders` в PostgreSQL и сохраняет их в CSV-файл:

```text
/tmp/orders.csv
```

Для экспорта используется команда PostgreSQL `\COPY`.

### 2. `copy_to_minio`

Переносит созданный CSV-файл в MinIO:

```text
myminio/data/orders.csv
```

### 3. `truncate_clickhouse`

Очищает существующую таблицу ClickHouse:

```text
default.orders
```

Это позволяет загрузить актуальный набор данных заново.

### 4. `load_to_clickhouse`

Читает CSV-файл из MinIO и загружает данные в:

```text
default.orders
```

### 5. `check_data`

Проверяет количество записей, загруженных в ClickHouse.

### 6. `check_view`

Проверяет аналитическую витрину:

```text
datamart.sales_by_product
```

Витрина группирует заказы по продуктам и рассчитывает:

* количество заказов;
* общую сумму продаж;
* среднюю сумму продажи;
* минимальную сумму продажи;
* максимальную сумму продажи.

### 7. `notify_success`

Завершает pipeline после успешного выполнения всех предыдущих задач.

## PostgreSQL

В проекте используется **один PostgreSQL-контейнер**, внутри которого находятся две базы данных:

```text
mydb
└── orders

airflow
└── метаданные Airflow
```

Такой подход позволяет использовать один PostgreSQL-сервис, сохраняя логическое разделение между данными приложения и метаданными Airflow.

## Airflow

Для выполнения задач используется:

```text
LocalExecutor
```

Airflow API Server доступен по адресу:

```text
http://localhost:8080
```

Для выполнения Docker-команд из Airflow используется Docker socket:

```text
/var/run/docker.sock
```

Это позволяет задачам DAG выполнять команды:

```bash
docker exec
docker cp
```

в отношении контейнеров pipeline.

> Docker socket предоставляет контейнеру Airflow значительный доступ к Docker daemon хоста. Такой подход подходит для локальной учебной среды, но требует дополнительного внимания к безопасности в production.

## Переменные окружения

Секретные настройки хранятся в локальном файле:

```text
.env
```

Сам `.env` **не хранится в Git**.

В репозитории находится только шаблон:

```text
.env.example
```

Создать локальный `.env` можно командой:

```bash
cp .env.example .env
```

После этого необходимо указать собственные значения переменных.

## Запуск проекта

Клонировать репозиторий:

```bash
git clone <repository-url>
cd de-big-pipeline
```

Создать `.env`:

```bash
cp .env.example .env
```

Запустить Docker Compose:

```bash
docker compose up -d
```

Проверить запущенные контейнеры:

```bash
docker ps
```

После запуска Airflow доступен по адресу:

```text
http://localhost:8080
```

## Данные для входа в Airflow

В локальной конфигурации создаётся пользователь администратора:

```text
Логин: admin
Пароль: admin
```

Эти данные предназначены только для локальной учебной среды.

## Запуск DAG

DAG можно запустить через интерфейс Airflow либо из терминала:

```bash
docker exec de-big-airflow-webserver airflow dags trigger datamart_update
```

Посмотреть запуски DAG:

```bash
docker exec de-big-airflow-webserver airflow dags list-runs datamart_update
```

Проверить состояние задач конкретного запуска:

```bash
docker exec de-big-airflow-webserver \
  airflow tasks states-for-dag-run datamart_update <RUN_ID>
```

## Тестирование отдельных задач

Каждую задачу DAG можно проверить отдельно.

Например:

```bash
docker exec de-big-airflow-webserver \
  airflow tasks test datamart_update export_csv
```

Успешное выполнение можно определить по строкам:

```text
Command exited with return code 0
```

и:

```text
new_state=success
```

## Поток данных

Полный процесс обработки данных выглядит следующим образом:

```text
PostgreSQL
    │
    │ экспорт в CSV
    ▼
orders.csv
    │
    │ загрузка
    ▼
MinIO
    │
    │ чтение CSV
    ▼
ClickHouse
    │
    │ агрегация
    ▼
sales_by_product
```

## Результат

Pipeline был полностью протестирован.

Все задачи DAG `datamart_update` успешно выполняются последовательно:

```text
export_csv          ✓
copy_to_minio       ✓
truncate_clickhouse ✓
load_to_clickhouse  ✓
check_data          ✓
check_view          ✓
notify_success      ✓
```

В результате данные проходят полный путь:

**PostgreSQL → MinIO → ClickHouse → Datamart**

## Цель проекта

Проект создан в рамках обучения Data Engineering и предназначен для практики следующих технологий и навыков:

* Docker и Docker Compose;
* Apache Airflow;
* PostgreSQL;
* объектное хранилище MinIO;
* ClickHouse;
* построение batch ETL pipeline;
* оркестрация задач;
* проверка качества и наличия данных;
* работа с Linux CLI;
* Git и GitHub.
