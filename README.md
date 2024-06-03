English version

# Instructions for using the data generation script

This script is designed to generate workflows using predefined parameters. You can customize the settings to suit your needs.

## Running the script

To run the script, use the following command in the terminal:

```bash
go run main.go Generator [parameters]


Options
--instance_total: Total number of worker processes that will be generated (default: 100000).
--workers: Number of parallel requests (default: half the number of CPUs).
--wfID: Workflow ID to generate (default: 2251799813831553).
--batch_size: Batch size to generate (default 1000).
--delta_record: Timeout between generating records (default: 1 second).
--postgresURL: String representing the connection to the PostgreSQL database. It should include the following components:
                username:password: Username and password for accessing the PostgreSQL database.
                host:port: Address and port of the PostgreSQL database.
                dbname: Name of the database.
                sslmode: SSL mode for the connection (optional, default is disable).
                timezone: Timezone of the database (optional, default is UTC).
                search_path: Search path of the database (optional, default is public).

                Example: postgres://username:password@localhost:5432/mydatabase?sslmode=disable&timezone=UTC&search_path=public


Examples
Running the script with default parameters:
go run main.go Generator --batch_size 500

Running a script with custom parameters:
go run main.go Generator --instance_total 50000 --workers 4 --wfID 1234567890123456 --batch_size 1000 --delta_record 2s








Russian version

# Инструкция по использованию скрипта генерации данных

Этот скрипт предназначен для генерации рабочих процессов с использованием предопределённых параметров. Вы можете настроить параметры в соответствии с вашими потребностями.

## Запуск скрипта

Чтобы запустить скрипт, используйте следующую команду в терминале:

```bash
go run main.go Generator [параметры]


Параметры
--instance_total: Общее количество рабочих процессов, которые будут сгенерированы (по умолчанию: 100000).
--workers: Количество параллельных запросов (по умолчанию: половина от числа CPU).
--wfID: ID рабочего процесса для генерации (по умолчанию: 2251799813831553).
--batch_size: Размер пакета для генерации (по умолчанию 1000).
--delta_record: Время ожидания между генерацией записей (по умолчанию: 1 секунда).
--postgresURL:  Строка подключения к базе данных PostgreSQL. Она должна содержать следующие элементы:
                username:password: Имя пользователя и пароль для доступа к базе данных PostgreSQL.
                host:port: Адрес и порт базы данных PostgreSQL.
                dbname: Имя базы данных.
                sslmode: Режим SSL для подключения (необязательный, по умолчанию disable).
                timezone: Часовой пояс базы данных (необязательный, по умолчанию UTC).
                search_path: Путь поиска базы данных (необязательный, по умолчанию public).

                Пример: postgres://username:password@localhost:5432/mydatabase?sslmode=disable&timezone=UTC&search_path=public


Примеры
Запуск скрипта с параметрами по умолчанию:
go run main.go Generator --batch_size 500

Запуск скрипта с пользовательскими параметрами:
go run main.go Generator --instance_total 50000 --workers 4 --wfID 1234567890123456 --batch_size 1000 --delta_record 2s

