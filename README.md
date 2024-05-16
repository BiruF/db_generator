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


Примеры
Запуск скрипта с параметрами по умолчанию:
go run main.go Generator --batch_size 500

Запуск скрипта с пользовательскими параметрами:
go run main.go Generator --instance_total 50000 --workers 4 --wfID 1234567890123456 --batch_size 1000 --delta_record 2s

