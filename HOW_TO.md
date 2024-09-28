# Инструкция для кейса хакатона

## Технические требования для размещения данных и решения задачи

1. Для размещения датасетов и итоговых результатов необходимо использовать Docker-контейнер на базе образа:  
   `clickhouse/clickhouse-server:24.8.4.13-alpine@sha256:88a45f9e328549b2579256c46ee38e5c0e25ae58303d9eb6d9c7ed8d6d2bbf3c`.

2. Исходные данные будут размещены в следующих таблицах Clickhouse:

   ```sql
   CREATE OR REPLACE TABLE table_dataset1 (
       uid UUID,
       full_name String,
       email String,
       address String,
       sex String,
       birthdate String,
       phone String
   ) ENGINE = MergeTree()
   PARTITION BY murmurHash3_32(uid) % 8
   ORDER BY uid;
   ```

   ```sql
   CREATE OR REPLACE TABLE table_dataset2 (
       uid UUID,
       first_name String,
       middle_name String,
       last_name String,
       birthdate String,
       phone String,
       address String
   ) ENGINE = MergeTree()
   PARTITION BY murmurHash3_32(uid) % 8
   ORDER BY uid;
   ```

   ```sql
   CREATE OR REPLACE TABLE table_dataset3 (
       uid UUID,
       name String,
       email String,
       birthdate String,
       sex String
   ) ENGINE = MergeTree()
   PARTITION BY murmurHash3_32(uid) % 8
   ORDER BY uid;
   ```

3. Итоговые результаты должны быть представлены в таблице `table_results` со следующей структурой:

   ```sql
   CREATE OR REPLACE TABLE table_results (
       id_is1 Array(UUID),
       id_is2 Array(UUID),
       id_is3 Array(UUID)
   ) ENGINE = MergeTree()
   ORDER BY id_is1;
   ```
   - Каждая колонка (`id_is1`, `id_is2`, `id_is3`) содержит массивы идентификаторов записей из соответствующего датасета.
   - Если в одном датасете обнаружены дублирующиеся записи, их идентификаторы объединяются в один массив.
   - Записи, которые соответствуют одному клиенту в нескольких датасетах, должны быть сгруппированы в одной строке, где массивы идентификаторов заполняются по каждому датасету.
   - Если соответствие найдено только в одном из датасетов, то массив идентификаторов заполняется только в одной колонке.
   - В результате, сумма длин всех массивов в каждой колонке должна соответствовать количеству строк в исходных датасетах.

Примеры результатов:
```text
[[<id1>], [<id2>], [<id3>]] # id1, id2, id3 - идентификаторы одного пользователя в трёх разных системах без дублей в каждой
[[<id1>, <id2>, <id3>], [], []] # id1, id2, id3 - идентификаторы дублей одного пользователя в первой системе
```
4. Решение должно быть предоставлено в виде Docker-образа и файлов сборки (Dockerfile и все необходимые зависимости). Docker-образ может быть передан в виде tar-файла или размещен в публичном Docker Registry. Образ должен основываться на публичных базовых образах.

5. Сборка образа будет выполняться командой:

   ```bash
   docker build -t solution .
   ```

6. Проверка решения будет производиться с использованием `docker-compose`. Пример конфигурации предоставлен в архиве `docker-compose.zip`. В данном примере содержатся скрипты создания таблиц и загрузки данных в Clickhouse. Ваше приложение должно использовать следующие параметры для подключения к базе данных:

   - **Хост:** `clickhouse`
   - **Пользователь:** `default`
   - **Пароль:** отсутствует

7. Ограничения на проверку решения:

   - **Хост-система:** Ubuntu 24.04 с установленным Docker (по [инструкции](https://docs.docker.com/engine/install/ubuntu/)).
   - **Архитектура:** x86_64.
   - **ОЗУ:** 16 ГБ.
   - **CPU:** 4 физических ядра.
   - **Диск:** 50 ГБ.
   - **Максимальное время выполнения:** 20 минут.