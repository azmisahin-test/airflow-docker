# Apache Airflow with Docker

Bu repo, Apache Airflow'u Docker Compose kullanarak kurmak için gereken dosyaları içerir.

## Gereksinimler

- Docker
- Docker Compose

```
docker-compose run airflow-scheduler airflow db init
docker-compose run airflow-webserver airflow db init
docker-compose up
```
