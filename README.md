# Apache Airflow with Docker

Bu repo, Apache Airflow'u Docker Compose kullanarak kurmak için gereken dosyaları içerir.

## Gereksinimler

- Docker
- Docker Compose

```
docker-compose run airflow-scheduler airflow db init
docker-compose run airflow-webserver airflow db init
docker-compose run airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password your_password_here

docker-compose up
```

```
http://localhost:8080
```

Username: airflow
Password: airflow
