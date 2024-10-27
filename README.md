# Apache Airflow with Docker

Bu repo, Apache Airflow'u Docker Compose kullanarak kurmak için gereken dosyaları içerir.

## Gereksinimler

- Docker
- Docker Compose

##
Veritabanını Başlatın: Veritabanını başlattığınızdan emin olun. Aşağıdaki komutları tekrar çalıştırın:


```
docker-compose run airflow-postgres
```

```
docker-compose run airflow-scheduler airflow db init
```

```
docker-compose run airflow-webserver airflow db init
```

Kullanıcı Oluşturun: Kullanıcı oluşturma adımını gerçekleştirin. Örnek olarak aşağıdaki komutu çalıştırın ve parolanızı girin:

```
docker-compose run airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password your_password_here

```

Parola kısmını istediğiniz gibi belirleyin.

Hizmetleri Başlatın: Tüm hizmetleri başlatmak için:

```
docker-compose up
```

Web Arayüzüne Erişim: Tarayıcınızdan http://localhost:8080 adresine gidin ve oluşturduğunuz kullanıcı adı (admin) ve parolayı kullanarak giriş yapın.
