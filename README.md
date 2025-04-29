# Student Airflow Template

This is a ready-to-run Apache Airflow + Docker environment designed for classroom use. Students can use this to run Airflow DAGs that connect to MongoDB and process stock data.

## ✅ Getting Started

1. Clone the repo or click "Use this template"
2. Run:

```bash
docker compose up airflow-webserver airflow-scheduler -d
docker compose exec airflow-webserver airflow db init
docker compose exec airflow-webserver airflow users create \
  --username admin --password airflow \
  --firstname Air --lastname Flow \
  --role Admin --email admin@example.com
```

3. Open [http://localhost:8080](http://localhost:8080)

Login with:
- **Username:** `admin`
- **Password:** `airflow`

4. Edit the `.env` file to point to your MongoDB Atlas connection

## 🧪 Included

- `example_mongo_etl.py`: reads from MongoDB, logs stock data
- `docker-compose.yaml`: runs Airflow + Postgres
- `requirements.txt`: installs `pymongo`, `requests` into Airflow container
