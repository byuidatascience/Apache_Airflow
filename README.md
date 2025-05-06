# Student Airflow Template

This is a ready-to-run Apache Airflow + Docker environment designed for classroom use. Students can use this to run Airflow DAGs that connect to MongoDB and process stock data.

## Before You Start

1. Run the airflow-core-fernet-key.py script to generate a fernet key. This key is used to encrypt sensitive data in Airflow, such as passwords and connection strings. You can run this script in your terminal or command prompt.

you might need to install the `cryptography` library if you don't have it already. You can do this by running:
```bash
pip install cryptography
```

Then, run the script:
```bash
python airflow-core-fernet-key.py
```
2. Copy the generated fernet key and paste it into the `.env` file in the `FERNET_KEY` variable.

3. Make sure you have Docker and Docker Compose installed on your machine. You can download them from the official Docker website.


## ✅ Getting Started

1. Clone the repo or click "Use this template"
2. Open the cloned repo in VS Code
2. In that VS Code Terminal Run:

```bash
docker compose up --build -d
```

<!-- ```bash
docker compose --env-file .env up -d
docker compose up airflow-webserver airflow-scheduler -d
docker compose exec airflow-webserver airflow db init
docker compose exec airflow-webserver airflow db migrate
docker compose exec airflow-webserver airflow users create \
  --username admin --password airflow \
  --firstname Air --lastname Flow \
  --role Admin --email admin@example.com
``` -->

3. Open [http://localhost:8080](http://localhost:8080)

Login with:
- **Username:** `admin`
- **Password:** `airflow`

4. Edit the `.env` file to point to your MongoDB Atlas connection

## 🧪 Included

- `example_mongo_etl.py`: reads from MongoDB, logs stock data
- `docker-compose.yaml`: runs Airflow + Postgres
- `requirements.txt`: installs `pymongo`, `requests` into Airflow container

## shut down

```bash
docker compose down
```

<!-- https://airflow.apache.org/docs/apache-airflow/stable/tutorial/index.html -->
<!-- https://www.youtube.com/watch?v=ouERCRRvkFQ -->
<!-- https://www.youtube.com/watch?v=RXWYPZ3T9ys -->