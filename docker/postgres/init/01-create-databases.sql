SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE superset'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'superset')\gexec

SELECT 'CREATE DATABASE model_registry'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'model_registry')\gexec
