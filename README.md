# demo-airflow
Demonstração de uso do orquestrador Apache Airflow

- Comando para baixar o arquivo `docker-compose.yaml`:
`curl -LfO https://airflow.apache.org/docs/apache-airflow/2.10.4/docker-compose.yaml`

- Dentro do arquivo `docker-compose.yaml`, setar a variável `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`

- Rodar o comando: `docker compose up -d` 