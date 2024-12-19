# demo-airflow
Demonstração de uso do orquestrador Apache Airflow

- Comando para baixar o arquivo `docker-compose.yaml`:
`curl -LfO https://airflow.apache.org/docs/apache-airflow/2.10.4/docker-compose.yaml`

- Dentro do arquivo `docker-compose.yaml`, setar a variável `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`
- Dentro do arquivo `docker-compose.yaml`, criar a variável `AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'`
- Modifique o `docker-compose.yaml`, comentando a linha `image` e descomentando a linha `build: .`
> `x-airflow-common:
  &airflow-common
  # Comment out the image line
  # image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.4}
  # Uncomment the build line
  build: .`

- Rodar o comando: `docker compose up --build -d` 