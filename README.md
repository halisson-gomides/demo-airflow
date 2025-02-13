# demo-airflow
Demonstração de uso do orquestrador Apache Airflow

- Comando para baixar o arquivo `docker-compose.yaml`:
`curl -LfO https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml`

- Dentro do arquivo `docker-compose.yaml`, setar a variável `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`
- Dentro do arquivo `docker-compose.yaml`, criar a variável `AIRFLOW__CORE__TEST_CONNECTION: 'Enabled'`
- Modifique o `docker-compose.yaml`, comentando a linha `image` e descomentando a linha `build: .`
- Adicione a seção `networks` em cada serviço do airflow com as entradas para uma rede airflow e outra 
para o banco de dados externo:
`networks:
      - airflow_network
      - postgres_network
`
- No final do arquivo, adicionar a configuração de cada rede:
`networks:
  airflow_network:
    driver: bridge
  postgres_network:
    name: app_network
    external: true
`

- Rodar o comando: `docker compose up --build -d` 