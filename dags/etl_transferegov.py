from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json
# import pandas as pd
from datetime import datetime

DB_CONN_ID = 'CONN_PG_TRANSFGOV'
API_CONN_ID = 'CONN_API_TRANSFGOV' 

# Argumentos para o Endpoint da API
ANO = '2024'

default_args={
    'owner':'airflow',
    'depends_on_past': False,
    'start_date':days_ago(1)
}

## DAG
with DAG(dag_id='etl_programas_especiais',
         default_args=default_args,
         description='ETL de Programas Especiais da API Transf Gov',
         schedule_interval='@daily',
         catchup=False) as dags:
    
    @task
    def extract_api_data():
        """Extrai dados da API de Transferencias Especiais usando conexão Airflow
        """
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        # API Endpoint
        endpoint = f"/programa_especial?ano_programa=eq. {ANO}&limit=10"

        # Requisição via HTTP Hook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Falha na requisição a API: {response.status_code}")
        

    @task
    def transform_api_data(api_data):
        """Transformacoes nos dados obtidos via API

        Args:
            api_data (json): Dados obtidos via API
        """
        transformed_data = [
            {
                "id_programa": d.get("id_programa"),
                "ano_programa": d.get("ano_programa"),
                "modalidade_programa": d.get("modalidade_programa"),
                "codigo_programa": d.get("codigo_programa"),
                "id_orgao_superior_programa": d.get("id_orgao_superior_programa"),
                "sigla_orgao_superior_programa": d.get("sigla_orgao_superior_programa"),
                "nome_orgao_superior_programa": d.get("nome_orgao_superior_programa"),
                "id_orgao_programa": d.get("id_orgao_programa"),
                "sigla_orgao_programa": d.get("sigla_orgao_programa"),
                "nome_orgao_programa": d.get("nome_orgao_programa"),
                "id_unidade_gestora_programa": d.get("id_unidade_gestora_programa"),
                "documentos_origem_programa": d.get("documentos_origem_programa"),
                "id_unidade_orcamentaria_responsavel_programa": d.get("id_unidade_orcamentaria_responsavel_programa"),
                "data_inicio_ciencia_programa": datetime.strptime(d.get("data_inicio_ciencia_programa"), '%Y-%m-%d').date(),
                "data_fim_ciencia_programa": datetime.strptime(d.get("data_fim_ciencia_programa"), '%Y-%m-%d').date(),
                "valor_necessidade_financeira_programa": float(d.get("valor_necessidade_financeira_programa")) / 100,
                "valor_total_disponibilizado_programa": float(d.get("valor_total_disponibilizado_programa")) / 100,
                "valor_impedido_programa": float(d.get("valor_impedido_programa")) / 100,
                "valor_a_disponibilizar_programa": float(d.get("valor_a_disponibilizar_programa")) / 100,
                "valor_documentos_habeis_gerados_programa": float(d.get("valor_documentos_habeis_gerados_programa")) / 100,
                "valor_obs_geradas_programa": float(d.get("valor_obs_geradas_programa")) / 100,
                "valor_disponibilidade_atual_programa": float(d.get("valor_disponibilidade_atual_programa")) / 100,
            } for d in api_data
        ]
        return transformed_data
    
    
    @task
    def load_to_database(tranformed_data):
        """Carrega os dados da API para o banco Postgres

        Args:
            tranformed_data (list): Lista de dicionarios com os dados retornados via api
        """
        pg_hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Criação da tabela se não existir
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS programas_especiais (
            id_programa INTEGER PRIMARY KEY,
            ano_programa INTEGER,
            modalidade_programa VARCHAR(50),
            codigo_programa VARCHAR(20),
            id_orgao_superior_programa INTEGER,
            sigla_orgao_superior_programa VARCHAR(10),
            nome_orgao_superior_programa VARCHAR(100),
            id_orgao_programa INTEGER,
            sigla_orgao_programa VARCHAR(10),
            nome_orgao_programa VARCHAR(100),
            id_unidade_gestora_programa INTEGER,
            documentos_origem_programa VARCHAR(100),
            id_unidade_orcamentaria_responsavel_programa INTEGER,
            data_inicio_ciencia_programa TIMESTAMP,
            data_fim_ciencia_programa TIMESTAMP,
            valor_necessidade_financeira_programa NUMERIC(15,2),
            valor_total_disponibilizado_programa NUMERIC(15,2),
            valor_impedido_programa NUMERIC(15,2),
            valor_a_disponibilizar_programa NUMERIC(15,2),
            valor_documentos_habeis_gerados_programa NUMERIC(15,2),
            valor_obs_geradas_programa NUMERIC(15,2),
            valor_disponibilidade_atual_programa NUMERIC(15,2)
        );
        """
        try:
            # Criando tabela
            cursor.execute(create_table_sql)

            # Inserindo dados
            for record in tranformed_data:
                row = tuple(record.values())
                insert_sql = """
                INSERT INTO programas_especiais VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (id_programa) 
                DO UPDATE SET
                    ano_programa = EXCLUDED.ano_programa,
                    modalidade_programa = EXCLUDED.modalidade_programa,
                    codigo_programa = EXCLUDED.codigo_programa,
                    id_orgao_superior_programa = EXCLUDED.id_orgao_superior_programa,
                    sigla_orgao_superior_programa = EXCLUDED.sigla_orgao_superior_programa,
                    nome_orgao_superior_programa = EXCLUDED.nome_orgao_superior_programa,
                    id_orgao_programa = EXCLUDED.id_orgao_programa,
                    sigla_orgao_programa = EXCLUDED.sigla_orgao_programa,
                    nome_orgao_programa = EXCLUDED.nome_orgao_programa,
                    id_unidade_gestora_programa = EXCLUDED.id_unidade_gestora_programa,
                    documentos_origem_programa = EXCLUDED.documentos_origem_programa,
                    id_unidade_orcamentaria_responsavel_programa = EXCLUDED.id_unidade_orcamentaria_responsavel_programa,
                    data_inicio_ciencia_programa = EXCLUDED.data_inicio_ciencia_programa,
                    data_fim_ciencia_programa = EXCLUDED.data_fim_ciencia_programa,
                    valor_necessidade_financeira_programa = EXCLUDED.valor_necessidade_financeira_programa,
                    valor_total_disponibilizado_programa = EXCLUDED.valor_total_disponibilizado_programa,
                    valor_impedido_programa = EXCLUDED.valor_impedido_programa,
                    valor_a_disponibilizar_programa = EXCLUDED.valor_a_disponibilizar_programa,
                    valor_documentos_habeis_gerados_programa = EXCLUDED.valor_documentos_habeis_gerados_programa,
                    valor_obs_geradas_programa = EXCLUDED.valor_obs_geradas_programa,
                    valor_disponibilidade_atual_programa = EXCLUDED.valor_disponibilidade_atual_programa
                """
                cursor.execute(insert_sql, row)
        
            conn.commit()
        except Exception as e:
            conn.rollback()
        finally:
            cursor.close()
            conn.close()


    ## DAG Worflow- ETL Pipeline
    api_data= extract_api_data()
    transformed_data=transform_api_data(api_data)
    load_to_database(transformed_data)
