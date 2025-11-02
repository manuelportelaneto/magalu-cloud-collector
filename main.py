import os
import datetime
import json
import boto3
import requests

from google.cloud import secretmanager

def get_secret(client, project_id, secret_id, version="latest"):
    """Função auxiliar para buscar um segredo no GCP Secret Manager."""
    # Constrói o nome completo do recurso do segredo
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    
    # Acessa a payload do segredo
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")

def run_collector():
    """
    Função principal.
    1. Obter Custo e Uso da MAGALU CLOUD do MÊS ATUAL.
    2. Persistir no DynamoDB.
    """
    try:
        print("--- Iniciando o coletor de custos da Magalu Cloud (v2) ---")
        
        # --- 1. Buscar Credenciais e Configurar Clientes ---
        # A única variável de ambiente que precisamos é a que aponta para nossa chave
        credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        if not credentials_path:
            raise ValueError("A variável de ambiente GOOGLE_APPLICATION_CREDENTIALS não está definida.")

        # O código agora descobre o project_id sozinho lendo o arquivo de chave!
        with open(credentials_path, 'r') as f:
            credentials_info = json.load(f)
        gcp_project_id = credentials_info.get("project_id")
        if not gcp_project_id:
            raise ValueError(f"Não foi possível encontrar o 'project_id' no arquivo de credenciais: {credentials_path}")

        print(f"Projeto GCP identificado a partir das credenciais: {gcp_project_id}")
        
        # Cliente do Secret Manager
        secret_client = secretmanager.SecretManagerServiceClient()

        # Credenciais para a AWS
        aws_access_key_id = get_secret(secret_client, gcp_project_id, "aws-access-key-id-finops")
        aws_secret_access_key = get_secret(secret_client, gcp_project_id, "aws-secret-access-key-finops")
        
        # Credenciais para a Magalu Cloud
        magalu_api_key = get_secret(secret_client, gcp_project_id, "magalu-api-key")
        magalu_secret_key = get_secret(secret_client, gcp_project_id, "magalu-secret-key")

        # Cliente DynamoDB
        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name='us-east-1'
        )
        table = dynamodb.Table("CloudMatrix_FinOps_Reports")
        
        # --- 2. Lógica de Coleta de Custo da Magalu Cloud ---
        headers = {
            "x-api-key": magalu_api_key,
            "x-secret-key": magalu_secret_key
        }
        billing_api_url = "https://billing.magalu.cloud/v1/billing"
        
        print(f"Consultando a API de faturamento da Magalu: {billing_api_url}")
        
        response = requests.get(billing_api_url, headers=headers, timeout=30)
        response.raise_for_status()
        billing_data = response.json()
        
        total_cost = billing_data.get('total_cost', 0.00)
        currency = billing_data.get('currency', 'BRL')
        
        summary_message = f"Magalu Cloud Monthly Cost (Month-to-Date): {total_cost:.2f} {currency}"
        print(summary_message)
            
        # --- 3. Preparar e Inserir no DynamoDB ---
        item = {
            'report_date': datetime.datetime.utcnow().isoformat(),
            'cloud_provider': 'MagaluCloud',
            'report_type': 'MonthlyCostToDate',
            'report_period': datetime.date.today().strftime('%Y-%m'),
            'raw_data': json.dumps(billing_data),
            'summary': summary_message
        }

        table.put_item(Item=item)
        print(f"Sucesso! Relatório salvo: {item['summary']}")
        print("--- Coletor finalizado com sucesso. ---")
        
    except Exception as e:
        print(f"ERRO FATAL ao executar o coletor da Magalu Cloud: {e}")
        print("--- Coletor finalizado com erro. ---")

if __name__ == "__main__":
    run_collector()