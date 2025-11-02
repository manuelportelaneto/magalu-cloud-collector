# main.py
import os
import datetime
import json
import boto3
import requests  # Importa a biblioteca para fazer chamadas HTTP

# Cliente do serviço do Google para buscar segredos
from google.cloud import secretmanager

def get_secret(secret_id, project_id, version="latest"):
    """Função auxiliar para buscar um segredo no GCP Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    
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
        print("--- Iniciando o coletor de custos da Magalu Cloud ---")
        
        # --- 1. Buscar Credenciais e Configurar Clientes ---
        gcp_project_id = os.environ.get("GCP_PROJECT_ID")
        
        # Credenciais para a AWS (para salvar no DynamoDB)
        aws_access_key_id = get_secret("aws-access-key-id-finops", gcp_project_id)
        aws_secret_access_key = get_secret("aws-secret-access-key-finops", gcp_project_id)
        
        # NOVAS credenciais para a Magalu Cloud API
        magalu_api_key = get_secret("magalu-api-key", gcp_project_id)
        magalu_secret_key = get_secret("magalu-secret-key", gcp_project_id)
        
        print(f"Credenciais obtidas com sucesso do projeto GCP: {gcp_project_id}")

        # Cliente DynamoDB (usando credenciais do Secret Manager)
        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name='us-east-1'
        )
        table = dynamodb.Table("CloudMatrix_FinOps_Reports")
        
        # --- 2. Lógica de Coleta de Custo da Magalu Cloud ---
        # A API da Magalu requer as chaves nos headers da requisição
        headers = {
            "x-api-key": magalu_api_key,
            "x-secret-key": magalu_secret_key
        }
        
        # Endpoint da API de Faturamento da Magalu
        billing_api_url = "https://billing.magalu.cloud/v1/billing"
        
        print(f"Consultando a API de faturamento da Magalu em: {billing_api_url}")
        
        response = requests.get(billing_api_url, headers=headers)
        response.raise_for_status()  # Lança uma exceção se a resposta for um erro (4xx ou 5xx)
        
        billing_data = response.json()
        
        # Extrai os dados de custo do retorno da API
        # A estrutura exata do JSON pode variar, ajuste se necessário
        total_cost = billing_data.get('total_cost', 0.00)
        currency = billing_data.get('currency', 'BRL') # Custos da Magalu geralmente em BRL
        
        summary_message = f"Magalu Cloud Monthly Cost (Month-to-Date): {total_cost:.2f} {currency}"
        raw_data = billing_data # Salva a resposta completa da API para análise
        
        print(summary_message)
            
        # --- 3. Preparar e Inserir no DynamoDB ---
        item = {
            'report_date': datetime.datetime.utcnow().isoformat(),
            'cloud_provider': 'MagaluCloud', # Corrigido para o provedor correto
            'report_type': 'MonthlyCostToDate',
            'report_period': datetime.date.today().strftime('%Y-%m'),
            'raw_data': json.dumps(raw_data),
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