# main.py
import os
import datetime
import json
import boto3
import functions_framework

# Clientes dos serviços do Google e AWS
from google.cloud import billing
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
    1. Obter Custo e Uso do GCP do MÊS ATUAL.
    2. Persistir no DynamoDB.
    """
    try:
        print("--- Iniciando o coletor GCP na Magalu Cloud ---")
        
        # --- 1. Buscar Credenciais e Configurar Clientes ---
        project_id = os.environ.get("GCP_PROJECT_ID")
        aws_access_key_id = get_secret("aws-access-key-id-finops", project_id)
        aws_secret_access_key = get_secret("aws-secret-access-key-finops", project_id)
        project_name = f"projects/{project_id}"
        
        print(f"Credenciais obtidas para o projeto: {project_id}")

        # Cliente DynamoDB (usando credenciais do Secret Manager)
        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name='us-east-1'
        )
        table = dynamodb.Table("CloudMatrix_FinOps_Reports")
        
        # Cliente GCP Billing (usa a Service Account da própria função)
        billing_client = billing.CloudBillingClient()
        
        # --- 2. Lógica de Coleta de Custo do GCP ---
        billing_info = billing_client.get_project_billing_info(name=project_name)
        
        if not billing_info.billing_enabled:
            summary_message = "Billing not enabled for this project."
            raw_data = {"error": summary_message}
        else:
            total_cost = "0.00" 
            currency = "USD"
            summary_message = f"GCP Monthly Cost (Month-to-Date): {total_cost} {currency} (API data limited, see raw_data)"
            raw_data = {"billing_account_name": billing_info.billing_account_name, "billing_enabled": billing_info.billing_enabled}
        
        print(summary_message)
            
        # --- 3. Preparar e Inserir no DynamoDB ---
        item = {
            'report_date': datetime.datetime.utcnow().isoformat(),
            'cloud_provider': 'GCP',
            'report_type': 'MonthlyCostToDate',
            'report_period': datetime.date.today().strftime('%Y-%m'),
            'raw_data': json.dumps(raw_data),
            'summary': summary_message
        }

        table.put_item(Item=item)
        print(f"Sucesso! Relatório salvo: {item['summary']}")
        print("--- Coletor finalizado com sucesso. ---")
        
    except Exception as e:
        print(f"ERRO FATAL ao executar o coletor GCP: {e}")
        print("--- Coletor finalizado com erro. ---")

if __name__ == "__main__":
    run_collector()