#!/usr/bin/env python3
"""
Utilitário para listar campos personalizados do Pipedrive
Usa API v1 para buscar campos de pessoas e negócios
"""

import sys
import os
import logging
import requests

# Adicionar pasta src ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from config import active_config
except ImportError:
    # Fallback para importação direta
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from src.config import active_config

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def testar_conexao():
    """Testa conexão com Pipedrive"""
    logger.info(">>> Testando conexão com Pipedrive...")
    
    try:
        # URL da API v1
        url = f"{active_config.PIPEDRIVE_BASE_URL_V1}/users"
        
        params = {
            'api_token': active_config.PIPEDRIVE_API_TOKEN
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                logger.info("OK: Conexão estabelecida com sucesso!")
                return True
            else:
                logger.error(f"Erro na resposta: {data.get('error', 'Erro desconhecido')}")
                return False
        else:
            logger.error(f"Erro HTTP: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Erro na conexão: {e}")
        return False

def buscar_campos_pessoas():
    """Busca campos personalizados de pessoas usando API v1"""
    print(">>> Buscando campos personalizados de PESSOAS...")
    
    try:
        # URL da API v1
        url = f"{active_config.PIPEDRIVE_BASE_URL_V1}/personFields"
        
        params = {
            'api_token': active_config.PIPEDRIVE_API_TOKEN
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('success'):
                fields = data.get('data', [])
                print(f"OK: Encontrados {len(fields)} campos de pessoas")
                
                # Filtrar apenas campos personalizados (não padrão)
                custom_fields = []
                for field in fields:
                    field_id = field.get('id')
                    field_name = field.get('name')
                    field_type = field.get('field_type')
                    field_key = field.get('key')
                    
                    # Campos personalizados têm key diferente de 'title', 'name', etc.
                    if field_key not in ['title', 'name', 'email', 'phone', 'org_name', 'address']:
                        custom_fields.append({
                            'id': field_id,
                            'name': field_name,
                            'type': field_type,
                            'key': field_key,
                            'options': field.get('options', [])
                        })
                
                if custom_fields:
                    print(f"Campos personalizados encontrados ({len(custom_fields)}):")
                    for field in custom_fields:
                        field_type = field['type']
                        field_name = field['name']
                        field_id = field['id']
                        field_key = field['key']
                        
                        # Determinar se é campo de múltipla escolha
                        is_multiple_choice = field_type in ['enum', 'set', 'varchar_options']
                        
                        print(f"   {field_name}")
                        print(f"      ID: {field_id}")
                        print(f"      Tipo: {field_type}")
                        print(f"      Key: {field_key}")
                        
                        # Se for campo de múltipla escolha, mostrar as opções
                        if is_multiple_choice and field['options']:
                            print(f"      Opções disponíveis:")
                            for option in field['options']:
                                option_id = option.get('id')
                                option_label = option.get('label')
                                print(f"         {option_id} = {option_label}")
                        elif field['options']:
                            print(f"      Opções disponíveis:")
                            for option in field['options']:
                                option_id = option.get('id')
                                option_label = option.get('label')
                                print(f"         {option_id} = {option_label}")
                        
                        print()  # Linha em branco para separar campos
                else:
                    print("Nenhum campo personalizado encontrado para pessoas")
                
                return custom_fields
            else:
                print(f"Erro na resposta: {data.get('error', 'Erro desconhecido')}")
                return []
        else:
            print(f"Erro HTTP: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"Erro na busca: {e}")
        return []

def buscar_campos_negocios():
    """Busca campos personalizados de negócios usando API v1"""
    print(">>> Buscando campos personalizados de NEGÓCIOS...")
    
    try:
        # URL da API v1
        url = f"{active_config.PIPEDRIVE_BASE_URL_V1}/dealFields"
        
        params = {
            'api_token': active_config.PIPEDRIVE_API_TOKEN
        }
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            
            if data.get('success'):
                fields = data.get('data', [])
                print(f"OK: Encontrados {len(fields)} campos de negócios")
                
                # Filtrar apenas campos personalizados (não padrão)
                custom_fields = []
                for field in fields:
                    field_id = field.get('id')
                    field_name = field.get('name')
                    field_type = field.get('field_type')
                    field_key = field.get('key')
                    
                    # Campos personalizados têm key diferente de 'title', 'value', etc.
                    if field_key not in ['title', 'value', 'currency', 'status', 'stage_id', 'pipeline_id']:
                        custom_fields.append({
                            'id': field_id,
                            'name': field_name,
                            'type': field_type,
                            'key': field_key,
                            'options': field.get('options', [])
                        })
                
                if custom_fields:
                    print(f"Campos personalizados encontrados ({len(custom_fields)}):")
                    for field in custom_fields:
                        field_type = field['type']
                        field_name = field['name']
                        field_id = field['id']
                        field_key = field['key']
                        
                        # Determinar se é campo de múltipla escolha
                        is_multiple_choice = field_type in ['enum', 'set', 'varchar_options']
                        
                        print(f"   {field_name}")
                        print(f"      ID: {field_id}")
                        print(f"      Tipo: {field_type}")
                        print(f"      Key: {field_key}")
                        
                        # Se for campo de múltipla escolha, mostrar as opções
                        if is_multiple_choice and field['options']:
                            print(f"      Opções disponíveis:")
                            for option in field['options']:
                                option_id = option.get('id')
                                option_label = option.get('label')
                                print(f"         {option_id} = {option_label}")
                        elif field['options']:
                            print(f"      Opções disponíveis:")
                            for option in field['options']:
                                option_id = option.get('id')
                                option_label = option.get('label')
                                print(f"         {option_id} = {option_label}")
                        
                        print()  # Linha em branco para separar campos
                else:
                    print("Nenhum campo personalizado encontrado para negócios")
                
                return custom_fields
            else:
                print(f"Erro na resposta: {data.get('error', 'Erro desconhecido')}")
                return []
        else:
            print(f"Erro HTTP: {response.status_code}")
            return []
            
    except Exception as e:
        print(f"Erro na busca: {e}")
        return []

def buscar_campos_cpf_cnpj(person_fields, deal_fields):
    """Busca campos que podem ser relacionados a CPF/CNPJ"""
    print(">>> Campos que podem ser relacionados a CPF/CNPJ:")
    
    cpf_cnpj_fields = []
    
    # Verificar campos de pessoas
    print("\nPESSOAS:")
    for field in person_fields:
        field_name = field['name'].lower()
        if any(keyword in field_name for keyword in ['cpf', 'cnpj', 'documento', 'rg', 'identidade']):
            print(f"   {field['name']} (ID: {field['id']}, Key: {field['key']})")
            cpf_cnpj_fields.append(field)
    
    # Verificar campos de negócios
    print("\nNEGÓCIOS:")
    for field in deal_fields:
        field_name = field['name'].lower()
        if any(keyword in field_name for keyword in ['cpf', 'cnpj', 'documento', 'cliente', 'devedor']):
            print(f"   {field['name']} (ID: {field['id']}, Key: {field['key']})")
            cpf_cnpj_fields.append(field)
    
    if not cpf_cnpj_fields:
        print("Nenhum campo relacionado a CPF/CNPJ encontrado")
    
    return cpf_cnpj_fields

def gerar_codigo_configuracao(person_fields, deal_fields):
    """Gera código de configuração baseado nos campos encontrados"""
    print("\n>>> Copie e cole no arquivo src/custom_fields_config.py:")
    
    print("\n# ========== CAMPOS DE PESSOAS ==========")
    print("PERSON_FIELDS = {")
    
    if person_fields:
        for field in person_fields:
            field_name = field['name'].upper().replace(' ', '_')
            field_key = field['key']
            print(f"    '{field_name}': '{field_key}',")
    else:
        print("    # Nenhum campo personalizado encontrado")
    
    print("}")
    
    print("\n# ========== CAMPOS DE NEGÓCIOS ==========")
    print("DEAL_FIELDS = {")
    
    if deal_fields:
        for field in deal_fields:
            field_name = field['name'].upper().replace(' ', '_')
            field_key = field['key']
            print(f"    '{field_name}': '{field_key}',")
    else:
        print("    # Nenhum campo personalizado encontrado")
    
    print("}")

def main():
    """Função principal"""
    print("=" * 80)
    print("LISTADOR DE CAMPOS PERSONALIZADOS DO PIPEDRIVE")
    print("=" * 80)
    
    # Testar conexão
    if not testar_conexao():
        logger.error("Falha na conexão. Verifique as configurações.")
        return
    
    print("\n" + "-" * 60)
    print(">> CAMPOS PERSONALIZADOS DE PESSOAS")
    print("-" * 60)
    
    # Buscar campos de pessoas
    person_fields = buscar_campos_pessoas()
    
    print("\n" + "-" * 60)
    print(">> CAMPOS PERSONALIZADOS DE NEGÓCIOS")
    print("-" * 60)
    
    # Buscar campos de negócios
    deal_fields = buscar_campos_negocios()
    
    print("\n" + "-" * 60)
    print(">> CAMPOS RELEVANTES PARA CPF/CNPJ")
    print("-" * 60)
    
    # Buscar campos relacionados a CPF/CNPJ
    cpf_cnpj_fields = buscar_campos_cpf_cnpj(person_fields, deal_fields)
    
    print("\n" + "-" * 60)
    print(">> CONFIGURAÇÃO ATUAL")
    print("-" * 60)
    
    # Mostrar configuração atual
    try:
        from custom_fields_config import CustomFieldsConfig
        CustomFieldsConfig.print_configuration_summary()
    except ImportError:
        print("Não foi possível carregar a configuração atual")
        print("Verifique se o arquivo src/custom_fields_config.py existe")
    
    print("\n" + "-" * 60)
    print(">> CÓDIGO DE CONFIGURAÇÃO SUGERIDO")
    print("-" * 60)
    
    # Gerar código de configuração
    gerar_codigo_configuracao(person_fields, deal_fields)
    
    print("\n" + "=" * 80)
    print("FIM DA LISTAGEM")
    print("=" * 80)

if __name__ == "__main__":
    main() 