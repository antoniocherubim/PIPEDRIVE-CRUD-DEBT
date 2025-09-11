"""
Configurações do projeto Pipedrive - Inadimplentes
"""
import os
from typing import List, Set
from dotenv import load_dotenv

# Carregar variáveis do arquivo .env
load_dotenv()

class Config:
    # Configurações da API do Pipedrive - URLs base para ambas versões
    PIPEDRIVE_API_TOKEN = os.getenv('PIPEDRIVE_API_TOKEN', '')
    PIPEDRIVE_DOMAIN = os.getenv('PIPEDRIVE_DOMAIN', 'sicoob-arrudaecorsi')
    PIPEDRIVE_BASE_URL_V1 = f'https://{PIPEDRIVE_DOMAIN}.pipedrive.com/api/v1'
    PIPEDRIVE_BASE_URL_V2 = f'https://{PIPEDRIVE_DOMAIN}.pipedrive.com/api/v2'
    PIPEDRIVE_OWNER_ID = os.getenv('PIPEDRIVE_OWNER_ID', None)  # ID do proprietário padrão
    
    # Endpoints disponíveis na API v2 (os demais usarão v1)
    V2_ENDPOINTS: Set[str] = {
        # Core entities com v2
        'deals', 'persons', 'organizations', 'products', 'activities',
        'pipelines', 'stages',
        
        # Search endpoints
        'deals/search', 'persons/search', 'organizations/search', 
        'products/search', 'leads/search', 'itemSearch', 'itemSearch/field',
        
        # Followers endpoints
        'deals/{id}/followers', 'persons/{id}/followers', 
        'organizations/{id}/followers', 'products/{id}/followers',
        'users/{id}/followers',
        
        # Deal products
        'deals/{id}/products', 'deals/products',
        
        # Product variations
        'products/{id}/variations'
    }
    
    # Configurações de arquivos
    TXT_INPUT_FOLDER = os.getenv('TXT_INPUT_FOLDER', './input/escritorio_cobranca')
    EXCEL_OUTPUT_FOLDER = os.getenv('EXCEL_OUTPUT_FOLDER', './input/excel')
    GARANTINORTE_FOLDER = os.getenv('GARANTINORTE_FOLDER', './input/garantinorte')
    BACKUP_FOLDER = os.getenv('BACKUP_FOLDER', './backup')
    
    # ========== CONFIGURAÇÕES DOS FUNIS BASE NOVA ==========
    # IDs dos 3 funis principais - BASEADO NO RESULTADO DO TESTE
    PIPELINE_BASE_NOVA_SDR_ID = 14  # ID do funil "BASE NOVA - SDR"
    PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID = 15  # ID do funil "BASE NOVA - Negociação"
    PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID = 17  # ID do funil "BASE NOVA - Formalização/Pagamento"
    
    # ========== CONFIGURAÇÕES DE FUNIS ESPECIAIS ==========
    # Pipeline JUDICIAL - não deve ser modificada
    PIPELINE_JUDICIAL_ID = 3  # ID do funil "JUDICIAL"
    
    # ========== CONFIGURAÇÕES DAS ETAPAS ==========
    # IDs REAIS dos estágios dos funis BASE NOVA (baseado no resultado do script)
    # Etapa principal para novos negócios
    STAGE_NOVAS_COBRANÇAS_ID = 110  # ID da etapa "NOVOS CONTRATOS" no funil BASE NOVA - SDR
    
    # Etapas "Iniciar Cobrança" - para reabrir casos perdidos
    STAGE_INICIAR_COBRANCA_ID = 115  # ID da etapa "Iniciar Cobrança" (principal)
    STAGE_INICIAR_COBRANCA_ALT_ID = 208  # ID alternativo da etapa "Iniciar Cobrança"
    
    # Etapas de exceção (não devem ser marcadas como perdidas)
    STAGE_ENVIAR_MINUTA_BOLETO_ID = 124  # ID da etapa "-> Enviar Boleto" no funil Formalização/Pagamento
    STAGE_AGUARDANDO_PAGAMENTO_ID = 173  # ID da etapa "Agdo Pagamento" no funil Negociação
    STAGE_ACOMPANHAMENTO_ACORDO_ID = 176  # ID da etapa "Acp. Acordo parcelado" no funil Negociação
    STAGE_BOLETO_PAGO_ID = 174  # ID da etapa "-> Boleto Pago" no funil Negociação
    
    # Lista de funis específicos para compatibilidade
    FUNIS_ESPECIFICOS: List[int] = [
        PIPELINE_BASE_NOVA_SDR_ID,
        PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID,
        PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID,
        PIPELINE_JUDICIAL_ID
    ]
    
    # Configurações de log
    LOG_LEVEL = 'INFO'
    LOGS_FOLDER = './logs'
    
    @classmethod
    def setup_logging(cls, script_name: str = 'pipedrive_system'):
        """
        Configura o sistema de logging com arquivo de log com timestamp
        
        Args:
            script_name: Nome do script para identificar o log
        """
        import logging
        import os
        from datetime import datetime
        
        # Criar pasta logs se não existir
        os.makedirs(cls.LOGS_FOLDER, exist_ok=True)
        
        # Gerar nome do arquivo com timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"{script_name}_{timestamp}.log"
        log_filepath = os.path.join(cls.LOGS_FOLDER, log_filename)
        
        # Configurar logging
        logging.basicConfig(
            level=getattr(logging, cls.LOG_LEVEL.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filepath, encoding='utf-8'),
                logging.StreamHandler()  # Também mostra no console
            ]
        )
        
        # Log inicial
        logger = logging.getLogger(__name__)
        logger.info(f"=== INICIANDO SISTEMA PIPEDRIVE ===")
        logger.info(f"Arquivo de log: {log_filepath}")
        logger.info(f"Nível de log: {cls.LOG_LEVEL}")
        
        return logger
    
    # Configurações de processamento
    CHUNK_SIZE = 100  # Quantidade de registros processados por vez
    
    # Configurações para API híbrida v1/v2
    DEFAULT_LIMIT = 500  # Limite máximo do Pipedrive
    
    @classmethod
    def get_api_version_for_endpoint(cls, endpoint: str) -> str:
        """
        Determina qual versão da API usar baseado no endpoint
        
        Args:
            endpoint: Endpoint da API (ex: 'deals', 'users/me', 'deals/123/products')
            
        Returns:
            'v1' ou 'v2'
        """
        # Normalizar endpoint removendo IDs específicos para comparação
        normalized_endpoint = endpoint
        
        # Substituir IDs numéricos por placeholder para comparação
        import re
        normalized_endpoint = re.sub(r'/\d+/', '/{id}/', normalized_endpoint)
        normalized_endpoint = re.sub(r'/\d+$', '/{id}', normalized_endpoint)
        
        # Verificar se o endpoint (ou sua forma normalizada) está na lista v2
        if normalized_endpoint in cls.V2_ENDPOINTS or endpoint in cls.V2_ENDPOINTS:
            return 'v2'
        
        # Verificar padrões específicos
        if endpoint.startswith(('deals', 'persons', 'organizations', 'products', 'activities')):
            # Endpoints principais têm v2, exceto alguns específicos
            if not any(v1_only in endpoint for v1_only in ['fields', 'filters', 'timeline']):
                return 'v2'
        
        if endpoint.startswith(('pipelines', 'stages')):
            return 'v2'
            
        if '/search' in endpoint or 'itemSearch' in endpoint:
            return 'v2'
        
        # Por padrão, usar v1 para endpoints não mapeados
        return 'v1'
    
    @classmethod
    def get_base_url_for_endpoint(cls, endpoint: str) -> str:
        """
        Retorna a URL base correta baseada no endpoint
        
        Args:
            endpoint: Endpoint da API
            
        Returns:
            URL base (v1 ou v2)
        """
        version = cls.get_api_version_for_endpoint(endpoint)
        return cls.PIPEDRIVE_BASE_URL_V2 if version == 'v2' else cls.PIPEDRIVE_BASE_URL_V1
    
    @classmethod
    def validate_config(cls) -> bool:
        """Valida se as configurações obrigatórias estão definidas"""
        if not cls.PIPEDRIVE_API_TOKEN:
            print("ERRO: PIPEDRIVE_API_TOKEN não está definido!")
            return False
        
        # Validar se os IDs dos funis estão configurados
        funis_config = [
            cls.PIPELINE_BASE_NOVA_SDR_ID,
            cls.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID,
            cls.PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID
        ]
        
        if any(funil_id <= 0 for funil_id in funis_config):
            print("AVISO: IDs dos funis BASE NOVA ainda são placeholders!")
            print("Atualize os seguintes IDs no config.py:")
            print(f"- PIPELINE_BASE_NOVA_SDR_ID: {cls.PIPELINE_BASE_NOVA_SDR_ID}")
            print(f"- PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID: {cls.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID}")
            print(f"- PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID: {cls.PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID}")
        
        return True

# Configurações específicas por ambiente
class DevelopmentConfig(Config):
    DEBUG = True

class ProductionConfig(Config):
    DEBUG = False

# Configuração ativa (alterar conforme ambiente)
active_config = DevelopmentConfig() 