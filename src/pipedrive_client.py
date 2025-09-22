"""
Cliente para interação com a API do Pipedrive
Suporta APIs v1 e v2 com sistema híbrido
"""
import requests
import logging
from typing import Dict, List, Optional, Any
from config import active_config
from custom_fields_config import CustomFieldsConfig

logger = logging.getLogger(__name__)

class PipedriveClient:
    def __init__(self):
        self.api_token = active_config.PIPEDRIVE_API_TOKEN
        self.base_url_v1 = active_config.PIPEDRIVE_BASE_URL_V1
        self.base_url_v2 = active_config.PIPEDRIVE_BASE_URL_V2
        self.v2_endpoints = active_config.V2_ENDPOINTS
        
        if not self.api_token:
            raise ValueError("PIPEDRIVE_API_TOKEN não configurado")
    
    def _get_base_url(self, endpoint: str) -> str:
        """Determina se deve usar API v1 ou v2 baseado no endpoint"""
        # Normalizar endpoint para comparação
        normalized_endpoint = endpoint.replace('/', '')
        
        # Verificar se é um endpoint v2
        for v2_endpoint in self.v2_endpoints:
            v2_normalized = v2_endpoint.replace('/', '').replace('{id}', '')
            if normalized_endpoint.startswith(v2_normalized):
                return self.base_url_v2
        
        return self.base_url_v1
    
    def _make_request(self, method: str, endpoint: str, data: Dict = None, params: Dict = None) -> Dict:
        """
        Faz requisição para a API do Pipedrive
        Usa v2 quando disponível, v1 como fallback
        """
        base_url = self._get_base_url(endpoint)
        url = f"{base_url}/{endpoint}"
        
        # Preparar parâmetros
        if params is None:
            params = {}
        params['api_token'] = self.api_token
        
        # Headers
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        try:
            # Fazer a requisição HTTP
            if method == 'GET':
                response = requests.get(url, params=params, headers=headers)
            elif method == 'POST':
                response = requests.post(url, json=data, params=params, headers=headers)
            elif method == 'PUT':
                response = requests.put(url, json=data, params=params, headers=headers)
            elif method == 'PATCH':
                response = requests.patch(url, json=data, params=params, headers=headers)
            elif method == 'DELETE':
                response = requests.delete(url, params=params, headers=headers)
            else:
                raise ValueError(f"Método HTTP não suportado: {method}")
            
            # TRATAMENTO MELHORADO DE ERROS HTTP
            if response.status_code >= 400:
                # Erro do cliente (4xx) ou servidor (5xx)
                try:
                    error_data = response.json()
                    error_message = error_data.get('error', f'HTTP {response.status_code}: {response.reason}')
                    logger.error(f"Erro na requisição {method} {url}: {error_message}")
                    logger.error(f"Status Code: {response.status_code}")
                    logger.error(f"Response: {response.text}")
                    return {'success': False, 'error': error_message, 'status_code': response.status_code}
                except ValueError:
                    # Resposta não é JSON válido
                    error_message = f'HTTP {response.status_code}: {response.reason}'
                    logger.error(f"Erro na requisição {method} {url}: {error_message}")
                    logger.error(f"Response: {response.text}")
                    return {'success': False, 'error': error_message, 'status_code': response.status_code}
            
            # Sucesso - tentar decodificar JSON
            try:
                return response.json()
            except ValueError:
                # Resposta não é JSON válido, mas status é de sucesso
                logger.warning(f"Resposta não-JSON para {method} {url}: {response.text}")
                return {'success': True, 'data': None, 'raw_response': response.text}
            
        except requests.exceptions.Timeout as e:
            error_msg = f"Timeout na requisição {method} {url}: {e}"
            logger.error(error_msg)
            return {'success': False, 'error': error_msg}
            
        except requests.exceptions.ConnectionError as e:
            error_msg = f"Erro de conexão {method} {url}: {e}"
            logger.error(error_msg)
            return {'success': False, 'error': error_msg}
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Erro na requisição {method} {url}: {e}"
            logger.error(error_msg)
            
            # Tentar extrair informações do erro da resposta
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    detailed_error = error_data.get('error', str(e))
                    logger.error(f"Detalhes do erro: {detailed_error}")
                    return {'success': False, 'error': detailed_error, 'status_code': e.response.status_code}
                except:
                    logger.error(f"Resposta de erro: {e.response.text}")
                    return {'success': False, 'error': f"{e.response.status_code}: {e.response.text}"}
            
            return {'success': False, 'error': str(e)}
    
    def _paginate_requests(self, endpoint: str, params: Dict = None) -> List[Dict]:
        """
        Faz requisições paginadas para a API com suporte híbrido v1/v2
        """
        all_data = []
        
        if params is None:
            params = {}
        
        # Determinar versão da API
        api_version = self._get_api_version_for_endpoint(endpoint)
        
        if api_version == 'v2':
            # API v2 usa cursor-based pagination
            return self._paginate_v2(endpoint, params)
        else:
            # API v1 usa page-based pagination
            return self._paginate_v1(endpoint, params)
    
    def _paginate_v1(self, endpoint: str, params: Dict) -> List[Dict]:
        """
        Paginação para API v1 (page-based)
        """
        all_data = []
        page = 1
        
        while True:
            params['page'] = page
            params['limit'] = 500  # Limite máximo do Pipedrive
            
            # Fazer requisição direta para v1
            url = f"{self.base_url_v1}/{endpoint}"
            params['api_token'] = self.api_token
            
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            try:
                import requests
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                result = response.json()
                
                if not result.get('success', False):
                    logger.error(f"Erro na paginação v1: {result.get('error', 'Erro desconhecido')}")
                    break
            except Exception as e:
                logger.error(f"Erro na requisição: {e}")
                break
            
            data = result.get('data', [])
            if not data:
                break
            
            all_data.extend(data)
            
            # Verificar se há mais páginas
            additional_data = result.get('additional_data', {})
            pagination = additional_data.get('pagination', {})
            
            if not pagination.get('more_items_in_collection', False):
                break
            
            page += 1
        
        return all_data
    
    def _paginate_v2(self, endpoint: str, params: Dict) -> List[Dict]:
        """
        Paginação para API v2 (cursor-based)
        """
        all_data = []
        cursor = None
        
        while True:
            # Criar uma cópia dos parâmetros para não modificar o original
            request_params = params.copy()
            request_params['limit'] = 500  # Limite máximo do Pipedrive
            
            if cursor:
                request_params['cursor'] = cursor
            
            result = self._make_request('GET', endpoint, params=request_params)
            
            if not result.get('success', False):
                logger.error(f"Erro na paginação v2: {result.get('error', 'Erro desconhecido')}")
                break
            
            data = result.get('data', [])
            if not data:
                break
            
            all_data.extend(data)
            logger.debug(f"Página v2: {len(data)} itens (Total: {len(all_data)})")
            
            # Verificar se há mais páginas usando next_cursor diretamente
            additional_data = result.get('additional_data', {})
            next_cursor = additional_data.get('next_cursor')
            
            if not next_cursor:
                logger.debug("Não há mais páginas na v2")
                break
            
            cursor = next_cursor
        
        logger.info(f"Paginação v2 concluída: {len(all_data)} itens total")
        return all_data
    
    def _get_api_version_for_endpoint(self, endpoint: str) -> str:
        """
        Determina versão da API baseada no endpoint
        """
        base_url = self._get_base_url(endpoint)
        return 'v2' if 'v2' in base_url else 'v1'
    
    def test_connection(self) -> bool:
        """Testa conexão com a API"""
        logger.info("Testando conexão com Pipedrive...")
        
        result = self._make_request('GET', 'users/me')
        
        if result.get('success'):
            user_data = result.get('data', {})
            logger.info(f"Conectado como: {user_data.get('name', 'N/A')} - {user_data.get('email', 'N/A')}")
            return True
        else:
            logger.error(f"Falha na conexão: {result.get('error', 'Erro desconhecido')}")
            return False
    
    def search_person_by_cpf(self, cpf: str) -> Optional[Dict]:
        """
        Busca pessoa por CPF usando API v2 com múltiplas tentativas
        DEPRECATED: Use search_person_by_document(document, 'PF') para melhor precisão
        """
        logger.info(f"Buscando pessoa por CPF: {cpf}")
        
        # Tentar primeiro a nova lógica assumindo PF
        result = self.search_person_by_document(cpf, 'PF')
        if result:
            return result
        
        # Fallback para a lógica antiga se não encontrou
        logger.debug("Tentando busca com lógica antiga como fallback")
        
        # Usar o ID do campo CPF da configuração
        cpf_field_id = CustomFieldsConfig.get_person_field_id('CPF')
        if not cpf_field_id:
            logger.error("ID do campo CPF não encontrado na configuração")
            return None
        
        # Gerar variações com a função antiga
        cpf_variants = self._normalize_document(cpf)
        return self._search_person_by_cpf_fallback(cpf_variants)
    
    def _search_person_by_cpf_fallback(self, cpf_variants: List[str]) -> Optional[Dict]:
        """
        Busca alternativa quando o campo CPF não é pesquisável
        Tenta buscar por nome ou busca geral
        """
        logger.info("Executando busca alternativa por CPF")
        
        for cpf_variant in cpf_variants:
            if not cpf_variant or len(cpf_variant.strip()) < 2:
                continue
                
            # Tentar busca geral (sem especificar campo)
            logger.debug(f"Tentando busca geral com: {cpf_variant}")
            
            result = self._make_request('GET', 'persons/search', params={
                'term': cpf_variant.strip(),
                'exact_match': False
        })
        
        if result and result.get('success') and result.get('data'):
            items = result['data'].get('items', [])
            for item_data in items:
                item = item_data.get('item', {})
                # Verificar se o CPF realmente confere nos campos personalizados
                if self._verify_cpf_match(item, cpf_variant):
                    logger.info(f"Pessoa encontrada via busca alternativa: {cpf_variant}")
                    return item
        
        logger.info("Pessoa não encontrada nem com busca alternativa")
        return None
    
    def _verify_cpf_match(self, person: Dict, target_cpf: str) -> bool:
        """
        Verifica se uma pessoa tem o CPF desejado nos campos personalizados
        """
        custom_fields = person.get('custom_fields', {})
        cpf_field_id = CustomFieldsConfig.get_person_field_id('CPF')
        
        if cpf_field_id in custom_fields:
            person_cpf = str(custom_fields[cpf_field_id]).strip()
            person_cpf_clean = self._clean_document(person_cpf)
            target_cpf_clean = self._clean_document(target_cpf)
            
            return person_cpf_clean == target_cpf_clean
        
        return False
    
    def search_person_by_cnpj(self, cnpj: str) -> Optional[Dict]:
        """Busca pessoa por CNPJ usando API v2"""
        logger.info(f"Buscando pessoa por CNPJ: {cnpj}")
        
        # Para CNPJ, assumir que usa o mesmo campo que CPF (ID CPF/CNPJ)
        cnpj_field_id = CustomFieldsConfig.get_person_field_id('CPF')
        
        result = self._make_request('GET', 'persons/search', params={
            'term': cnpj,
            'field': cnpj_field_id,
            'exact_match': False  # Alterado para False para encontrar CNPJs sem zeros à esquerda
        })
        
        if result.get('success') and result.get('data'):
            items = result['data'].get('items', [])
            return items[0].get('item') if items else None
        return None
    
    def search_organization_by_cnpj(self, cnpj: str) -> Optional[Dict]:
        """
        Busca organização por CNPJ usando API v2 com múltiplas tentativas
        DEPRECATED: Use search_person_by_document(document, 'PJ') para melhor precisão
        """
        logger.info(f"Buscando organização por CNPJ: {cnpj}")
        
        # Tentar primeiro a nova lógica assumindo PJ
        result = self.search_person_by_document(cnpj, 'PJ')
        if result:
            return result
        
        # Fallback para busca alternativa
        logger.debug("Tentando busca com lógica antiga como fallback")
        cnpj_variants = self._normalize_document(cnpj)
        return self._search_organization_fallback(cnpj_variants)
    
    def create_person(self, name: str, document: str, additional_data: Dict = None, person_type: str = 'PF') -> Optional[Dict]:
        """
        Cria nova pessoa no Pipedrive usando API v2
        
        Args:
            name: Nome da pessoa
            document: CPF ou CNPJ
            additional_data: Dados adicionais da pessoa
            person_type: Tipo da pessoa (PF ou PJ)
        """
        logger.info(f"Criando pessoa: {name} - {person_type}")
        
        person_data = {
            'owner_id': active_config.PIPEDRIVE_OWNER_ID if hasattr(active_config, 'PIPEDRIVE_OWNER_ID') else None,
        }
        
        # CORREÇÃO: Verificar se additional_data contém first_name/last_name
        # Se sim, usar esses campos; se não, usar name
        if additional_data and ('first_name' in additional_data or 'last_name' in additional_data):
            # Usar first_name e last_name se disponíveis
            if 'first_name' in additional_data:
                person_data['first_name'] = additional_data['first_name']
            if 'last_name' in additional_data:
                person_data['last_name'] = additional_data['last_name']
        else:
            # Usar name como fallback
            person_data['name'] = name
        
        # Determinar versão da API
        api_version = active_config.get_api_version_for_endpoint('persons')
        person_custom_fields = {}
        
        # Adicionar documento usando o ID do campo configurado
        # CORREÇÃO: Remover duplicação - o CPF será processado junto com outros campos personalizados
        # cpf_field_id = CustomFieldsConfig.get_person_field_id('CPF')
        # if cpf_field_id != 'SUBSTITUIR_PELO_ID_REAL':
        #     person_custom_fields[cpf_field_id] = document
        
        # CORREÇÃO: Adicionar CPF ao additional_data para processamento uniforme
        if additional_data is None:
            additional_data = {}
        additional_data['CPF'] = document
        
        # CORREÇÃO: Tratar campos especiais para API v2
        phones = []
        emails = []
        
        # Adicionar dados adicionais
        if additional_data:
            for key, value in additional_data.items():
                # CORREÇÃO: Limitar campos short text a 255 caracteres
                if isinstance(value, str):
                    value = self._limit_short_text_fields(value, 255)
                
                # CORREÇÃO: Remover zeros à esquerda de campos numéricos específicos
                if key in ['phone', 'telefone', 'numero_contrato', 'numero_operacao', 'id_operacao']:
                    if isinstance(value, str):
                        value = self._remove_leading_zeros(value)
                
                # CORREÇÃO: API v2 requer phones e emails como arrays
                if api_version == 'v2':
                    if key == 'phone':
                        # Converter telefone único para array
                        phones.append({'value': value, 'primary': True})
                        continue
                    elif key == 'email':
                        # Converter email único para array
                        emails.append({'value': value, 'primary': True})
                        continue
                    elif key == 'address':
                        # CORREÇÃO: Converter endereço para formato de objeto da API v2 e usar como campo personalizado
                        address_obj = self._format_address_for_api_v2(value)
                        field_id = CustomFieldsConfig.get_person_field_id('ENDERECO')
                        if field_id != 'SUBSTITUIR_PELO_ID_REAL':
                            person_custom_fields[field_id] = address_obj
                        continue
                
                # Verificar se é campo personalizado (usar nomes para mapeamento na API v2)
                if key in ['CPF', 'DATA_NASCIMENTO', 'IDADE', 
                          'ESTADO_CIVIL', 'CONDICAO_CPF', 'ENDERECO']:
                    # Para API v2, usar o nome do campo; a estruturação será feita depois
                    field_id = CustomFieldsConfig.get_person_field_id(key)
                    if field_id != 'SUBSTITUIR_PELO_ID_REAL':
                        # CORREÇÃO: Formatar campos personalizados conforme tipo usando a função específica
                        formatted_value = self._format_custom_field_value_for_api_v2(key, value)
                        if formatted_value is not None:
                            # CORREÇÃO: Adicionar log detalhado para debug
                            logger.debug(f"Campo personalizado {key} (ID: {field_id}): valor={value} (tipo: {type(value)}) -> formatado={formatted_value} (tipo: {type(formatted_value)})")
                            person_custom_fields[field_id] = formatted_value
                elif key in ['first_name', 'last_name']:
                    # CORREÇÃO: first_name e last_name já foram tratados acima
                    # Não adicionar novamente para evitar conflito
                    continue
                else:
                    # Campos padrão (não personalizados)
                    person_data[key] = value
        
        # CORREÇÃO: Adicionar phones e emails como arrays para API v2
        if api_version == 'v2':
            if phones:
                person_data['phones'] = phones
            if emails:
                person_data['emails'] = emails
        
        # Estruturar campos baseado na versão da API
        if api_version == 'v2' and person_custom_fields:
            person_data['custom_fields'] = person_custom_fields
        else:
            # API v1: campos personalizados direto no corpo
            person_data.update(person_custom_fields)
        
        # Validação: se vier org_id, garantir que a organização exista; senão, remover
        if 'org_id' in person_data and person_data['org_id']:
            try:
                if not self.get_organization_by_id(int(person_data['org_id'])):
                    logger.warning(f"org_id {person_data['org_id']} não existe/acessível. Removendo antes do envio.")
                    person_data.pop('org_id', None)
            except Exception:
                # Se não conseguir validar, melhor remover para evitar 400
                logger.warning(f"Falha ao validar org_id {person_data.get('org_id')}. Removendo antes do envio.")
                person_data.pop('org_id', None)
        
        result = self._make_request('POST', 'persons', data=person_data)
        
        if result.get('success'):
            logger.info(f"Pessoa criada com sucesso: ID {result['data']['id']}")
            return result['data']
        else:
            logger.error(f"Erro ao criar pessoa: {result.get('error', 'Erro desconhecido')}")
            status_code = result.get('status_code')
            error_msg = result.get('error', '')
            # Fallback: se erro 400 por org_id inválido, tentar novamente sem org_id
            if status_code == 400 and 'org_id' in person_data:
                logger.warning("Tentando criar pessoa novamente sem org_id devido a erro 400 de organização inválida.")
                retry_payload = {k: v for k, v in person_data.items() if k != 'org_id'}
                retry = self._make_request('POST', 'persons', data=retry_payload)
                if retry.get('success'):
                    logger.info(f"Pessoa criada (fallback sem org_id): ID {retry['data']['id']}")
                    return retry['data']
            return None
    
    def create_organization(self, name: str, cnpj: str, additional_data: Dict = None) -> Optional[Dict]:
        """
        Cria nova organização no Pipedrive usando API v2
        
        Args:
            name: Nome da organização
            cnpj: CNPJ da organização
            additional_data: Dados adicionais da organização
        """
        logger.info(f"Criando organização: {name}")
        
        organization_data = {
            'name': name,
            'owner_id': active_config.PIPEDRIVE_OWNER_ID if hasattr(active_config, 'PIPEDRIVE_OWNER_ID') else None,
        }
        
        # Determinar versão da API
        api_version = active_config.get_api_version_for_endpoint('organizations')
        org_custom_fields = {}
        
        # Adicionar CNPJ usando o ID do campo configurado
        cnpj_field_id = CustomFieldsConfig.get_organization_field_id('CPF_CNPJ')
        if cnpj_field_id != 'SUBSTITUIR_PELO_ID_REAL':
            org_custom_fields[cnpj_field_id] = cnpj
        
        # Adicionar dados adicionais
        if additional_data:
            for key, value in additional_data.items():
                # Verificar se é campo personalizado
                if key in ['TELEFONE', 'ENDERECO', 'NOME_FANTASIA', 'CNAE', 
                          'QUANTIDADE_FUNCIONARIOS', 'SOCIOS', 'DATA_ABERTURA', 
                          'EMAILS', 'TELEFONE_HOT', 'NOME_EMPRESA', 'CPF_CNPJ']:
                    field_id = CustomFieldsConfig.get_organization_field_id(key)
                    if field_id != 'SUBSTITUIR_PELO_ID_REAL':
                        org_custom_fields[field_id] = value
                else:
                    # Campos padrão (não personalizados) - filtrar campos proibidos
                    if key not in ['notes']:  # CORREÇÃO: Campo 'notes' não é permitido na API v2 para organizações
                        organization_data[key] = value
                    else:
                        logger.warning(f"Campo '{key}' não é permitido para organizações na API v2 - ignorando")
        
        # Estruturar campos baseado na versão da API
        if api_version == 'v2' and org_custom_fields:
            organization_data['custom_fields'] = org_custom_fields
        else:
            # API v1: campos personalizados direto no corpo
            organization_data.update(org_custom_fields)
        
        logger.debug(f"Dados da organização para envio (API {api_version}): {organization_data}")
        
        result = self._make_request('POST', 'organizations', data=organization_data)
        
        if result.get('success'):
            logger.info(f"Organização criada com sucesso: ID {result['data']['id']}")
            return result['data']
        else:
            # Tratamento melhorado de erros
            error_msg = result.get('error', 'Erro desconhecido')
            status_code = result.get('status_code', 'N/A')
            
            logger.error(f"Erro ao criar organização '{name}': {error_msg}")
            logger.error(f"Status Code: {status_code}")
            logger.error(f"Dados enviados: {organization_data}")
            
            # Log específico para erro 400 (Bad Request)
            if status_code == 400:
                logger.error("⚠️  Erro 400: Verifique se todos os campos enviados são válidos para a API v2")
                
                # Verificar se há campos proibidos
                forbidden_fields = [k for k in organization_data.keys() if k in ['notes']]
                if forbidden_fields:
                    logger.error(f"Campos proibidos detectados: {forbidden_fields}")
            
            return None
    
    def create_deal(self, title: str, person_id: int = None, pipeline_id: int = None, stage_id: int = None, value: float = None, custom_fields: Dict = None, org_id: int = None) -> Optional[Dict]:
        """
        Cria novo negócio no Pipedrive usando API v2
        
        Args:
            title: Título do negócio
            person_id: ID da pessoa vinculada (opcional)
            pipeline_id: ID do funil
            stage_id: ID da etapa
            value: Valor do negócio
            custom_fields: Campos personalizados
            org_id: ID da organização vinculada (opcional)
        """
        logger.info(f"Criando negócio: {title}")
        
        deal_data = {
            'title': title,
            'owner_id': active_config.PIPEDRIVE_OWNER_ID if hasattr(active_config, 'PIPEDRIVE_OWNER_ID') else None,
            'status': 'open'
        }
        
        # Adicionar person_id ou org_id
        if person_id:
            deal_data['person_id'] = person_id
        if org_id:
            deal_data['org_id'] = org_id
        
        if pipeline_id:
            deal_data['pipeline_id'] = pipeline_id
        if stage_id:
            deal_data['stage_id'] = stage_id
        if value:
            deal_data['value'] = value
        
        # Adicionar campos personalizados usando configuração
        if custom_fields:
            # Determinar versão da API
            api_version = active_config.get_api_version_for_endpoint('deals')
            deal_custom_fields = {}
            
            for key, value in custom_fields.items():
                # Tentar mapear o nome do campo para o ID configurado
                field_id = CustomFieldsConfig.get_deal_field_id(key)
                if field_id != 'SUBSTITUIR_PELO_ID_REAL':
                    # TAG_ATRASO é campo de múltipla escolha - deve ser enviado como array
                    if key == 'TAG_ATRASO':
                        deal_custom_fields[field_id] = [value]  # Converter para array
                    else:
                        deal_custom_fields[field_id] = value
            
            # Estruturar campos baseado na versão da API
            if api_version == 'v2' and deal_custom_fields:
                deal_data['custom_fields'] = deal_custom_fields
            else:
                # API v1: campos personalizados direto no corpo
                deal_data.update(deal_custom_fields)
        
        result = self._make_request('POST', 'deals', data=deal_data)
        
        if result.get('success'):
            logger.info(f"Negócio criado com sucesso: ID {result['data']['id']}")
            return result['data']
        else:
            logger.error(f"Erro ao criar negócio: {result.get('error', 'Erro desconhecido')}")
            return None
    
    def update_deal(self, deal_id: int, data: Dict) -> bool:
        """
        Atualiza negócio existente usando PATCH (v2) ou PUT (v1)
        
        Args:
            deal_id: ID do negócio
            data: Dados para atualização
        """
        logger.info(f"Atualizando negócio ID: {deal_id}")
        logger.debug(f"Dados recebidos para atualização: {data}")
        
        endpoint = f'deals/{deal_id}'
        api_version = active_config.get_api_version_for_endpoint(endpoint)
        
        # Separar campos padrão de campos personalizados
        standard_fields = {}
        custom_fields = {}
        
        for key, value in data.items():
            # Pular campos None ou vazios
            if value is None:
                continue
                
            # Tentar mapear campos personalizados
            if key in ['ID_CPF_CNPJ', 'COOPERADO', 'COOPERATIVA', 'TODOS_CONTRATOS', 
                      'TODAS_OPERACOES', 'VENCIMENTO_MAIS_ANTIGO', 'NUMERO_CONTRATO',
                      'TIPO_ACAO_CARTEIRA', 'AVALISTAS', 'DIAS_DE_ATRASO', 
                      'VALOR_TOTAL_DA_DIVIDA', 'VALOR_TOTAL_VENCIDO', 'CONDICAO_CPF',
                      'VALOR_TOTAL_COM_JUROS', 'TOTAL_DE_PARCELAS', 'TAG_ATRASO',
                      'CONTRATO_GARANTINORTE', 'DATA_PREJUIZO_MAIS_ANTIGO', 'DATA_TERCEIRIZACAO',
                      'DATA_PREVISTA_HONRA', 'VALOR_MINIMO', 'ESPE_PARCELAMENTO']:
                field_id = CustomFieldsConfig.get_deal_field_id(key)
                if field_id != 'SUBSTITUIR_PELO_ID_REAL':
                    # CORREÇÃO: Formatar campos personalizados conforme tipo
                    formatted_value = self._format_custom_field_value_for_api_v2(key, value)
                    if formatted_value is not None:
                        # CORREÇÃO: Adicionar log detalhado para debug
                        logger.debug(f"Campo personalizado {key} (ID: {field_id}): valor={value} (tipo: {type(value)}) -> formatado={formatted_value} (tipo: {type(formatted_value)})")
                        custom_fields[field_id] = formatted_value
            else:
                # Campos padrão (não personalizados)
                standard_fields[key] = value
        
        # Montar dados finais baseado na versão da API
        if api_version == 'v2' and custom_fields:
            # API v2: campos personalizados dentro do objeto custom_fields
            update_data = standard_fields.copy()
            update_data['custom_fields'] = custom_fields
        else:
            # API v1: campos personalizados direto no corpo
            update_data = {**standard_fields, **custom_fields}
        
        # Usar PATCH para v2, PUT para v1
        method = 'PATCH' if api_version == 'v2' else 'PUT'
        
        logger.debug(f"Dados estruturados para envio (API {api_version}): {update_data}")
        
        result = self._make_request(method, endpoint, data=update_data)
        
        if result.get('success'):
            logger.info(f"Negócio atualizado com sucesso: ID {deal_id}")
            return True
        else:
            # Tratamento melhorado de erros
            error_msg = result.get('error', 'Erro desconhecido')
            status_code = result.get('status_code', 'N/A')
            
            logger.error(f"Erro ao atualizar negócio {deal_id}: {error_msg}")
            logger.error(f"Status Code: {status_code}")
            logger.error(f"Dados enviados: {update_data}")
            
            # Log específico para erro 400 (Bad Request)
            if status_code == 400:
                logger.error("⚠️  Erro 400: Verifique se todos os campos enviados são válidos para a API v2")
                
                # Log detalhado dos campos personalizados que podem estar causando problema
                if 'custom_fields' in update_data:
                    logger.error("Campos personalizados enviados:")
                    for field_id, field_value in update_data['custom_fields'].items():
                        logger.error(f"  - {field_id}: {field_value} (tipo: {type(field_value)})")
            
            return False
    
    def get_deals_by_person(self, person_id: int) -> List[Dict]:
        """
        Busca todos os negócios de uma pessoa
        
        Args:
            person_id: ID da pessoa
            
        Returns:
            Lista de negócios
        """
        logger.info(f"Buscando negócios da pessoa ID: {person_id}")
        
        # v2 usa parâmetro, v1 usa endpoint específico
        api_version = active_config.get_api_version_for_endpoint('deals')
        
        if api_version == 'v2':
            return self._paginate_requests('deals', {'person_id': person_id})
        else:
            return self._paginate_requests(f'persons/{person_id}/deals', {'status': 'all_not_deleted'})
    
    def get_deals_by_pipeline(self, pipeline_id: int) -> List[Dict]:
        """
        Busca todos os negócios de um funil
        
        Args:
            pipeline_id: ID do funil
            
        Returns:
            Lista de negócios
        """
        logger.info(f"Buscando negócios do funil ID: {pipeline_id}")
        
        return self._paginate_requests('deals', {'pipeline_id': pipeline_id})
    
    def mark_deal_as_lost(self, deal_id: int, reason: str = None) -> bool:
        """
        Marca negócio como perdido
        
        Args:
            deal_id: ID do negócio
            reason: Motivo da perda
        """
        logger.info(f"Marcando negócio {deal_id} como perdido")
        
        data = {'status': 'lost'}
        if reason:
            data['lost_reason'] = reason
        
        return self.update_deal(deal_id, data)
    
    def mark_deal_as_won(self, deal_id: int) -> bool:
        """
        Marca negócio como ganho
        
        Args:
            deal_id: ID do negócio
        """
        logger.info(f"Marcando negócio {deal_id} como ganho")
        
        data = {'status': 'won'}
        
        return self.update_deal(deal_id, data)
    
    def get_pipelines(self) -> List[Dict]:
        """
        Busca todos os funis usando API v2
        
        Returns:
            Lista de funis
        """
        logger.info("Buscando funis")
        
        result = self._make_request('GET', 'pipelines')
        
        if result.get('success'):
            return result.get('data', [])
        return []
    
    def get_pipeline_stages(self, pipeline_id: int) -> List[Dict]:
        """
        Busca etapas de um funil usando API v2
        
        Args:
            pipeline_id: ID do funil
            
        Returns:
            Lista de etapas
        """
        logger.info(f"Buscando etapas do funil ID: {pipeline_id}")
        
        result = self._make_request('GET', 'stages', {'pipeline_id': pipeline_id})
        
        if result.get('success'):
            return result.get('data', [])
        return []
    
    def get_person_by_id(self, person_id: int) -> Optional[Dict]:
        """
        Busca pessoa por ID usando API v2
        
        Args:
            person_id: ID da pessoa
            
        Returns:
            Dados da pessoa
        """
        logger.info(f"Buscando pessoa ID: {person_id}")
        
        result = self._make_request('GET', f'persons/{person_id}')
        
        if result.get('success'):
            return result.get('data')
        return None
    
    def get_organization_by_id(self, org_id: int) -> Optional[Dict]:
        """
        Busca organização por ID usando API v2
        
        Args:
            org_id: ID da organização
            
        Returns:
            Dados da organização
        """
        logger.info(f"Buscando organização ID: {org_id}")
        
        result = self._make_request('GET', f'organizations/{org_id}')
        
        if result.get('success'):
            return result.get('data')
        return None
    
    def get_all_persons(self) -> List[Dict]:
        """
        Busca todas as pessoas usando paginação v2 (cursor-based)
        
        Returns:
            Lista de pessoas
        """
        logger.info("Buscando todas as pessoas")
        
        all_persons = []
        limit = 500
        cursor = ''
        
        url = f"{self.base_url_v2}/persons"
        
        while True:
            params = {
                'limit': limit,
                'api_token': self.api_token,
                'cursor': cursor
            }
            
            try:
                import requests
                response = requests.get(url, params=params)
                response.raise_for_status()
                
                result = response.json()
                
                if not result.get('success', False):
                    logger.error(f"Erro na requisição: {result.get('error', 'Erro desconhecido')}")
                    break
                
                data = result.get('data', [])
                if not data:
                    break
                
                all_persons.extend(data)
                
                # Verificar se há mais páginas usando next_cursor
                additional_data = result.get('additional_data', {})
                next_cursor = additional_data.get('next_cursor')
                
                if next_cursor is None:
                    break
                
                cursor = next_cursor
                
            except Exception as e:
                logger.error(f"Erro na requisição: {e}")
                break
        
        logger.info(f"Total de pessoas encontradas: {len(all_persons)}")
        return all_persons
    
    def get_deal_by_id(self, deal_id: int) -> Optional[Dict]:
        """
        Busca negócio por ID usando API v2
        
        Args:
            deal_id: ID do negócio
            
        Returns:
            Dados do negócio
        """
        logger.info(f"Buscando negócio ID: {deal_id}")
        
        result = self._make_request('GET', f'deals/{deal_id}')
        
        if result.get('success'):
            return result.get('data')
        return None
    
    def search_deals_by_title(self, title: str) -> List[Dict]:
        """
        Busca negócios por título usando API v2
        
        Args:
            title: Título para buscar
            
        Returns:
            Lista de negócios
        """
        logger.info(f"Buscando negócios por título: {title}")
        
        result = self._make_request('GET', 'deals/search', params={
            'term': title,
            'exact_match': False
        })
        
        if result.get('success') and result.get('data'):
            items = result['data'].get('items', [])
            return [item.get('item') for item in items if item.get('item')]
        return []
    
    def update_person(self, person_id: int, data: Dict) -> bool:
        """
        Atualiza pessoa existente usando PATCH (v2) ou PUT (v1)
        
        Args:
            person_id: ID da pessoa
            data: Dados para atualização
        """
        logger.info(f"Atualizando pessoa ID: {person_id}")
        logger.debug(f"Dados recebidos para atualização da pessoa: {data}")
        
        endpoint = f'persons/{person_id}'
        api_version = active_config.get_api_version_for_endpoint(endpoint)
        
        # Separar campos padrão de campos personalizados
        standard_fields = {}
        custom_fields = {}
        
        for key, value in data.items():
            # Pular campos None ou vazios
            if value is None:
                continue
                
            # CORREÇÃO: API v2 requer phones e emails como arrays
            if api_version == 'v2' and key in ['phone', 'email']:
                if key == 'phone':
                    # Converter telefone único para array
                    standard_fields['phones'] = [{'value': value, 'primary': True}]
                elif key == 'email':
                    # Converter email único para array
                    standard_fields['emails'] = [{'value': value, 'primary': True}]
                continue
            # CORREÇÃO: API v2 não permite address como campo padrão
            elif api_version == 'v2' and key == 'address':
                # Mover endereço para campo personalizado ENDERECO
                field_id = CustomFieldsConfig.get_person_field_id('ENDERECO')
                if field_id != 'SUBSTITUIR_PELO_ID_REAL':
                    # Converter string de endereço para objeto
                    address_obj = self._format_address_for_api_v2(value)
                    custom_fields[field_id] = address_obj
                continue
            # Tentar mapear campos personalizados
            elif key in ['CPF', 'DATA_NASCIMENTO', 'IDADE', 'ESTADO_CIVIL', 'CONDICAO_CPF', 'ENDERECO']:
                field_id = CustomFieldsConfig.get_person_field_id(key)
                if field_id != 'SUBSTITUIR_PELO_ID_REAL':
                    # CORREÇÃO: Formatar campos personalizados conforme tipo
                    formatted_value = self._format_custom_field_value_for_api_v2(key, value)
                    if formatted_value is not None:
                        # CORREÇÃO: Adicionar log detalhado para debug
                        logger.debug(f"Campo personalizado {key} (ID: {field_id}): valor={value} (tipo: {type(value)}) -> formatado={formatted_value} (tipo: {type(formatted_value)})")
                        custom_fields[field_id] = formatted_value
            else:
                # Campos padrão (não personalizados)
                standard_fields[key] = value
        
        # Montar dados finais baseado na versão da API
        if api_version == 'v2' and custom_fields:
            # API v2: campos personalizados dentro do objeto custom_fields
            update_data = standard_fields.copy()
            update_data['custom_fields'] = custom_fields
        else:
            # API v1: campos personalizados direto no corpo
            update_data = {**standard_fields, **custom_fields}
        
        # Usar PATCH para v2, PUT para v1
        method = 'PATCH' if api_version == 'v2' else 'PUT'
        
        logger.debug(f"Dados estruturados para envio (API {api_version}): {update_data}")
        
        result = self._make_request(method, endpoint, data=update_data)
        
        if result.get('success'):
            logger.info(f"Pessoa atualizada com sucesso: ID {person_id}")
            return True
        else:
            # Tratamento melhorado de erros
            error_msg = result.get('error', 'Erro desconhecido')
            status_code = result.get('status_code', 'N/A')
            
            logger.error(f"Erro ao atualizar pessoa {person_id}: {error_msg}")
            logger.error(f"Status Code: {status_code}")
            logger.error(f"Dados enviados: {update_data}")
            
            # Log específico para erro 400 (Bad Request)
            if status_code == 400:
                logger.error("⚠️  Erro 400: Verifique se todos os campos enviados são válidos para a API v2")
                # Se o erro for org_id inválido, remover e reenviar
                if 'org_id' in update_data and isinstance(error_msg, str) and 'org_id' in error_msg:
                    logger.warning("Removendo org_id e tentando novamente o PATCH da pessoa.")
                    retry_payload = update_data.copy()
                    retry_payload.pop('org_id', None)
                    retry_result = self._make_request(method, endpoint, data=retry_payload)
                    if retry_result.get('success'):
                        logger.info(f"Pessoa atualizada com sucesso (fallback sem org_id): ID {person_id}")
                        return True
                
                # Log detalhado dos campos personalizados que podem estar causando problema
                if 'custom_fields' in update_data:
                    logger.error("Campos personalizados enviados:")
                    for field_id, field_value in update_data['custom_fields'].items():
                        logger.error(f"  - {field_id}: {field_value} (tipo: {type(field_value)})")
            
            return False
    
    def _format_custom_field_value_for_api_v2(self, field_name: str, value: any) -> any:
        """
        Formata valor do campo personalizado para API v2
        
        Args:
            field_name: Nome do campo
            value: Valor original
            
        Returns:
            Valor formatado conforme tipo do campo para API v2
        """
        # CORREÇÃO: Adicionar log para debug
        logger.debug(f"Formatando campo personalizado: {field_name} = {value} (tipo: {type(value)})")
        
        if value is None or value == '':
            return None
        
        # Campos de data - formato YYYY-MM-DD
        if field_name in ['DATA_NASCIMENTO', 'VENCIMENTO_MAIS_ANTIGO', 'DATA_PREJUIZO_MAIS_ANTIGO', 'DATA_TERCEIRIZACAO', 'DATA_PREVISTA_HONRA']:
            if isinstance(value, str) and value.strip():
                try:
                    # Tentar converter de DD/MM/YYYY para YYYY-MM-DD
                    if '/' in value:
                        day, month, year = value.split('/')
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                    return value
                except:
                    return None
            return None
        
        # Campos monetários - formato {"value": valor, "currency": "BRL"}
        if field_name in ['VALOR_TOTAL_DA_DIVIDA', 'VALOR_TOTAL_VENCIDO', 'VALOR_TOTAL_COM_JUROS', 'VALOR_MINIMO']:
            if isinstance(value, (int, float)) and value > 0:
                return {"value": float(value), "currency": "BRL"}
            return None
        
        # Campos numéricos
        if field_name in ['IDADE', 'DIAS_DE_ATRASO', 'ESPE_PARCELAMENTO']:
            if isinstance(value, str) and value.strip():
                try:
                    return int(value)
                except:
                    return None
            elif isinstance(value, (int, float)):
                return int(value)
            return None
        
        # ID_CPF_CNPJ (negócio) - numérico
        if field_name == 'ID_CPF_CNPJ':
            if isinstance(value, (int, float)):
                return int(value)
            if isinstance(value, str) and value.strip():
                clean = ''.join(filter(str.isdigit, value))
                if clean:
                    try:
                        return int(clean)
                    except:
                        return None
            return None
        
        # ESTADO_CIVIL (pessoa) - opção única (ID numérico)
        if field_name == 'ESTADO_CIVIL':
            if isinstance(value, (int, float)):
                return int(value)
            if isinstance(value, str) and value.strip():
                import unicodedata
                s = unicodedata.normalize('NFD', value.upper())
                s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
                s = s.replace('(', '').replace(')', '').replace(' ', '')
                mapping = {
                    'CASADO': 102,
                    'CASADOA': 102,
                    'SOLTEIRO': 103,
                    'SOLTEIROA': 103,
                    'VIUVO': 104,
                    'VIUVOA': 104,
                    'DIVORCIADO': 123,
                    'DIVORCIADOA': 123,
                    'SEPARADO': 124,
                    'SEPARADOA': 124,
                }
                if s in mapping:
                    logger.info(f"ESTADO CIVIL: '{value}' mapeado para ID {mapping[s]}")
                    return mapping[s]
                logger.warning(f"ESTADO CIVIL desconhecido: '{value}' - usando SOLTEIRO como padrão")
                return 103
            return None
        
        # CONDICAO_CPF (pessoa e negócio) - texto curto (string)
        if field_name == 'CONDICAO_CPF':
            if value is None:
                return None
            # Normalizar e limitar a 255
            text = str(value).strip()
            if not text:
                return None
            # Padronizar 'LIMPO' como 'NORMAL' se necessário
            if text.upper() == 'LIMPO':
                text = 'NORMAL'
            return text[:255]
        
        # Campo de múltipla escolha - TAG_ATRASO (negócio)
        if field_name == 'TAG_ATRASO':
            if isinstance(value, list):
                return value
            if isinstance(value, (int, float)):
                return [122] if int(value) > 90 else [121]
            if isinstance(value, str) and value.strip():
                u = value.upper()
                if any(tok in u for tok in ['> 90', '>90', 'PREJU', 'ACIMA']):
                    return [122]
                if any(tok in u for tok in ['<90', 'INAD', 'EVITAR']):
                    return [121]
                if u.isdigit():
                    return [122] if int(u) > 90 else [121]
                return [121]
            return None
        
        # ENDERECO (pessoa) - objeto
        if field_name == 'ENDERECO':
            logger.debug(f"Processando campo ENDERECO: {value} (tipo: {type(value)})")
            if isinstance(value, dict):
                # Se já é um dicionário, verificar se está no formato correto da API v2
                if 'route' in value and 'value' in value:
                    # Já está no formato correto da API v2
                    logger.debug(f"ENDERECO já está no formato correto da API v2: {value}")
                    return value
                else:
                    # Converter do formato antigo para o novo
                    address_string = value.get('address', '')
                    if address_string:
                        formatted_address = self._format_address_for_api_v2(address_string)
                        logger.debug(f"ENDERECO convertido do formato antigo: {formatted_address}")
                        return formatted_address
                    return None
            if isinstance(value, str) and value.strip():
                formatted_address = self._format_address_for_api_v2(value)
                logger.debug(f"ENDERECO convertido de string: {formatted_address}")
                return formatted_address
            return None
        
        # Padrão: strings curtas limitadas e outros valores como vieram
        if isinstance(value, str):
            v = value.strip()
            return v[:255] if v else None
        return value
    
    def update_organization(self, org_id: int, data: Dict) -> bool:
        """
        Atualiza organização existente usando PATCH (v2) ou PUT (v1)
        
        Args:
            org_id: ID da organização
            data: Dados para atualização
        """
        logger.info(f"Atualizando organização ID: {org_id}")
        logger.debug(f"Dados recebidos para atualização da organização: {data}")
        
        endpoint = f'organizations/{org_id}'
        api_version = active_config.get_api_version_for_endpoint(endpoint)
        
        # Separar campos padrão de campos personalizados
        standard_fields = {}
        custom_fields = {}
        
        for key, value in data.items():
            # Pular campos None ou vazios
            if value is None:
                continue
                
            # Tentar mapear campos personalizados
            if key in ['CPF_CNPJ', 'TELEFONE', 'ENDERECO', 'NOME_FANTASIA', 'CNAE', 
                      'QUANTIDADE_FUNCIONARIOS', 'SOCIOS', 'DATA_ABERTURA', 
                      'EMAILS', 'TELEFONE_HOT', 'NOME_EMPRESA']:
                field_id = CustomFieldsConfig.get_organization_field_id(key)
                if field_id != 'SUBSTITUIR_PELO_ID_REAL':
                    custom_fields[field_id] = value
            else:
                # Campos padrão (não personalizados)
                standard_fields[key] = value
        
        # Montar dados finais baseado na versão da API
        if api_version == 'v2' and custom_fields:
            # API v2: campos personalizados dentro do objeto custom_fields
            update_data = standard_fields.copy()
            update_data['custom_fields'] = custom_fields
        else:
            # API v1: campos personalizados direto no corpo
            update_data = {**standard_fields, **custom_fields}
        
        # Usar PATCH para v2, PUT para v1
        method = 'PATCH' if api_version == 'v2' else 'PUT'
        
        logger.debug(f"Dados estruturados para envio (API {api_version}): {update_data}")
        
        result = self._make_request(method, endpoint, data=update_data)
        
        if result.get('success'):
            logger.info(f"Organização {org_id} atualizada com sucesso (API {api_version})")
            return True
        else:
            logger.error(f"Erro ao atualizar organização {org_id}: {result.get('error', 'Erro desconhecido')}")
            return False
    
    def delete_deal(self, deal_id: int) -> bool:
        """
        Deleta negócio
        
        Args:
            deal_id: ID do negócio
        """
        logger.info(f"Deletando negócio ID: {deal_id}")
        
        result = self._make_request('DELETE', f'deals/{deal_id}')
        
        if result.get('success'):
            logger.info(f"Negócio {deal_id} deletado com sucesso")
            return True
        else:
            logger.error(f"Erro ao deletar negócio {deal_id}: {result.get('error', 'Erro desconhecido')}")
            return False
    
    def delete_person(self, person_id: int) -> bool:
        """
        Deleta pessoa
        
        Args:
            person_id: ID da pessoa
        """
        logger.info(f"Deletando pessoa ID: {person_id}")
        
        result = self._make_request('DELETE', f'persons/{person_id}')
        
        if result.get('success'):
            logger.info(f"Pessoa {person_id} deletada com sucesso")
            return True
        else:
            logger.error(f"Erro ao deletar pessoa {person_id}: {result.get('error', 'Erro desconhecido')}")
            return False
    
    def get_user_info(self) -> Optional[Dict]:
        """
        Busca informações do usuário atual usando API v1 (users/me só existe em v1)
        
        Returns:
            Dados do usuário
        """
        logger.info("Buscando informações do usuário")
        
        result = self._make_request('GET', 'users/me')
        
        if result.get('success'):
            return result.get('data')
        return None 

    def _clean_document(self, document: str) -> str:
        """Remove formatação do documento, mantendo apenas números"""
        import re
        return re.sub(r'[^0-9]', '', str(document))
    
    def _normalize_document(self, document: str) -> List[str]:
        """
        Normaliza documento gerando variações válidas
        
        Args:
            document: Documento original (pode ter tamanho incorreto)
            
        Returns:
            Lista de variações do documento para tentar na busca
        """
        # Limpar documento (só números)
        clean_doc = self._clean_document(document)
        
        if not clean_doc:
            return []
        
        variants = []
        
        # 1. Documento original limpo
        variants.append(clean_doc)
        
        # 2. Documento sem zeros à esquerda
        stripped = clean_doc.lstrip('0')
        if stripped and stripped != clean_doc:
            variants.append(stripped)
        
        # 3. Se documento tem mais que 14 dígitos, tentar extrair CPF/CNPJ válidos
        if len(clean_doc) > 14:
            # Tentar últimos 11 dígitos (CPF)
            if len(clean_doc) >= 11:
                cpf_candidate = clean_doc[-11:]
                variants.append(cpf_candidate)
            
            # Tentar últimos 14 dígitos (CNPJ)
            if len(clean_doc) >= 14:
                cnpj_candidate = clean_doc[-14:]
                variants.append(cnpj_candidate)
        
        # 4. Se documento tem entre 11-14 dígitos, pode ser CPF ou CNPJ
        elif 11 <= len(clean_doc) <= 14:
            # Se tem 11 dígitos, é CPF
            if len(clean_doc) == 11:
                variants.append(clean_doc)
            # Se tem 14 dígitos, é CNPJ
            elif len(clean_doc) == 14:
                variants.append(clean_doc)
            # Se tem 12-13 dígitos, pode ser CPF com zeros extras
            else:
                # Tentar como CPF (últimos 11 dígitos)
                if len(clean_doc) >= 11:
                    cpf_candidate = clean_doc[-11:]
                    variants.append(cpf_candidate)
        
        # 5. Se documento muito curto, tentar preencher com zeros à esquerda até CPF
        elif len(clean_doc) < 11:
            # Preencher até 11 dígitos (CPF)
            cpf_padded = clean_doc.zfill(11)
            variants.append(cpf_padded)
        
        # Remover duplicatas mantendo ordem
        unique_variants = []
        for variant in variants:
            if variant and variant not in unique_variants:
                unique_variants.append(variant)
        
        return unique_variants 

    def _normalize_document_by_type(self, document: str, person_type: str) -> List[str]:
        """
        Normaliza documento baseado no tipo de pessoa (PF ou PJ)
        
        Args:
            document: Documento do TXT (sempre 15 dígitos com zeros à esquerda)
            person_type: 'PF' ou 'PJ'
            
        Returns:
            Lista de variações do documento para tentar na busca
        """
        # Limpar documento (só números)
        clean_doc = self._clean_document(document)
        
        if not clean_doc:
            return []
        
        variants = []
        
        # Determinar tamanho correto baseado no tipo
        if person_type == 'PF':
            # CPF deve ter 11 dígitos
            target_length = 11
        elif person_type == 'PJ':
            # CNPJ deve ter 14 dígitos
            target_length = 14
        else:
            # Tipo indefinido, tentar ambos
            logger.warning(f"Tipo de pessoa indefinido: {person_type}. Tentando PF e PJ.")
            variants.extend(self._normalize_document_by_type(document, 'PF'))
            variants.extend(self._normalize_document_by_type(document, 'PJ'))
            # Remover duplicatas
            return list(dict.fromkeys(variants))
        
        # Extrair documento com tamanho correto
        if len(clean_doc) >= target_length:
            # Pegar os últimos N dígitos (tamanho correto)
            correct_doc = clean_doc[-target_length:]
            variants.append(correct_doc)
            
            # Também tentar sem zeros à esquerda
            stripped_doc = correct_doc.lstrip('0')
            if stripped_doc and stripped_doc != correct_doc:
                variants.append(stripped_doc)
        else:
            # Documento muito curto, preencher com zeros à esquerda
            padded_doc = clean_doc.zfill(target_length)
            variants.append(padded_doc)
            
            # Também tentar sem zeros à esquerda
            stripped_doc = clean_doc.lstrip('0')
            if stripped_doc:
                variants.append(stripped_doc)
        
        # Remover duplicatas mantendo ordem
        unique_variants = []
        for variant in variants:
            if variant and variant not in unique_variants:
                unique_variants.append(variant)
        
        return unique_variants 

    def search_person_by_document(self, document: str, person_type: str) -> Optional[Dict]:
        """
        Busca pessoa por documento considerando o tipo de pessoa (PF ou PJ)
        
        Args:
            document: Documento do TXT (15 dígitos com zeros à esquerda)
            person_type: 'PF' ou 'PJ'
        """
        logger.info(f"Buscando {person_type} por documento: {document}")
        
        # Normalizar documento baseado no tipo
        variants = self._normalize_document_by_type(document, person_type)
        logger.debug(f"Variantes geradas para {person_type}: {variants}")
        
        # MUDANÇA: Todos os tipos (PF e PJ) são tratados como pessoas
        return self._search_person_with_variants(variants)
    
    def _search_person_with_variants(self, cpf_variants: List[str]) -> Optional[Dict]:
        """Busca pessoa usando lista de variantes de CPF"""
        cpf_field_id = CustomFieldsConfig.get_person_field_id('CPF')
        if not cpf_field_id:
            logger.error("ID do campo CPF não encontrado na configuração")
            return None
        
        # Primeira tentativa: buscar usando o campo CPF configurado
        for i, cpf_variant in enumerate(cpf_variants):
            if not cpf_variant or len(cpf_variant.strip()) < 2:
                continue
            
            logger.info(f"Tentando busca PF com variante: '{cpf_variant}'")
            
            result = self._make_request('GET', 'persons/search', params={
                'term': cpf_variant.strip(),
                'field': cpf_field_id,
                'exact_match': False
            })
            
            # Se deu erro de campo não permitido, tentar fallback
            if result and not result.get('success'):
                error_msg = result.get('error', '').lower()
                if 'not allowed' in error_msg or 'validation failed' in error_msg:
                    logger.warning(f"Campo CPF não é pesquisável. Tentando busca alternativa...")
                    return self._search_person_by_cpf_fallback(cpf_variants)
            
            # Se sucesso e tem dados
            if result and result.get('success') and result.get('data'):
                items = result['data'].get('items', [])
                if items:
                    logger.info(f"Pessoa encontrada com CPF: {cpf_variant}")
                    return items[0].get('item')
        
        # Se chegou aqui, tentar busca alternativa
        logger.info("Tentando busca alternativa por CPF...")
        return self._search_person_by_cpf_fallback(cpf_variants)
    
    def _search_organization_with_variants(self, cnpj_variants: List[str]) -> Optional[Dict]:
        """Busca organização usando lista de variantes de CNPJ"""
        cnpj_field_id = CustomFieldsConfig.get_organization_field_id('CPF_CNPJ')
        if not cnpj_field_id:
            logger.error("ID do campo CPF_CNPJ não encontrado na configuração")
            return None
        
        for i, cnpj_variant in enumerate(cnpj_variants):
            if not cnpj_variant or len(cnpj_variant.strip()) < 2:
                continue
            
            logger.info(f"Tentando busca PJ com variante: '{cnpj_variant}'")
            
            result = self._make_request('GET', 'organizations/search', params={
                'term': cnpj_variant.strip(),
                'field': cnpj_field_id,
                'exact_match': False
            })
            
            # Se deu erro de campo não permitido, tentar fallback
            if result and not result.get('success'):
                error_msg = result.get('error', '').lower()
                if 'not allowed' in error_msg or 'validation failed' in error_msg:
                    logger.warning(f"Campo CNPJ não é pesquisável. Tentando busca alternativa...")
                    return self._search_organization_fallback(cnpj_variants)
            
            if result and result.get('success') and result.get('data'):
                items = result['data'].get('items', [])
                if items:
                    logger.info(f"Organização encontrada com CNPJ: {cnpj_variant}")
                    return items[0].get('item')
        
        # Se chegou aqui, tentar busca alternativa
        return self._search_organization_fallback(cnpj_variants)
    
    def _search_organization_fallback(self, cnpj_variants: List[str]) -> Optional[Dict]:
        """Busca alternativa para organizações quando campo CNPJ não é pesquisável"""
        logger.info("Executando busca alternativa por CNPJ")
        
        for cnpj_variant in cnpj_variants:
            if not cnpj_variant or len(cnpj_variant.strip()) < 2:
                continue
                
            # Tentar busca geral (sem especificar campo)
            logger.debug(f"Tentando busca geral de organização com: {cnpj_variant}")
            
            result = self._make_request('GET', 'organizations/search', params={
                'term': cnpj_variant.strip(),
                'exact_match': False
            })
            
            if result and result.get('success') and result.get('data'):
                items = result['data'].get('items', [])
                for item_data in items:
                    item = item_data.get('item', {})
                    # Verificar se o CNPJ realmente confere
                    if self._verify_cnpj_match(item, cnpj_variant):
                        logger.info(f"Organização encontrada via busca alternativa: {cnpj_variant}")
                        return item
        
        logger.info("Organização não encontrada nem com busca alternativa")
        return None
    
    def _verify_cnpj_match(self, organization: Dict, target_cnpj: str) -> bool:
        """Verifica se uma organização tem o CNPJ desejado"""
        custom_fields = organization.get('custom_fields', {})
        cnpj_field_id = CustomFieldsConfig.get_organization_field_id('CPF_CNPJ')
        
        if cnpj_field_id in custom_fields:
            org_cnpj = str(custom_fields[cnpj_field_id]).strip()
            org_cnpj_clean = self._clean_document(org_cnpj)
            target_cnpj_clean = self._clean_document(target_cnpj)
            
            return org_cnpj_clean == target_cnpj_clean
        
        return False 

    def _remove_leading_zeros(self, value: str) -> str:
        """
        Remove zeros à esquerda de campos numéricos específicos
        
        Args:
            value: Valor do campo (string)
            
        Returns:
            Valor sem zeros à esquerda
        """
        if not isinstance(value, str):
            return value
        
        # Remover espaços em branco
        value = value.strip()
        
        # Se o valor for apenas zeros, retornar "0"
        if value == "0" * len(value):
            return "0"
        
        # Remover zeros à esquerda
        return value.lstrip('0') or "0"  # Se ficar vazio, retornar "0"

    def _limit_short_text_fields(self, value: str, max_length: int = 255) -> str:
        """
        Limita campos short text ao tamanho máximo permitido
        
        Args:
            value: Valor do campo
            max_length: Tamanho máximo (padrão 255 para short text)
            
        Returns:
            Valor truncado se necessário
        """
        if not isinstance(value, str):
            return value
        
        if len(value) > max_length:
            logger.warning(f"Campo truncado de {len(value)} para {max_length} caracteres: {value[:50]}...")
            return value[:max_length]
        
        return value

    def _format_address_for_api_v2(self, address_string: str) -> Dict:
        """
        Converte string de endereço para o formato de objeto esperado pela API v2
        
        Args:
            address_string: String do endereço (ex: "Rua das Flores, 123, Centro, São Paulo, SP, 01234-567")
            
        Returns:
            Objeto de endereço no formato da API v2
        """
        if not address_string or not isinstance(address_string, str):
            return {
                "route": "",
                "value": "",
                "country": "Brasil",
                "locality": "",
                "postal_code": "",
                "sublocality": "",
                "street_number": "",
                "admin_area_level_1": "",
                "admin_area_level_2": "",
                "formatted_address": ""
            }
        
        # Limpar e dividir o endereço
        parts = [part.strip() for part in address_string.split(',') if part.strip()]
        
        # Extrair número da rua se presente
        street_number = ""
        route = ""
        if parts:
            first_part = parts[0]
            # Tentar extrair número do endereço (ex: "Rua das Flores 123")
            import re
            number_match = re.search(r'(\d+)(?:\s*$|\s+)', first_part)
            if number_match:
                street_number = number_match.group(1)
                route = first_part.replace(street_number, '').strip()
            else:
                route = first_part
        
        # Construir objeto no formato correto da API v2
        address_obj = {
            "route": route,
            "value": address_string,
            "country": "Brasil",
            "locality": parts[2] if len(parts) > 2 else "",  # Cidade (Município)
            "postal_code": parts[4] if len(parts) > 4 else "",  # CEP
            "sublocality": parts[1] if len(parts) > 1 else "",  # Bairro
            "street_number": street_number,
            "admin_area_level_1": parts[3] if len(parts) > 3 else "",  # Estado (UF)
            "admin_area_level_2": parts[2] if len(parts) > 2 else "",  # Município (mesmo que locality)
            "formatted_address": address_string
        }
        
        return address_obj 