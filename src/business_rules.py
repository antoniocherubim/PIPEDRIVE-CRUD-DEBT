"""
Regras de negócio refinadas para processamento de inadimplentes no Pipedrive
Implementa lógica específica para os 3 funis: BASE NOVA - SDR, BASE NOVA - NEGOCIAÇÃO, BASE NOVA - FORMALIZAÇÃO/PAGAMENTO
Processa TXT diretamente com mapeamento completo de campos e backup SQLite automático
"""
import logging
import os
import sys
from typing import Dict, List, Set, Tuple, Optional

# Adicionar o diretório utils ao path para importar backup_sqlite
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
utils_path = os.path.join(project_root, 'utils')
sys.path.insert(0, utils_path)

from pipedrive_client import PipedriveClient
from file_processor import FileProcessor
from config import active_config
from custom_fields_config import CustomFieldsConfig
from utils.backup_sqlite import BackupSQLite

logger = logging.getLogger(__name__)

class BusinessRulesProcessor:
    def __init__(self, pipedrive_client: PipedriveClient = None, 
                 file_processor: FileProcessor = None,
                 db_name: str = None):
        self.pipedrive = pipedrive_client or PipedriveClient()
        self.file_processor = file_processor or FileProcessor()
        
        # Inicializar backup SQLite
        self.backup_sqlite = BackupSQLite(db_name)
        
        # ID do processamento atual
        self.current_processing_id = None
        
        # Estatísticas de processamento
        self.processing_stats = {
            'pessoas_criadas': [],
            'negocios_criados': [],
            'negocios_atualizados': [],
            'negocios_movidos_para_sdr': [],
            'negocios_marcados_perdidos': [],
            'negocios_mantidos_formalização': [],
            'erros': [],
            'backup_sqlite_salvos': 0,
            'backup_sqlite_atualizados': 0
        }
    
    def process_inadimplentes_from_txt(self, txt_path: str) -> Dict:
        """
        Processa arquivo TXT diretamente e aplica regras de negócio refinadas
        Com backup SQLite automático
        """
        logger.info(f"Iniciando processamento de inadimplentes do TXT com backup SQLite: {txt_path}")
        
        # Resetar estatísticas
        self.processing_stats = {
            'pessoas_criadas': [],
            'negocios_criados': [],
            'negocios_atualizados': [],
            'negocios_movidos_para_sdr': [],
            'negocios_marcados_perdidos': [],
            'negocios_mantidos_formalização': [],
            'erros': [],
            'backup_sqlite_salvos': 0,
            'backup_sqlite_atualizados': 0
        }
        
        try:
            # 1. Processar TXT diretamente
            inadimplentes_data = self.file_processor.process_txt_file_direct(txt_path)
            if not inadimplentes_data:
                logger.error("Nenhum dado encontrado no arquivo TXT")
                return self.processing_stats
            
            logger.info(f"Total de inadimplentes no arquivo: {len(inadimplentes_data)}")
            
            # 2. Iniciar log de processamento no SQLite
            arquivo_nome = os.path.basename(txt_path)
            self.current_processing_id = self.backup_sqlite.iniciar_processamento(
                arquivo_nome, len(inadimplentes_data)
            )
            
            # 3. Extrair documentos atuais do TXT
            documentos_txt = set()
            for item in inadimplentes_data:
                cpf_cnpj = item.get('cpf_cnpj', '')
                tipo_pessoa = item.get('tipo_pessoa', '')
                if cpf_cnpj and tipo_pessoa != 'INDEFINIDO':
                    documentos_txt.add((tipo_pessoa.lower(), cpf_cnpj))
            
            logger.info(f"Documentos válidos no TXT: {len(documentos_txt)}")
            
            # 4. Processar cada inadimplente com backup
            for inadimplente in inadimplentes_data:
                try:
                    self._process_single_inadimplente_with_backup(inadimplente, arquivo_nome)
                except Exception as e:
                    doc_id = inadimplente.get('cpf_cnpj', 'N/A')
                    error_msg = f"Erro ao processar inadimplente {doc_id}: {e}"
                    logger.error(error_msg)
                    self.processing_stats['erros'].append(error_msg)
            
            # 5. Processar documentos que não estão mais no TXT
            self._process_removed_documents_from_txt(documentos_txt)
            
            # 6. Finalizar processamento no SQLite
            self.backup_sqlite.finalizar_processamento(
                self.current_processing_id,
                len(self.processing_stats['pessoas_criadas']),
                self.processing_stats['backup_sqlite_atualizados'],
                0,  # removidos (será implementado se necessário)
                'concluido'
            )
            
            # 7. Log estatísticas finais
            self._log_final_stats()
            
            return self.processing_stats
            
        except Exception as e:
            error_msg = f"Erro no processamento geral: {e}"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)
            
            # Finalizar processamento com erro
            if self.current_processing_id:
                self.backup_sqlite.finalizar_processamento(
                    self.current_processing_id, 0, 0, 0, 'erro', error_msg
                )
            
            return self.processing_stats
    
    def _process_single_inadimplente_with_backup(self, inadimplente: Dict, arquivo_origem: str):
        """
        Processa um único inadimplente e salva no backup SQLite
        """
        cpf_cnpj = inadimplente.get('cpf_cnpj', '')
        nome = inadimplente.get('nome', '')
        tipo_pessoa = inadimplente.get('tipo_pessoa', '')
        
        if not cpf_cnpj or tipo_pessoa == 'INDEFINIDO':
            logger.warning(f"Documento inválido ignorado: {cpf_cnpj}")
            return
        
        logger.info(f"Processando {tipo_pessoa}: {nome} - Doc: {cpf_cnpj}")
        
        # Buscar pessoa existente pelo documento
        pessoa_existente = self._search_person_by_document(cpf_cnpj, tipo_pessoa)
        
        pipedrive_person_id = None
        pipedrive_org_id = None
        pipedrive_deal_id = None
        status_operacao = 'criado'
        pipeline_atual = None
        stage_atual = None
        
        if pessoa_existente:
            # Documento EXISTE: aplicar regras de atualização
            entity_id = pessoa_existente['id']
            
            if tipo_pessoa == 'PF':
                pipedrive_person_id = entity_id
            else:
                pipedrive_org_id = entity_id
            
            # Buscar negócios existentes
            negocios = self._get_deals_for_entity(entity_id, cpf_cnpj, tipo_pessoa)
            
            if negocios:
                # Pegar o primeiro negócio como referência
                deal = negocios[0]
                pipedrive_deal_id = deal.get('id')
                
                # Determinar pipeline e stage atual
                pipeline_id = deal.get('pipeline_id')
                stage_id = deal.get('stage_id')
                
                pipeline_atual = self._get_pipeline_name(pipeline_id)
                stage_atual = self._get_stage_name(stage_id)
            
            # Aplicar regras de atualização
            self._handle_existing_person_from_txt(pessoa_existente, inadimplente)
            status_operacao = 'atualizado'
            self.processing_stats['backup_sqlite_atualizados'] += 1
            
        else:
            # Documento NOVO: criar pessoa e negócio
            resultado_criacao = self._handle_new_person_from_txt_with_backup(inadimplente)
            
            if resultado_criacao:
                if tipo_pessoa == 'PF':
                    pipedrive_person_id = resultado_criacao.get('entity_id')
                else:
                    pipedrive_org_id = resultado_criacao.get('entity_id')
                
                pipedrive_deal_id = resultado_criacao.get('deal_id')
                pipeline_atual = 'BASE NOVA - SDR'
                stage_atual = 'NOVAS COBRANÇAS'
                
                status_operacao = 'criado'
                self.processing_stats['backup_sqlite_salvos'] += 1
        
        # Salvar no backup SQLite
        sucesso_backup = self.backup_sqlite.salvar_entidade_devedor(
            inadimplente=inadimplente,
            pipedrive_person_id=pipedrive_person_id,
            pipedrive_org_id=pipedrive_org_id,
            pipedrive_deal_id=pipedrive_deal_id,
            status_operacao=status_operacao,
            pipeline_atual=pipeline_atual,
            stage_atual=stage_atual,
            origem_arquivo=arquivo_origem
        )
        
        if not sucesso_backup:
            error_msg = f"Falha ao salvar backup SQLite para {nome} ({cpf_cnpj})"
            logger.warning(error_msg)
            self.processing_stats['erros'].append(error_msg)
    
    def _search_person_by_document(self, documento: str, tipo_pessoa: str) -> Dict:
        """
        Busca pessoa por CPF ou organização por CNPJ dependendo do tipo
        Usa a nova lógica que considera o tipo de pessoa para normalização
        """
        return self.pipedrive.search_person_by_document(documento, tipo_pessoa)
    
    def _handle_new_person_from_txt_with_backup(self, inadimplente: Dict) -> Dict:
        """
        Cria nova pessoa/organização e negócio para documento que não existe no Pipedrive
        Caso criar: se estiver no txt e não tiver registro nenhum no pipe deve ser criado pessoa/organização
        e vinculado negócio em BASE NOVA SRD etapa NOVAS COBRANÇAS
        """
        cpf_cnpj_original = inadimplente.get('cpf_cnpj', '')
        nome = inadimplente.get('nome', '')
        tipo_pessoa = inadimplente.get('tipo_pessoa', '')
        
        # Normalizar documento baseado no tipo de pessoa
        cpf_cnpj_normalizado = self._get_normalized_document(cpf_cnpj_original, tipo_pessoa)
        
        logger.info(f"Criando nova entidade {tipo_pessoa}: {nome} - Doc: {cpf_cnpj_normalizado}")
        
        if tipo_pessoa == 'PF':
            # Criar pessoa física com documento normalizado
            person_data = self.file_processor._build_person_data_from_txt(inadimplente)
            # Atualizar dados com documento normalizado
            person_data = self._update_person_data_with_normalized_document(person_data, cpf_cnpj_normalizado)
            nova_entidade = self.pipedrive.create_person(nome, cpf_cnpj_normalizado, person_data, tipo_pessoa)
            entity_type = 'pessoa'
            entity_id_field = 'person_id'
            
        elif tipo_pessoa == 'PJ':
            # MUDANÇA: Criar PJ como pessoa ao invés de organização
            person_data = self.file_processor._build_person_data_from_txt(inadimplente)
            # Atualizar dados com documento normalizado
            person_data = self._update_person_data_with_normalized_document(person_data, cpf_cnpj_normalizado)
            nova_entidade = self.pipedrive.create_person(nome, cpf_cnpj_normalizado, person_data, tipo_pessoa)
            entity_type = 'pessoa'
            entity_id_field = 'person_id'
            
        else:
            logger.error(f"Tipo de pessoa inválido: {tipo_pessoa}")
            return
        
        if nova_entidade:
            entity_id = nova_entidade['id']
            
            self.processing_stats['pessoas_criadas'].append({
                'id': entity_id,
                'nome': nome,
                'documento': cpf_cnpj_normalizado,  # Usar documento normalizado
                'tipo': tipo_pessoa,
                'entity_type': entity_type
            })
            
            # Criar negócio vinculado à entidade com documento normalizado
            titulo_negocio = f"{cpf_cnpj_normalizado} - {nome}"
            deal_data = self._build_deal_data_from_txt(inadimplente, entity_id)
            
            # Atualizar dados com documento normalizado
            deal_data = self._update_deal_data_with_normalized_document(deal_data, cpf_cnpj_normalizado)
            
            # Preparar parâmetros para criação do negócio
            deal_params = {
                'title': titulo_negocio,
                'pipeline_id': active_config.PIPELINE_BASE_NOVA_SDR_ID,
                'stage_id': active_config.STAGE_NOVAS_COBRANÇAS_ID
            }
            
            # Adicionar person_id ou org_id dependendo do tipo
            if entity_id_field == 'person_id':
                deal_params['person_id'] = entity_id
            else:
                deal_params['org_id'] = entity_id
            
            novo_negocio = self.pipedrive.create_deal(**deal_params)
            
            if novo_negocio:
                # Atualizar negócio com campos personalizados
                success = self.pipedrive.update_deal(novo_negocio['id'], deal_data)
                
                self.processing_stats['negocios_criados'].append({
                    'id': novo_negocio['id'],
                    'titulo': titulo_negocio,
                    'entity_id': entity_id,
                    'entity_type': entity_type,
                    'pipeline': 'BASE NOVA - SDR',
                    'stage': 'NOVAS COBRANÇAS'
                })
                logger.info(f"Negócio criado com sucesso: {titulo_negocio}")
                
                # MUDANÇA: PJs agora são pessoas, não organizações
                # Se for PJ, verificar se há pessoa física relacionada nos avalistas
                # CORREÇÃO: Desabilitar temporariamente o processamento de avalistas para evitar busca incorreta
                # if tipo_pessoa == 'PJ':
                #     self._process_related_persons_for_person(inadimplente, entity_id)
            else:
                error_msg = f"Falha ao criar negócio para {nome}"
                logger.error(error_msg)
                self.processing_stats['erros'].append(error_msg)
        else:
            error_msg = f"Falha ao criar {entity_type} {nome}"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)
            return None
        
        return {
            'entity_id': entity_id,
            'deal_id': novo_negocio['id'] if novo_negocio else None,
            'entity_type': entity_type
        }
    
    def _process_related_persons_for_organization(self, inadimplente: Dict, org_id: int):
        """
        Processa pessoas físicas relacionadas à organização (avalistas/sócios)
        Cria pessoas físicas se não existirem e vincula à organização
        """
        avalistas_info = inadimplente.get('avalistas_info', [])
        
        for avalista in avalistas_info:
            if avalista.get('tipo_pessoa') == 'PF':
                cpf_avalista = avalista.get('cpf_cnpj', '')
                nome_avalista = avalista.get('nome', '')
                
                if cpf_avalista and nome_avalista:
                    # CORREÇÃO: Normalizar CPF do avalista
                    cpf_avalista_normalizado = self._get_normalized_document(cpf_avalista, 'PF')
                    
                    # Verificar se pessoa física já existe usando documento normalizado
                    pessoa_existente = self.pipedrive.search_person_by_document(cpf_avalista_normalizado, 'PF')
                    
                    if not pessoa_existente:
                        # Verificar se a organização existe antes de tentar vincular
                        organizacao_existe = self.pipedrive.get_organization_by_id(org_id)
                        if not organizacao_existe:
                            logger.warning(f"Organização {org_id} não encontrada, pulando criação de pessoa vinculada: {nome_avalista}")
                            continue
                        
                        # Criar pessoa física relacionada
                        logger.info(f"Criando pessoa física relacionada à organização {org_id}: {nome_avalista} - CPF: {cpf_avalista_normalizado}")
                        
                        # Preparar dados básicos da pessoa
                        person_data = {
                            'org_id': org_id,  # Vincular à organização
                            'notes': f"Avalista/Sócio da organização (ID: {org_id})\nResponsabilidade: {avalista.get('responsabilidade', 'N/A')}"
                        }
                        
                        # Adicionar owner_id se disponível
                        if hasattr(active_config, 'PIPEDRIVE_OWNER_ID') and active_config.PIPEDRIVE_OWNER_ID:
                            person_data['owner_id'] = active_config.PIPEDRIVE_OWNER_ID
                        
                        # Adicionar CPF normalizado usando configuração
                        cpf_field_id = CustomFieldsConfig.get_person_field_id('CPF')
                        if cpf_field_id != 'SUBSTITUIR_PELO_ID_REAL':
                            person_data[cpf_field_id] = cpf_avalista_normalizado
                        
                        nova_pessoa = self.pipedrive.create_person(
                            nome_avalista, 
                            cpf_avalista_normalizado, 
                            person_data, 
                            'PF'
                        )
                        
                        if nova_pessoa:
                            logger.info(f"Pessoa física criada e vinculada à organização: {nome_avalista}")
                            self.processing_stats['pessoas_criadas'].append({
                                'id': nova_pessoa['id'],
                                'nome': nome_avalista,
                                'documento': cpf_avalista_normalizado,  # Usar documento normalizado
                                'tipo': 'PF',
                                'entity_type': 'pessoa_vinculada_organizacao',
                                'org_id': org_id
                            })
                        else:
                            logger.warning(f"Falha ao criar pessoa física relacionada: {nome_avalista}")
                    else:
                        # Pessoa existe, atualizar vinculação com organização se necessário
                        logger.info(f"Pessoa física já existe, verificando vinculação: {nome_avalista}")
                        
                        # Verificar se a organização existe antes de tentar vincular
                        organizacao_existe = self.pipedrive.get_organization_by_id(org_id)
                        if not organizacao_existe:
                            logger.warning(f"Organização {org_id} não encontrada, pulando vinculação para: {nome_avalista}")
                            continue
                        
                        # Verificar se já está vinculada à organização
                        pessoa_org_id = pessoa_existente.get('org_id')
                        if pessoa_org_id != org_id:
                            # Atualizar vinculação
                            update_data = {'org_id': org_id}
                            success = self.pipedrive.update_person(pessoa_existente['id'], update_data)
                            
                            if success:
                                logger.info(f"Vinculação atualizada para organização {org_id}: {nome_avalista}")
                            else:
                                logger.warning(f"Falha ao atualizar vinculação: {nome_avalista}")

    def _process_related_persons_for_person(self, inadimplente: Dict, person_id: int):
        """
        Processa pessoas físicas relacionadas à pessoa jurídica (avalistas/sócios)
        Cria pessoas físicas se não existirem e vincula à pessoa jurídica
        """
        avalistas_info = inadimplente.get('avalistas_info', [])
        
        for avalista in avalistas_info:
            if avalista.get('tipo_pessoa') == 'PF':
                cpf_avalista = avalista.get('cpf_cnpj', '')
                nome_avalista = avalista.get('nome', '')
                
                if cpf_avalista and nome_avalista:
                    # CORREÇÃO: Normalizar CPF do avalista
                    cpf_avalista_normalizado = self._get_normalized_document(cpf_avalista, 'PF')
                    
                    # Verificar se pessoa física já existe usando documento normalizado
                    pessoa_existente = self.pipedrive.search_person_by_document(cpf_avalista_normalizado, 'PF')
                    
                    if not pessoa_existente:
                        # Criar pessoa física relacionada
                        logger.info(f"Criando pessoa física relacionada à pessoa jurídica {person_id}: {nome_avalista} - CPF: {cpf_avalista_normalizado}")
                        
                        # Preparar dados básicos da pessoa
                        person_data = {
                            'notes': f"Avalista/Sócio da pessoa jurídica (ID: {person_id})\nResponsabilidade: {avalista.get('responsabilidade', 'N/A')}"
                        }
                        
                        # Adicionar owner_id se disponível
                        if hasattr(active_config, 'PIPEDRIVE_OWNER_ID') and active_config.PIPEDRIVE_OWNER_ID:
                            person_data['owner_id'] = active_config.PIPEDRIVE_OWNER_ID
                        
                        # Adicionar CPF normalizado usando configuração
                        cpf_field_id = CustomFieldsConfig.get_person_field_id('CPF')
                        if cpf_field_id != 'SUBSTITUIR_PELO_ID_REAL':
                            person_data[cpf_field_id] = cpf_avalista_normalizado
                        
                        nova_pessoa = self.pipedrive.create_person(
                            nome_avalista, 
                            cpf_avalista_normalizado, 
                            person_data, 
                            'PF'
                        )
                        
                        if nova_pessoa:
                            logger.info(f"Pessoa física criada e relacionada à pessoa jurídica: {nome_avalista}")
                            self.processing_stats['pessoas_criadas'].append({
                                'id': nova_pessoa['id'],
                                'nome': nome_avalista,
                                'documento': cpf_avalista_normalizado,  # Usar documento normalizado
                                'tipo': 'PF',
                                'entity_type': 'pessoa_vinculada_pessoa_juridica',
                                'person_id': person_id
                            })
                        else:
                            logger.warning(f"Falha ao criar pessoa física relacionada: {nome_avalista}")
                    else:
                        # Pessoa existe, apenas registrar
                        logger.info(f"Pessoa física já existe: {nome_avalista}")
    
    def _handle_existing_person_from_txt(self, entity: Dict, inadimplente: Dict):
        """
        Aplica regras de atualização para entidade (pessoa/organização) que já existe no Pipedrive
        """
        entity_id = entity['id']
        cpf_cnpj_original = inadimplente.get('cpf_cnpj', '')
        nome = inadimplente.get('nome', '')
        tipo_pessoa = inadimplente.get('tipo_pessoa', '')
        
        # Normalizar documento baseado no tipo de pessoa
        cpf_cnpj_normalizado = self._get_normalized_document(cpf_cnpj_original, tipo_pessoa)
        
        entity_type = 'pessoa' if tipo_pessoa == 'PF' else 'organização'
        
        logger.info(f"Atualizando {entity_type} existente {tipo_pessoa}: {nome} (ID: {entity_id})")
        
        # ATUALIZAR DADOS DA ENTIDADE COM INFORMAÇÕES DO TXT
        self._update_existing_entity_data(entity_id, inadimplente, tipo_pessoa, cpf_cnpj_normalizado)
        
        # MUDANÇA: Todos os tipos (PF e PJ) são tratados como pessoas
        negocios = self.pipedrive.get_deals_by_person(entity_id)
        
        # Separar negócios por funis
        negocios_base_nova = []
        negocios_outras_bases = []
        negocios_judiciais = []
        
        # Gerar todas as variantes do documento para busca
        document_variants = self._get_document_variants(cpf_cnpj_original, tipo_pessoa)
        
        for negocio in negocios:
            pipeline_id = negocio.get('pipeline_id')
            titulo = negocio.get('title', '')
            
            # Verificar se é negócio relacionado ao documento (contém qualquer variante no título)
            is_related = any(variant in titulo for variant in document_variants if variant)
            
            if is_related:
                if pipeline_id == active_config.PIPELINE_JUDICIAL_ID:
                    # Pipeline JUDICIAL: não mexer, apenas registrar
                    negocios_judiciais.append(negocio)
                elif pipeline_id in [active_config.PIPELINE_BASE_NOVA_SDR_ID, 
                                    active_config.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID,
                                    active_config.PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID]:
                    negocios_base_nova.append(negocio)
                else:
                    negocios_outras_bases.append(negocio)
        
        # Aplicar regras conforme localização dos negócios
        if negocios_judiciais:
            # Existem negócios na pipeline JUDICIAL: não mexer, apenas registrar
            self._register_judicial_deals(negocios_judiciais, inadimplente)
        
        if negocios_outras_bases:
            # Existem no pipe em outras bases: Status deve ir para open; jogar para BASE NOVA SDR, etapa NOVAS COBRANÇAS
            self._move_deals_to_base_nova_sdr(negocios_outras_bases, inadimplente)
        
        if negocios_base_nova:
            # Existem no pipe nas 3 bases corretas: aplicar regras específicas
            self._apply_rules_to_base_nova_deals(negocios_base_nova, inadimplente)
        
        # SEMPRE criar novo negócio se não existir nas bases corretas (independente de ter na JUDICIAL)
        if not negocios_base_nova and not negocios_outras_bases:
            # Não existe negócio nas bases corretas: criar novo na BASE NOVA SDR com documento normalizado
            self._create_new_deal_for_existing_entity(entity_id, inadimplente, tipo_pessoa, cpf_cnpj_normalizado, nome, entity_type)
    
    def _update_existing_entity_data(self, entity_id: int, inadimplente: Dict, tipo_pessoa: str, cpf_cnpj_normalizado: str):
        """
        Atualiza dados de pessoa/organização existente com informações do TXT
        
        Args:
            entity_id: ID da entidade no Pipedrive
            inadimplente: Dados do TXT
            tipo_pessoa: 'PF' ou 'PJ'
            cpf_cnpj_normalizado: Documento normalizado
        """
        nome = inadimplente.get('nome', '')
        
        if tipo_pessoa == 'PF':
            # Atualizar pessoa física
            person_data = self.file_processor._build_person_data_from_txt(inadimplente)
            # Atualizar com documento normalizado
            person_data = self._update_person_data_with_normalized_document(person_data, cpf_cnpj_normalizado)
            
            logger.debug(f"Dados construídos para pessoa {nome}: {person_data}")
            
            # CORREÇÃO: Filtrar APENAS valores None (não strings vazias nem outros valores falsy)
            filtered_data = {k: v for k, v in person_data.items() if v is not None}
            
            logger.debug(f"Dados após filtragem para pessoa {nome}: {filtered_data}")
            
            # Verificar se há dados para atualizar
            if not filtered_data:
                logger.warning(f"Nenhum dado válido para atualizar pessoa {nome} (ID: {entity_id})")
                return
            
            logger.info(f"Enviando {len(filtered_data)} campos para atualização da pessoa {nome}")
            success = self.pipedrive.update_person(entity_id, filtered_data)
            
            if success:
                logger.info(f"Dados da pessoa {nome} (ID: {entity_id}) atualizados com sucesso")
                self.processing_stats.setdefault('pessoas_atualizadas', []).append({
                    'id': entity_id,
                    'nome': nome,
                    'documento': cpf_cnpj_normalizado,
                    'tipo': tipo_pessoa
                })
            else:
                logger.warning(f"Falha ao atualizar dados da pessoa {nome} (ID: {entity_id})")
                
        elif tipo_pessoa == 'PJ':
            # MUDANÇA: Atualizar PJ como pessoa ao invés de organização
            person_data = self.file_processor._build_person_data_from_txt(inadimplente)
            # Atualizar com documento normalizado
            person_data = self._update_person_data_with_normalized_document(person_data, cpf_cnpj_normalizado)
            
            logger.debug(f"Dados construídos para pessoa jurídica {nome}: {person_data}")
            
            # CORREÇÃO: Filtrar APENAS valores None (não strings vazias nem outros valores falsy)
            filtered_data = {k: v for k, v in person_data.items() if v is not None}
            
            logger.debug(f"Dados após filtragem para pessoa jurídica {nome}: {filtered_data}")
            
            # Verificar se há dados para atualizar
            if not filtered_data:
                logger.warning(f"Nenhum dado válido para atualizar pessoa jurídica {nome} (ID: {entity_id})")
                return
            
            logger.info(f"Enviando {len(filtered_data)} campos para atualização da pessoa jurídica {nome}")
            success = self.pipedrive.update_person(entity_id, filtered_data)
            
            if success:
                logger.info(f"Dados da pessoa jurídica {nome} (ID: {entity_id}) atualizados com sucesso")
                self.processing_stats.setdefault('pessoas_atualizadas', []).append({
                    'id': entity_id,
                    'nome': nome,
                    'documento': cpf_cnpj_normalizado,
                    'tipo': tipo_pessoa
                })
            else:
                logger.warning(f"Falha ao atualizar dados da pessoa jurídica {nome} (ID: {entity_id})")
    
    def _build_deal_data_from_txt(self, inadimplente: Dict, person_id: int) -> Dict:
        """
        Constrói dados do negócio a partir do TXT com todos os campos mapeados
        """
        # Normalizar documento para uso no título e ID_CPF_CNPJ
        cpf_cnpj_original = inadimplente.get('cpf_cnpj', '')
        tipo_pessoa = inadimplente.get('tipo_pessoa', '')
        nome = inadimplente.get('nome', '')
        cpf_cnpj_normalizado = self._get_normalized_document(cpf_cnpj_original, tipo_pessoa)

        # Processar avalistas apenas para PJ; para PF não realizar buscas nem montar texto
        if tipo_pessoa == 'PJ':
            avalistas_text = self._process_avalistas_with_links(inadimplente.get('avalistas_info', []))
        else:
            avalistas_text = ""

        # Construir título sempre com documento normalizado
        titulo_negocio = f"{cpf_cnpj_normalizado} - {nome}"

        # Construir dados usando configuração de campos
        deal_data = {
            # Valor principal
            'value': inadimplente.get('valor_total_com_juros', 0),
            # Sempre atualizar o título
            'title': titulo_negocio,
        }

        # Calcular TAG_ATRASO baseado nos dias de atraso
        dias_atraso = inadimplente.get('dias_atraso_maximo', 0)
        if dias_atraso > 90:
            tag_atraso = 122  # EVITAR PREJU (ATRASO > 90 DIAS)
        else:
            tag_atraso = 121  # EVITAR INAD (ATRASO < 90 DIAS)

        # Mapear campos personalizados usando configuração
        custom_fields_mapping = {
            'COOPERADO': inadimplente.get('cooperado', ''),
            'COOPERATIVA': inadimplente.get('cooperativa', 'OURO VERDE'),
            'TODOS_CONTRATOS': inadimplente.get('todos_contratos', ''),
            'TODAS_OPERACOES': inadimplente.get('todas_operacoes', ''),
            'VENCIMENTO_MAIS_ANTIGO': inadimplente.get('vencimento_mais_antigo', ''),
            'NUMERO_CONTRATO': inadimplente.get('numero_contrato', ''),
            'TIPO_ACAO_CARTEIRA': inadimplente.get('tipo_acao_carteira', ''),
            # Sempre passar o documento normalizado como string
            'ID_CPF_CNPJ': str(cpf_cnpj_normalizado),
            'AVALISTAS': avalistas_text,
            'DIAS_DE_ATRASO': dias_atraso,
            'VALOR_TOTAL_DA_DIVIDA': inadimplente.get('valor_total_divida', 0),
            'VALOR_TOTAL_VENCIDO': inadimplente.get('valor_total_vencido', 0),
            'CONDICAO_CPF': inadimplente.get('condicao_cpf', ''),
            'VALOR_TOTAL_COM_JUROS': inadimplente.get('valor_total_com_juros', 0),
            'TOTAL_DE_PARCELAS': inadimplente.get('total_parcelas', ''),
            'TAG_ATRASO': tag_atraso,  # Usar valor calculado baseado nos dias de atraso
            'CONTRATO_GARANTINORTE': inadimplente.get('contrato_garantinorte', '')
        }

        # Adicionar campos personalizados usando nomes de campos (não IDs)
        # A função update_deal fará o mapeamento para IDs conforme necessário
        for field_name, field_value in custom_fields_mapping.items():
            deal_data[field_name] = self._format_custom_field_value(field_name, field_value)

        return deal_data
    
    def _process_avalistas_with_links(self, avalistas_info: List[Dict]) -> str:
        """
        Processa lista de avalistas verificando se já estão vinculados a outros negócios
        """
        if not avalistas_info:
            return ""
        
        avalistas_text = []
        
        for avalista in avalistas_info:
            cpf_cnpj = avalista.get('cpf_cnpj', '')
            nome = avalista.get('nome', '')
            tipo_pessoa = avalista.get('tipo_pessoa', '')
            responsabilidade = avalista.get('responsabilidade', '')
            
            if not cpf_cnpj or not nome:
                continue
            
            # CORREÇÃO: Normalizar documento do avalista
            cpf_cnpj_normalizado = self._get_normalized_document(cpf_cnpj, tipo_pessoa)
            
            # Verificar se avalista já existe no Pipedrive usando documento normalizado
            pessoa_avalista = self._search_person_by_document(cpf_cnpj_normalizado, tipo_pessoa)
            
            avalista_info = f"{nome} ({tipo_pessoa}: {cpf_cnpj_normalizado})"
            
            if responsabilidade:
                avalista_info += f" - {responsabilidade}"
            
            if pessoa_avalista:
                # Buscar negócios do avalista
                negocios_avalista = self.pipedrive.get_deals_by_person(pessoa_avalista['id'])
                
                if negocios_avalista:
                    # Listar negócios vinculados
                    negocios_ids = [str(neg['id']) for neg in negocios_avalista[:3]]  # Máximo 3 para não ficar muito longo
                    
                    if len(negocios_avalista) > 3:
                        negocios_info = f"Vinculado aos negócios #{', #'.join(negocios_ids)} e +{len(negocios_avalista) - 3} outros"
                    else:
                        negocios_info = f"Vinculado ao(s) negócio(s) #{', #'.join(negocios_ids)}"
                    
                    avalista_info += f" - {negocios_info}"
                else:
                    avalista_info += " - Pessoa cadastrada, sem negócios"
            else:
                avalista_info += " - Novo"
            
            avalistas_text.append(avalista_info)
        
        return "\n".join(avalistas_text)
    
    def _move_deals_to_base_nova_sdr(self, negocios: List[Dict], inadimplente: Dict):
        """
        Move negócios de outras bases para BASE NOVA SDR, etapa NOVAS COBRANÇAS
        """
        deal_data = self._build_deal_data_from_txt(inadimplente, 0)  # person_id será ignorado na atualização
        
        for negocio in negocios:
            deal_id = negocio['id']
            titulo = negocio.get('title', '')
            
            # Atualizar status para open, mover para BASE NOVA SDR e atualizar campos
            update_data = {
                'status': 'open',
                'pipeline_id': active_config.PIPELINE_BASE_NOVA_SDR_ID,
                'stage_id': active_config.STAGE_NOVAS_COBRANÇAS_ID,
                **deal_data  # Adicionar todos os campos personalizados
            }
            
            success = self.pipedrive.update_deal(deal_id, update_data)
            
            if success:
                self.processing_stats['negocios_movidos_para_sdr'].append({
                    'id': deal_id,
                    'titulo': titulo,
                    'origem': 'outras_bases'
                })
                logger.info(f"Negócio movido para BASE NOVA SDR: {titulo}")
            else:
                error_msg = f"Falha ao mover negócio {titulo} para BASE NOVA SDR"
                logger.error(error_msg)
                self.processing_stats['erros'].append(error_msg)
    
    def _apply_rules_to_base_nova_deals(self, negocios: List[Dict], inadimplente: Dict):
        """
        Aplica regras específicas para negócios que estão nas 3 bases corretas
        Esta no txt: status open pipe = ok; won pipe = precisa ir para open; status lost pipe = precisa ir para open
        """
        deal_data = self._build_deal_data_from_txt(inadimplente, 0)  # person_id será ignorado na atualização

        for negocio in negocios:
            deal_id = negocio['id']
            titulo = negocio.get('title', '')
            status = negocio.get('status', '')

            # Sempre atualizar campos personalizados E o título
            update_data = deal_data.copy()
            # Forçar atualização do título
            update_data['title'] = deal_data['title']
            # Forçar atualização do campo ID_CPF_CNPJ
            update_data['ID_CPF_CNPJ'] = deal_data['ID_CPF_CNPJ']

            # Caso está no txt: verificar status
            if status == 'open':
                # Status open = ok, apenas atualizar dados
                logger.info(f"Negócio já está open, atualizando campos: {titulo}")
                self.processing_stats['negocios_atualizados'].append({
                    'id': deal_id,
                    'titulo': titulo,
                    'acao': 'mantido_open_campos_atualizados'
                })
                
            elif status in ['won', 'lost']:
                # Won ou lost = precisa ir para open
                update_data['status'] = 'open'
                
                self.processing_stats['negocios_atualizados'].append({
                    'id': deal_id,
                    'titulo': titulo,
                    'acao': f'alterado_de_{status}_para_open_campos_atualizados'
                })
                logger.info(f"Negócio alterado para open e campos atualizados: {titulo}")
            
            # Executar atualização
            success = self.pipedrive.update_deal(deal_id, update_data)
            
            if not success:
                error_msg = f"Falha ao atualizar negócio {titulo}"
                logger.error(error_msg)
                self.processing_stats['erros'].append(error_msg)
    
    def _process_removed_documents_from_txt(self, documentos_txt: Set[Tuple[str, str]]):
        """
        Processa documentos que não estão mais no TXT atual
        Nao esta no txt: aplicar regras específicas por funil
        """
        logger.info("Processando documentos que não estão mais no TXT...")
        
        # Buscar todos os negócios nos 3 funis principais
        all_deals = self._get_all_deals_in_base_nova_pipelines()
        
        for deal in all_deals:
            titulo = deal.get('title', '')
            pipeline_id = deal.get('pipeline_id')
            stage_id = deal.get('stage_id')
            deal_id = deal['id']
            
            # Extrair documento do título
            documento = self._extract_document_from_title(titulo)
            
            if documento:
                # Determinar tipo de pessoa
                tipo_pessoa = 'pf' if len(documento) == 11 else 'pj' if len(documento) == 14 else 'indefinido'
                
                # Verificar se documento está no TXT
                documento_no_txt = (tipo_pessoa, documento) in documentos_txt
                
                if not documento_no_txt:
                    # Documento não está mais no TXT: aplicar regras
                    self._apply_removal_rules(deal, pipeline_id, stage_id)
    
    def _apply_removal_rules(self, deal: Dict, pipeline_id: int, stage_id: int):
        """
        Aplica regras para documentos que não estão mais no TXT
        Inclui regra de reabrir casos perdidos para "Iniciar Cobrança"
        """
        deal_id = deal['id']
        titulo = deal.get('title', '')
        status = deal.get('status', '')
        
        if pipeline_id == active_config.PIPELINE_JUDICIAL_ID:
            # Pipeline JUDICIAL: não mexer, preservar
            logger.info(f"Preservando negócio JUDICIAL (não modificado): {titulo}")
            self.processing_stats.setdefault('negocios_judiciais_preservados', []).append({
                'id': deal_id,
                'titulo': titulo,
                'pipeline': 'JUDICIAL',
                'acao': 'preservado_remocao_txt'
            })
            
        elif pipeline_id == active_config.PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID:
            # BASE NOVA - FORMALIZAÇÃO/PAGAMENTO: deve permanecer open
            logger.info(f"Mantendo negócio open na FORMALIZAÇÃO: {titulo}")
            self.processing_stats['negocios_mantidos_formalização'].append({
                'id': deal_id,
                'titulo': titulo
            })
            
        elif pipeline_id in [active_config.PIPELINE_BASE_NOVA_SDR_ID, 
                            active_config.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID]:
            # BASE NOVA - SDR ou NEGOCIAÇÃO: verificar se está em etapas específicas
            etapas_excecao = [
                active_config.STAGE_ENVIAR_MINUTA_BOLETO_ID,
                active_config.STAGE_AGUARDANDO_PAGAMENTO_ID,
                active_config.STAGE_ACOMPANHAMENTO_ACORDO_ID,
                active_config.STAGE_BOLETO_PAGO_ID
            ]
            
            if stage_id in etapas_excecao:
                # Está em etapa de exceção: não marcar como perdido
                logger.info(f"Mantendo negócio em etapa de exceção: {titulo}")
                self.processing_stats['negocios_atualizados'].append({
                    'id': deal_id,
                    'titulo': titulo,
                    'acao': 'mantido_etapa_excecao'
                })
            else:
                # NOVA REGRA: Se o negócio está perdido (status = 'lost'), reabrir para "Iniciar Cobrança"
                if status == 'lost':
                    logger.info(f"Reabrindo negócio perdido para Iniciar Cobrança: {titulo}")
                    self._reopen_deal_to_iniciar_cobranca(deal_id, titulo, pipeline_id)
                else:
                    # Negócio não está perdido: marcar como perdido normalmente
                    self._mark_deal_as_lost(deal_id, titulo, "Não consta mais no TXT do banco")
        else:
            # Outros pipelines: verificar se está perdido para reabrir
            if status == 'lost':
                logger.info(f"Reabrindo negócio perdido para Iniciar Cobrança (outro pipeline): {titulo}")
                self._reopen_deal_to_iniciar_cobranca(deal_id, titulo, active_config.PIPELINE_BASE_NOVA_SDR_ID)
            else:
                # Marcar como perdido
                self._mark_deal_as_lost(deal_id, titulo, "Não consta mais no TXT do banco")
    
    def _get_all_deals_in_base_nova_pipelines(self) -> List[Dict]:
        """
        Busca todos os negócios nos 3 funis da BASE NOVA
        """
        all_deals = []
        
        for pipeline_id in [active_config.PIPELINE_BASE_NOVA_SDR_ID,
                           active_config.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID,
                           ]:
            deals = self.pipedrive.get_deals_by_pipeline(pipeline_id)
            all_deals.extend(deals)
        
        return all_deals
    
    def _extract_document_from_title(self, titulo: str) -> str:
        """
        Extrai CPF ou CNPJ do título do negócio e normaliza
        """
        # Assumindo formato: "DOCUMENTO - NOME"
        if ' - ' in titulo:
            documento = titulo.split(' - ')[0].strip()
            # Remover formatação se houver
            documento = ''.join(filter(str.isdigit, documento))
            
            # CORREÇÃO: Normalizar documento extraído
            if documento:
                # Determinar tipo baseado no tamanho
                if len(documento) <= 11:
                    # Possível CPF - normalizar para 11 dígitos
                    return documento.zfill(11)
                elif len(documento) <= 14:
                    # Possível CNPJ - normalizar para 14 dígitos
                    return documento.zfill(14)
                else:
                    # Documento muito longo, tentar extrair CPF ou CNPJ válidos
                    if len(documento) >= 11:
                        return documento[-11:].zfill(11)  # Últimos 11 dígitos como CPF
                    elif len(documento) >= 14:
                        return documento[-14:].zfill(14)  # Últimos 14 dígitos como CNPJ
            
            return documento
        return ''
    
    def _reopen_deal_to_iniciar_cobranca(self, deal_id: int, titulo: str, target_pipeline_id: int):
        """
        Reabre negócio perdido para a etapa "Iniciar Cobrança"
        """
        logger.info(f"Reabrindo negócio perdido para Iniciar Cobrança: {titulo}")
        
        try:
            # Determinar etapa de destino baseada no pipeline
            if target_pipeline_id == active_config.PIPELINE_BASE_NOVA_SDR_ID:
                target_stage_id = active_config.STAGE_INICIAR_COBRANCA_ID
                pipeline_name = "BASE NOVA - SDR"
            elif target_pipeline_id == active_config.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID:
                target_stage_id = active_config.STAGE_INICIAR_COBRANCA_ID
                pipeline_name = "BASE NOVA - NEGOCIAÇÃO"
            else:
                # Para outros pipelines, usar BASE NOVA - SDR
                target_pipeline_id = active_config.PIPELINE_BASE_NOVA_SDR_ID
                target_stage_id = active_config.STAGE_INICIAR_COBRANCA_ID
                pipeline_name = "BASE NOVA - SDR"
            
            # Atualizar negócio: status = 'open', pipeline e stage
            update_data = {
                'status': 'open',
                'pipeline_id': target_pipeline_id,
                'stage_id': target_stage_id
            }
            
            success = self.pipedrive.update_deal(deal_id, update_data)
            
            if success:
                self.processing_stats.setdefault('negocios_reabertos_cobranca', []).append({
                    'id': deal_id,
                    'titulo': titulo,
                    'pipeline_origem': 'perdido',
                    'pipeline_destino': pipeline_name,
                    'etapa_destino': 'Iniciar Cobrança',
                    'acao': 'reaberto_para_iniciar_cobranca'
                })
                logger.info(f"Negócio reaberto para Iniciar Cobrança com sucesso: {titulo}")
            else:
                error_msg = f"Falha ao reabrir negócio {titulo} para Iniciar Cobrança"
                logger.error(error_msg)
                self.processing_stats['erros'].append(error_msg)
                
        except Exception as e:
            error_msg = f"Erro ao reabrir negócio {titulo} para Iniciar Cobrança: {e}"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)
    
    def _mark_deal_as_lost(self, deal_id: int, titulo: str, reason: str):
        """
        Marca negócio como perdido
        """
        logger.info(f"Marcando negócio como perdido: {titulo}")
        
        success = self.pipedrive.mark_deal_as_lost(deal_id, reason)
        
        if success:
            self.processing_stats['negocios_marcados_perdidos'].append({
                'id': deal_id,
                'titulo': titulo,
                'motivo': reason
            })
        else:
            error_msg = f"Falha ao marcar negócio {titulo} como perdido"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)
    
    def _log_final_stats(self):
        """Log das estatísticas finais do processamento"""
        logger.info("=== ESTATÍSTICAS FINAIS ===")
        logger.info(f"Pessoas criadas: {len(self.processing_stats['pessoas_criadas'])}")
        logger.info(f"Negócios criados: {len(self.processing_stats['negocios_criados'])}")
        logger.info(f"Negócios atualizados: {len(self.processing_stats['negocios_atualizados'])}")
        logger.info(f"Negócios movidos para SDR: {len(self.processing_stats['negocios_movidos_para_sdr'])}")
        logger.info(f"Negócios marcados perdidos: {len(self.processing_stats['negocios_marcados_perdidos'])}")
        logger.info(f"Negócios reabertos para Iniciar Cobrança: {len(self.processing_stats.get('negocios_reabertos_cobranca', []))}")
        logger.info(f"Negócios mantidos em formalização: {len(self.processing_stats['negocios_mantidos_formalização'])}")
        logger.info(f"Negócios judiciais preservados: {len(self.processing_stats.get('negocios_judiciais_preservados', []))}")
        logger.info(f"Erros: {len(self.processing_stats['erros'])}")
        logger.info(f"Backup SQLite salvos: {self.processing_stats['backup_sqlite_salvos']}")
        logger.info(f"Backup SQLite atualizados: {self.processing_stats['backup_sqlite_atualizados']}")
        
        if self.processing_stats['erros']:
            logger.warning("ERROS ENCONTRADOS:")
            for erro in self.processing_stats['erros'][:5]:  # Mostrar apenas os primeiros 5
                logger.warning(f"  - {erro}")
                
    def _get_normalized_document(self, document: str, person_type: str) -> str:
        """
        Retorna o documento normalizado baseado no tipo de pessoa
        
        Args:
            document: Documento original (15 dígitos com zeros à esquerda)
            person_type: 'PF' ou 'PJ'
            
        Returns:
            Documento normalizado (11 dígitos para PF, 14 para PJ)
        """
        variants = self.pipedrive._normalize_document_by_type(document, person_type)
        return variants[0] if variants else document
        
    def _get_document_variants(self, document: str, person_type: str) -> List[str]:
        """
        Retorna todas as variantes possíveis do documento para busca
        
        Args:
            document: Documento original
            person_type: 'PF' ou 'PJ'
            
        Returns:
            Lista com todas as variantes possíveis
        """
        variants = self.pipedrive._normalize_document_by_type(document, person_type)
        
        # Adicionar também o documento original para compatibilidade
        variants.append(document)
        
        # Remover duplicatas
        return list(dict.fromkeys(variants))
        
    def _update_deal_data_with_normalized_document(self, deal_data: Dict, normalized_document: str) -> Dict:
        """
        Atualiza os dados do negócio com o documento normalizado
        
        Args:
            deal_data: Dados originais do negócio
            normalized_document: Documento normalizado
            
        Returns:
            Dados atualizados com documento normalizado
        """
        # CORREÇÃO: Manter documento como string para preservar zeros à esquerda
        # Atualizar campo ID_CPF_CNPJ usando o nome do campo (não o ID)
        # A função update_deal fará o mapeamento para o ID correto
        deal_data['ID_CPF_CNPJ'] = normalized_document
        
        return deal_data
        
    def _update_person_data_with_normalized_document(self, person_data: Dict, normalized_document: str) -> Dict:
        """
        Atualiza os dados da pessoa com o documento normalizado
        
        Args:
            person_data: Dados originais da pessoa
            normalized_document: Documento normalizado
            
        Returns:
            Dados atualizados com documento normalizado
        """
        # Atualizar campo CPF usando o nome do campo (não o ID)
        # A função create_person/update_person fará o mapeamento para o ID correto
        person_data['CPF'] = normalized_document
        
        return person_data
        
    def _update_organization_data_with_normalized_document(self, org_data: Dict, normalized_document: str) -> Dict:
        """
        Atualiza os dados da organização com o documento normalizado
        
        Args:
            org_data: Dados originais da organização
            normalized_document: Documento normalizado
            
        Returns:
            Dados atualizados com documento normalizado
        """
        # Atualizar campo CPF_CNPJ usando o nome do campo (não o ID)
        # A função create_organization fará o mapeamento para o ID correto
        org_data['CPF_CNPJ'] = normalized_document
        
        return org_data
    
    def get_processing_summary(self) -> str:
        """
        Retorna resumo do processamento em formato texto
        """
        summary = []
        summary.append("=== RESUMO DO PROCESSAMENTO ===")
        summary.append(f"✅ Pessoas criadas: {len(self.processing_stats['pessoas_criadas'])}")
        summary.append(f"🔄 Pessoas atualizadas: {len(self.processing_stats.get('pessoas_atualizadas', []))}")
        summary.append(f"🏢 Organizações atualizadas: {len(self.processing_stats.get('organizacoes_atualizadas', []))}")
        summary.append(f"✅ Negócios criados: {len(self.processing_stats['negocios_criados'])}")
        summary.append(f"🔄 Negócios atualizados: {len(self.processing_stats['negocios_atualizados'])}")
        summary.append(f"📍 Movidos para BASE NOVA SDR: {len(self.processing_stats['negocios_movidos_para_sdr'])}")
        summary.append(f"❌ Marcados como perdidos: {len(self.processing_stats['negocios_marcados_perdidos'])}")
        summary.append(f"🔒 Mantidos na formalização: {len(self.processing_stats['negocios_mantidos_formalização'])}")
        summary.append(f"⚖️  Negócios judiciais preservados: {len(self.processing_stats.get('negocios_judiciais_preservados', []))}")
        summary.append(f"⚠️  Erros: {len(self.processing_stats['erros'])}")
        summary.append(f"✅ Backup SQLite salvos: {self.processing_stats['backup_sqlite_salvos']}")
        summary.append(f"🔄 Backup SQLite atualizados: {self.processing_stats['backup_sqlite_atualizados']}")
        
        return "\n".join(summary) 
    
    # Método para compatibilidade com interface existente
    def process_inadimplentes_from_excel(self, excel_path: str) -> Dict:
        """
        Método de compatibilidade - redireciona para processamento TXT
        """
        logger.warning("Método Excel obsoleto - redirecionando para processamento TXT")
        # Procurar arquivo TXT correspondente ou mais recente
        txt_path = self.file_processor.find_latest_txt_file()
        if txt_path:
            return self.process_inadimplentes_from_txt(txt_path)
        else:
            error_msg = "Nenhum arquivo TXT encontrado para processamento"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)
            return self.processing_stats 

    def _register_judicial_deals(self, negocios_judiciais: List[Dict], inadimplente: Dict):
        """
        Registra negócios da pipeline JUDICIAL sem modificá-los
        """
        for negocio in negocios_judiciais:
            deal_id = negocio['id']
            titulo = negocio.get('title', '')
            
            logger.info(f"Negócio JUDICIAL preservado (não modificado): {titulo}")
            
            self.processing_stats.setdefault('negocios_judiciais_preservados', []).append({
                'id': deal_id,
                'titulo': titulo,
                'pipeline': 'JUDICIAL',
                'acao': 'preservado_sem_modificacao'
            })
    
    def _create_new_deal_for_existing_entity(self, entity_id: int, inadimplente: Dict, tipo_pessoa: str, cpf_cnpj_normalizado: str, nome: str, entity_type: str):
        """
        Cria novo negócio para entidade existente quando não há negócios nas bases corretas
        """
        titulo_negocio = f"{cpf_cnpj_normalizado} - {nome}"
        deal_data = self._build_deal_data_from_txt(inadimplente, entity_id)
        
        # Atualizar dados com documento normalizado
        deal_data = self._update_deal_data_with_normalized_document(deal_data, cpf_cnpj_normalizado)
        
        # Preparar parâmetros para criação do negócio
        deal_params = {
            'title': titulo_negocio,
            'pipeline_id': active_config.PIPELINE_BASE_NOVA_SDR_ID,
            'stage_id': active_config.STAGE_NOVAS_COBRANÇAS_ID
        }
        
        # MUDANÇA: Todos os tipos (PF e PJ) são tratados como pessoas
        deal_params['person_id'] = entity_id
        
        novo_negocio = self.pipedrive.create_deal(**deal_params)
        
        if novo_negocio:
            # Atualizar negócio com campos personalizados
            self.pipedrive.update_deal(novo_negocio['id'], deal_data)
            
            self.processing_stats['negocios_criados'].append({
                'id': novo_negocio['id'],
                'titulo': titulo_negocio,
                'entity_id': entity_id,
                'entity_type': entity_type,
                'pipeline': 'BASE NOVA - SDR',
                'stage': 'NOVAS COBRANÇAS'
            })
            
            logger.info(f"Novo negócio criado para entidade existente: {titulo_negocio}")

            # Se for PJ, processar pessoas físicas relacionadas
            if tipo_pessoa == 'PJ':
                self._process_related_persons_for_organization(inadimplente, entity_id)
        else:
            error_msg = f"Falha ao criar novo negócio para entidade existente {nome}"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)

    def _format_custom_field_value(self, field_name: str, value: any) -> any:
        """
        Formata valor do campo personalizado baseado no tipo de campo
        
        Args:
            field_name: Nome do campo
            value: Valor original
            
        Returns:
            Valor formatado conforme tipo do campo
        """
        # CORREÇÃO: TAG_ATRASO pode ter valor 0 (que significa sem atraso)
        if field_name == 'TAG_ATRASO':
            if value is None or value == '':
                return None
            # Para TAG_ATRASO, não retornar None se valor for 0
        else:
            # Para outros campos, manter a lógica original
            if value is None or value == '' or value == 0:
                return None
        
        # Campos de data - formato YYYY-MM-DD
        if field_name in ['VENCIMENTO_MAIS_ANTIGO', 'DATA_PREJUIZO_MAIS_ANTIGO', 'DATA_TERCEIRIZACAO', 'DATA_PREVISTA_HONRA']:
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
        
        # CORREÇÃO: Campo ID_CPF_CNPJ - normalizar documento
        if field_name == 'ID_CPF_CNPJ':
            if isinstance(value, str) and value.strip():
                # Normalizar documento baseado no tamanho
                clean_value = ''.join(filter(str.isdigit, value))
                if len(clean_value) == 11:
                    # CPF - manter 11 dígitos
                    return clean_value
                elif len(clean_value) == 14:
                    # CNPJ - manter 14 dígitos
                    return clean_value
                elif len(clean_value) > 14:
                    # Documento muito longo, extrair CPF ou CNPJ
                    if len(clean_value) >= 11:
                        return clean_value[-11:]  # Últimos 11 dígitos como CPF
                    elif len(clean_value) >= 14:
                        return clean_value[-14:]  # Últimos 14 dígitos como CNPJ
                else:
                    # Documento muito curto, preencher com zeros
                    if len(clean_value) <= 11:
                        return clean_value.zfill(11)  # Preencher até 11 dígitos
                    else:
                        return clean_value.zfill(14)  # Preencher até 14 dígitos
            elif isinstance(value, (int, float)):
                # CORREÇÃO: Converter para string e normalizar
                return str(int(value))  # Sempre retornar como string
            return None
        
        # Campos numéricos
        if field_name in ['DIAS_DE_ATRASO', 'ESPE_PARCELAMENTO']:
            if isinstance(value, str) and value.strip():
                try:
                    return int(value)
                except:
                    return None
            elif isinstance(value, (int, float)):
                return int(value)
            return None
        
        # CORREÇÃO: Campo TODAS_OPERACOES - remover zeros à esquerda
        if field_name == 'TODAS_OPERACOES':
            if isinstance(value, str) and value.strip():
                # Remover zeros à esquerda
                return value.lstrip('0') if value.lstrip('0') else '0'
            elif isinstance(value, (int, float)):
                return str(int(value))
            return None
        
        # CORREÇÃO: Campo TODOS_CONTRATOS - remover zeros à esquerda
        if field_name == 'TODOS_CONTRATOS':
            if isinstance(value, str) and value.strip():
                # Remover zeros à esquerda
                return value.lstrip('0') if value.lstrip('0') else '0'
            elif isinstance(value, (int, float)):
                return str(int(value))
            return None
        
        # CORREÇÃO: Campo NUMERO_CONTRATO - remover zeros à esquerda
        if field_name == 'NUMERO_CONTRATO':
            if isinstance(value, str) and value.strip():
                # Remover zeros à esquerda
                return value.lstrip('0') if value.lstrip('0') else '0'
            elif isinstance(value, (int, float)):
                return str(int(value))
            return None
        
        # CORREÇÃO: Campo TOTAL_DE_PARCELAS - remover zeros à esquerda
        if field_name == 'TOTAL_DE_PARCELAS':
            if isinstance(value, str) and value.strip():
                # Remover zeros à esquerda
                return value.lstrip('0') if value.lstrip('0') else '0'
            elif isinstance(value, (int, float)):
                return str(int(value))
            return None
        
        # Campo de múltipla escolha - TAG_ATRASO
        if field_name == 'TAG_ATRASO':
            # CORREÇÃO: Se o valor já for um ID válido (121 ou 122), retornar como lista
            if isinstance(value, (int, float)) and value in [121, 122]:
                return [value]  # Retornar o ID como lista
            # CORREÇÃO: O valor já vem como número do TXT, não como string
            elif isinstance(value, (int, float)) and value > 0:
                # Valor numérico direto do TXT (dias de atraso)
                if value > 90:
                    return [122]  # EVITAR PREJU (ATRASO > 90 DIAS)
                else:
                    return [121]  # EVITAR INAD (ATRASO < 90 DIAS)
            elif isinstance(value, str) and value.strip():
                # Mapear valores de texto para IDs válidos (IDs corretos do Pipedrive)
                # ID 121: EVITAR INAD (ATRASO < 90 DIAS)  
                # ID 122: EVITAR PREJU (ATRASO > 90 DIAS)
                value_upper = value.upper()
                
                # Se contém indicação de > 90 dias, usar ID 122
                if any(indicator in value_upper for indicator in ['> 90', '>90', 'PREJU', 'ACIMA']):
                    return [122]
                # Se contém indicação de < 90 dias, usar ID 121
                elif any(indicator in value_upper for indicator in ['<90', 'INAD', 'EVITAR']):
                    return [121]
                # Para valores numéricos, determinar se > ou < 90
                elif value.isdigit():
                    days = int(value)
                    return [122] if days > 90 else [121]
                else:
                    # Fallback: se não identificar claramente, considerar < 90 dias
                    return [121]
            return None
        
        # Campos de texto - não podem estar vazios
        if isinstance(value, str):
            stripped_value = value.strip()
            return stripped_value if stripped_value else None
        
        # CORREÇÃO: Para campos que podem ser vazios mas não None
        if field_name in ['CONTRATO_GARANTINORTE', 'AVALISTAS']:
            if value is None or value == '':
                return ''  # Retornar string vazia em vez de None
            elif isinstance(value, str):
                return value.strip()
            else:
                return str(value)
        
        return value 

    def _get_deals_for_entity(self, entity_id: int, cpf_cnpj: str, tipo_pessoa: str) -> List[Dict]:
        """
        Busca negócios para uma entidade (pessoa ou organização)
        """
        if tipo_pessoa == 'PF':
            return self.pipedrive.get_deals_by_person(entity_id)
        else:
            # Para organizações, usar busca por título
            return self.pipedrive.search_deals_by_title(cpf_cnpj)
    
    def _get_pipeline_name(self, pipeline_id: int) -> str:
        """
        Retorna nome do pipeline baseado no ID
        """
        pipeline_names = {
            active_config.PIPELINE_BASE_NOVA_SDR_ID: 'BASE NOVA - SDR',
            active_config.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID: 'BASE NOVA - NEGOCIAÇÃO',
            active_config.PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID: 'BASE NOVA - FORMALIZAÇÃO/PAGAMENTO'
        }
        return pipeline_names.get(pipeline_id, f'Pipeline {pipeline_id}')
    
    def _get_stage_name(self, stage_id: int) -> str:
        """
        Retorna nome da etapa baseado no ID
        """
        # Aqui você pode mapear os IDs das etapas para nomes
        # Por enquanto, retornar o ID
        return f'Stage {stage_id}'
    
    def gerar_relatorio_backup_sqlite(self) -> str:
        """
        Gera relatório específico do backup SQLite
        """
        return self.backup_sqlite.gerar_relatorio_backup()
    
    def buscar_entidade_no_backup(self, documento: str, tipo_pessoa: str) -> Optional[Dict]:
        """
        Busca entidade específica no backup SQLite
        """
        return self.backup_sqlite.buscar_entidade_por_documento(documento, tipo_pessoa) 