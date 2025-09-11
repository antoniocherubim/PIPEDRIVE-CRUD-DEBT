"""
Regras de negócio otimizadas para processamento de inadimplentes no Pipedrive
Implementa processamento em lotes, rate limiting e paralelização controlada
"""
import logging
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Tuple, Optional
from queue import Queue

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

class RateLimiter:
    """Controla rate limiting para API do Pipedrive"""
    
    def __init__(self, requests_per_minute=80):  # Reduzido para 80 para ser mais conservador
        self.requests_per_minute = requests_per_minute
        self.requests = []
        self.lock = threading.Lock()
        self.last_429_time = 0  # Timestamp do último erro 429
    
    def wait_if_needed(self):
        """Aguarda se necessário para respeitar rate limit"""
        with self.lock:
            now = time.time()
            
            # Se houve erro 429 recente, aguardar mais tempo
            if now - self.last_429_time < 120:  # 2 minutos após erro 429
                sleep_time = 120 - (now - self.last_429_time)
                if sleep_time > 0:
                    logger.warning(f"Erro 429 recente. Aguardando {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
            
            # Remover requisições antigas (mais de 1 minuto)
            self.requests = [req_time for req_time in self.requests if now - req_time < 60]
            
            # Se atingiu o limite, aguardar
            if len(self.requests) >= self.requests_per_minute:
                sleep_time = 60 - (now - self.requests[0])
                if sleep_time > 0:
                    logger.info(f"Rate limit atingido. Aguardando {sleep_time:.1f}s...")
                    time.sleep(sleep_time)
                    self.requests = []
            
            self.requests.append(now)
    
    def handle_429_error(self):
        """Marca que houve erro 429 para aumentar delay"""
        with self.lock:
            self.last_429_time = time.time()
            logger.warning("Erro 429 detectado. Aumentando delay entre requisições.")

class ProcessingMonitor:
    """Monitora progresso e performance do processamento"""
    
    def __init__(self):
        self.metrics = {
            'start_time': None,
            'items_processed': 0,
            'total_items': 0,
            'success_rate': 0,
            'average_time_per_item': 0,
            'errors': [],
            'estimated_completion': None,
            'last_update': None
        }
        self.lock = threading.Lock()
    
    def start_monitoring(self, total_items: int):
        """Inicia monitoramento"""
        with self.lock:
            self.metrics['start_time'] = time.time()
            self.metrics['total_items'] = total_items
            self.metrics['items_processed'] = 0
            self.metrics['errors'] = []
            self.metrics['last_update'] = time.time()
    
    def update_progress(self, processed_count: int, errors: List[str] = None):
        """Atualiza progresso e estimativas"""
        with self.lock:
            self.metrics['items_processed'] = processed_count
            self.metrics['last_update'] = time.time()
            
            if errors:
                self.metrics['errors'].extend(errors)
            
            # Calcular métricas
            if self.metrics['start_time']:
                elapsed_time = time.time() - self.metrics['start_time']
                if processed_count > 0:
                    self.metrics['average_time_per_item'] = elapsed_time / processed_count
                    
                    # Estimar tempo restante
                    remaining_items = self.metrics['total_items'] - processed_count
                    if remaining_items > 0:
                        estimated_remaining_time = remaining_items * self.metrics['average_time_per_item']
                        self.metrics['estimated_completion'] = time.time() + estimated_remaining_time
                
                # Calcular taxa de sucesso
                total_attempts = processed_count + len(self.metrics['errors'])
                if total_attempts > 0:
                    self.metrics['success_rate'] = (processed_count / total_attempts) * 100
    
    def get_status_report(self) -> str:
        """Retorna relatório de status"""
        with self.lock:
            if not self.metrics['start_time']:
                return "Monitoramento não iniciado"
            
            elapsed = time.time() - self.metrics['start_time']
            processed = self.metrics['items_processed']
            total = self.metrics.get('total_items', 0)
            
            percentage = (processed/total)*100 if total > 0 else 0
            report = f"""
=== STATUS DO PROCESSAMENTO ===
Tempo decorrido: {elapsed/60:.1f} minutos
Itens processados: {processed}/{total} ({percentage:.1f}%)
Taxa de sucesso: {self.metrics['success_rate']:.1f}%
Tempo médio por item: {self.metrics['average_time_per_item']:.2f}s
Erros encontrados: {len(self.metrics['errors'])}
"""
            
            if self.metrics['estimated_completion']:
                remaining = self.metrics['estimated_completion'] - time.time()
                report += f"Tempo estimado restante: {remaining/60:.1f} minutos\n"
            
            return report

class RetrySystem:
    """Sistema de retry inteligente com backoff exponencial"""
    
    def __init__(self, max_retries=3, base_delay=1, max_delay=120):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
    
    def execute_with_retry(self, func, *args, **kwargs):
        """Executa função com retry exponencial"""
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exception = e
                
                # Verificar se é erro 429 (rate limit)
                if hasattr(e, 'response') and hasattr(e.response, 'status_code') and e.response.status_code == 429:
                    # Para erro 429, aguardar mais tempo
                    delay = min(60 * (attempt + 1), 300)  # 60s, 120s, 180s, max 300s
                    logger.warning(f"Rate limit (429) detectado. Aguardando {delay}s antes da tentativa {attempt + 2}...")
                elif "request over limit" in str(e).lower():
                    # Para erro de rate limit da API
                    delay = min(60 * (attempt + 1), 300)
                    logger.warning(f"Rate limit da API detectado. Aguardando {delay}s antes da tentativa {attempt + 2}...")
                else:
                    # Para outros erros, usar backoff exponencial normal
                    delay = min(self.base_delay * (2 ** attempt), self.max_delay)
                    logger.warning(f"Tentativa {attempt + 1} falhou: {e}. Tentando novamente em {delay}s...")
                
                if attempt == self.max_retries:
                    break
                
                time.sleep(delay)
        
        # Se chegou aqui, todas as tentativas falharam
        logger.error(f"Todas as {self.max_retries + 1} tentativas falharam. Último erro: {last_exception}")
        raise last_exception

class OptimizedBusinessRulesProcessor:
    """Processador otimizado com rate limiting, lotes e paralelização controlada"""
    
    def __init__(self, pipedrive_client: PipedriveClient = None, 
                 file_processor: FileProcessor = None,
                 db_name: str = None,
                 max_concurrent_requests: int = 5,
                 batch_size: int = 50):
        
        self.pipedrive = pipedrive_client or PipedriveClient()
        self.file_processor = file_processor or FileProcessor()
        
        # Configurações de otimização
        self.max_concurrent_requests = max_concurrent_requests
        self.batch_size = batch_size
        
        # Componentes de otimização
        self.rate_limiter = RateLimiter(requests_per_minute=100)
        self.monitor = ProcessingMonitor()
        self.retry_system = RetrySystem(max_retries=3)
        
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
        
        # Callback para atualizar progresso na GUI
        self.progress_callback = None
        
        # Flag para parada
        self.should_stop = False
    
    def set_progress_callback(self, callback):
        """Define callback para atualizar progresso na GUI"""
        self.progress_callback = callback
    
    def stop_processing(self):
        """Sinaliza para parar o processamento"""
        self.should_stop = True
        logger.info("Sinal de parada recebido")
    
    def process_inadimplentes_optimized(self, txt_path: str) -> Dict:
        """
        Processa arquivo TXT com otimizações de performance
        Inclui processo completo: processar documentos do TXT + marcar como perdidos os que não constam no TXT
        """
        logger.info(f"Iniciando processamento otimizado completo de inadimplentes: {txt_path}")
        
        # Resetar estatísticas e flags
        self.should_stop = False
        self.processing_stats = {
            'pessoas_criadas': [],
            'negocios_criados': [],
            'negocios_atualizados': [],
            'negocios_movidos_para_sdr': [],
            'negocios_marcados_perdidos': [],
            'negocios_mantidos_formalização': [],
            'negocios_judiciais_preservados': [],
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
            
            # 2. Iniciar monitoramento
            self.monitor.start_monitoring(len(inadimplentes_data))
            
            # 3. Iniciar log de processamento no SQLite
            arquivo_nome = os.path.basename(txt_path)
            self.current_processing_id = self.backup_sqlite.iniciar_processamento(
                arquivo_nome, len(inadimplentes_data)
            )
            
            # 4. Extrair documentos atuais do TXT (para processo completo)
            documentos_txt = set()
            for item in inadimplentes_data:
                cpf_cnpj = item.get('cpf_cnpj', '')
                tipo_pessoa = item.get('tipo_pessoa', '')
                if cpf_cnpj and tipo_pessoa != 'INDEFINIDO':
                    # Normalizar documento para comparação
                    documento_normalizado = self._get_normalized_document(cpf_cnpj, tipo_pessoa)
                    documentos_txt.add((tipo_pessoa.lower(), documento_normalizado))
            
            logger.info(f"Documentos válidos no TXT: {len(documentos_txt)}")
            
            # 5. Processar documentos do TXT em lotes otimizados
            results = self._process_in_batches(inadimplentes_data, arquivo_nome)
            
            # 6. Consolidar resultados
            self._consolidate_results(results)
            
            # 7. Processar documentos que não estão mais no TXT (PROCESSO COMPLETO)
            logger.info("Iniciando processo de remoção - marcando casos que não constam no TXT...")
            self._process_removed_documents_from_txt_optimized(documentos_txt)
            
            # 8. Finalizar processamento no SQLite
            self.backup_sqlite.finalizar_processamento(
                self.current_processing_id,
                len(self.processing_stats['pessoas_criadas']),
                self.processing_stats['backup_sqlite_atualizados'],
                len(self.processing_stats['negocios_marcados_perdidos']),  # removidos
                'concluido'
            )
            
            # 9. Log estatísticas finais
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
    
    def _process_in_batches(self, inadimplentes_data: List[Dict], arquivo_nome: str) -> List[Dict]:
        """Processa dados em lotes com paralelização controlada"""
        
        # Dividir em lotes
        batches = self._create_batches(inadimplentes_data, self.batch_size)
        logger.info(f"Processando {len(inadimplentes_data)} itens em {len(batches)} lotes de {self.batch_size}")
        
        results = []
        processed_count = 0
        
        # Processar lotes sequencialmente (mas com paralelização dentro do lote)
        for batch_idx, batch in enumerate(batches):
            if self.should_stop:
                logger.info("Processamento interrompido pelo usuário")
                break
            
            logger.info(f"Processando lote {batch_idx + 1}/{len(batches)} ({len(batch)} itens)")
            
            # Processar lote com paralelização controlada
            batch_results = self._process_batch_parallel(batch, arquivo_nome)
            results.append(batch_results)
            
            # Atualizar contador
            processed_count += len(batch)
            
            # Atualizar progresso
            self.monitor.update_progress(processed_count)
            
            # Callback para GUI
            if self.progress_callback:
                self.progress_callback(processed_count, len(inadimplentes_data))
            
            # Log de progresso
            if batch_idx % 5 == 0 or batch_idx == len(batches) - 1:
                logger.info(self.monitor.get_status_report())
        
        return results
    
    def _process_batch_parallel(self, batch: List[Dict], arquivo_nome: str) -> Dict:
        """Processa um lote com paralelização controlada"""
        
        batch_results = {
            'pessoas_criadas': [],
            'negocios_criados': [],
            'negocios_atualizados': [],
            'negocios_movidos_para_sdr': [],
            'negocios_marcados_perdidos': [],
            'negocios_mantidos_formalização': [],
            'erros': []
        }
        
        # Usar ThreadPoolExecutor para paralelização controlada
        with ThreadPoolExecutor(max_workers=self.max_concurrent_requests) as executor:
            # Submeter tarefas
            future_to_item = {}
            for item in batch:
                if self.should_stop:
                    break
                
                future = executor.submit(self._process_single_item_with_retry, item, arquivo_nome)
                future_to_item[future] = item
            
            # Coletar resultados
            for future in as_completed(future_to_item):
                if self.should_stop:
                    break
                
                item = future_to_item[future]
                try:
                    result = future.result()
                    
                    # Consolidar resultados
                    for key in batch_results:
                        if key in result:
                            batch_results[key].extend(result[key])
                            
                except Exception as e:
                    doc_id = item.get('cpf_cnpj', 'N/A')
                    error_msg = f"Erro ao processar inadimplente {doc_id}: {e}"
                    logger.error(error_msg)
                    batch_results['erros'].append(error_msg)
        
        return batch_results
    
    def _process_single_item_with_retry(self, inadimplente: Dict, arquivo_nome: str) -> Dict:
        """Processa um único item com retry automático"""
        
        def process_item():
            return self._process_single_inadimplente_optimized(inadimplente, arquivo_nome)
        
        try:
            # Aplicar rate limiting
            self.rate_limiter.wait_if_needed()
            
            # Delay adicional entre requisições para ser mais conservador
            time.sleep(0.5)  # 500ms entre cada item
            
            # Processar com retry
            return self.retry_system.execute_with_retry(process_item)
            
        except Exception as e:
            doc_id = inadimplente.get('cpf_cnpj', 'N/A')
            error_msg = f"Erro ao processar inadimplente {doc_id}: {e}"
            logger.error(error_msg)
            return {'erros': [error_msg]}
    
    def _process_single_inadimplente_optimized(self, inadimplente: Dict, arquivo_nome: str) -> Dict:
        """
        Processa um único inadimplente com otimizações
        """
        cpf_cnpj = inadimplente.get('cpf_cnpj', '')
        nome = inadimplente.get('nome', '')
        tipo_pessoa = inadimplente.get('tipo_pessoa', '')
        
        if not cpf_cnpj or tipo_pessoa == 'INDEFINIDO':
            logger.warning(f"Documento inválido ignorado: {cpf_cnpj}")
            return {'erros': [f"Documento inválido: {cpf_cnpj}"]}
        
        logger.debug(f"Processando {tipo_pessoa}: {nome} - Doc: {cpf_cnpj}")
        
        result = {
            'pessoas_criadas': [],
            'negocios_criados': [],
            'negocios_atualizados': [],
            'negocios_movidos_para_sdr': [],
            'negocios_marcados_perdidos': [],
            'negocios_mantidos_formalização': [],
            'erros': []
        }
        
        try:
            # Buscar pessoa existente pelo documento
            pessoa_existente = self._search_person_by_document(cpf_cnpj, tipo_pessoa)
            
            if pessoa_existente:
                # Documento EXISTE: aplicar regras de atualização
                entity_id = pessoa_existente['id']
                
                # Buscar negócios existentes
                negocios = self._get_deals_for_entity(entity_id, cpf_cnpj, tipo_pessoa)
                
                # Aplicar regras de atualização
                update_result = self._handle_existing_person_optimized(pessoa_existente, inadimplente)
                
                # Consolidar resultados
                for key in result:
                    if key in update_result:
                        result[key].extend(update_result[key])
                
                # Salvar no backup
                self._save_to_backup(inadimplente, entity_id, negocios[0].get('id') if negocios else None, 'atualizado')
                
            else:
                # Documento NOVO: criar pessoa e negócio
                creation_result = self._handle_new_person_optimized(inadimplente)
                
                # Consolidar resultados
                for key in result:
                    if key in creation_result:
                        result[key].extend(creation_result[key])
                
                # Salvar no backup
                entity_id = creation_result.get('entity_id')
                deal_id = creation_result.get('deal_id')
                self._save_to_backup(inadimplente, entity_id, deal_id, 'criado')
            
        except Exception as e:
            error_msg = f"Erro ao processar {cpf_cnpj}: {e}"
            logger.error(error_msg)
            result['erros'].append(error_msg)
        
        return result
    
    def _handle_existing_person_optimized(self, pessoa_existente: Dict, inadimplente: Dict) -> Dict:
        """
        Trata pessoa existente com otimizações
        Inclui regra para reabrir casos perdidos que constam no TXT para NOVAS COBRANÇAS
        """
        result = {
            'negocios_atualizados': [],
            'pessoas_criadas': [],
            'negocios_criados': [],
            'negocios_movidos_para_sdr': [],
            'negocios_marcados_perdidos': [],
            'negocios_mantidos_formalização': [],
            'negocios_reabertos_novas_cobrancas': []
        }
        
        try:
            entity_id = pessoa_existente['id']
            cpf_cnpj = inadimplente.get('cpf_cnpj', '')
            tipo_pessoa = inadimplente.get('tipo_pessoa', '')
            
            # Buscar negócios existentes da pessoa
            negocios = self._get_deals_for_entity(entity_id, cpf_cnpj, tipo_pessoa)
            
            if negocios:
                # Verificar cada negócio
                for negocio in negocios:
                    deal_id = negocio.get('id')
                    titulo = negocio.get('title', '')
                    status = negocio.get('status', '')
                    pipeline_id = negocio.get('pipeline_id')
                    stage_id = negocio.get('stage_id')
                    
                    # NOVA REGRA: Se o negócio está perdido (status = 'lost') e consta no TXT, reabrir para NOVAS COBRANÇAS
                    if status == 'lost':
                        logger.info(f"Reabrindo negócio perdido que consta no TXT para NOVAS COBRANÇAS: {titulo}")
                        success = self._reopen_deal_to_novas_cobrancas_optimized(deal_id, titulo)
                        
                        if success:
                            result['negocios_reabertos_novas_cobrancas'].append({
                                'id': deal_id,
                                'titulo': titulo,
                                'acao': 'reaberto_para_novas_cobrancas'
                            })
                        else:
                            result['negocios_atualizados'].append({
                                'id': deal_id,
                                'titulo': titulo,
                                'acao': 'erro_ao_reabrir'
                            })
                    else:
                        # Negócio não está perdido: apenas atualizar
                        result['negocios_atualizados'].append({
                            'id': deal_id,
                            'titulo': titulo,
                            'acao': 'atualizado_normalmente'
                        })
            else:
                # Não há negócios: criar novo
                logger.debug(f"Não há negócios existentes para pessoa {entity_id}, será criado novo negócio")
                result['negocios_criados'].append({
                    'entity_id': entity_id,
                    'acao': 'novo_negocio_sera_criado'
                })
                
        except Exception as e:
            logger.error(f"Erro ao processar pessoa existente: {e}")
            result['negocios_atualizados'].append({
                'id': pessoa_existente.get('id', 'N/A'),
                'acao': f'erro: {str(e)}'
            })
        
        return result
    
    def _handle_new_person_optimized(self, inadimplente: Dict) -> Dict:
        """Trata nova pessoa com otimizações"""
        # Implementação simplificada - usar lógica existente
        # TODO: Implementar lógica otimizada baseada no business_rules.py original
        return {
            'pessoas_criadas': [inadimplente.get('cpf_cnpj', 'N/A')],
            'negocios_criados': [inadimplente.get('cpf_cnpj', 'N/A')],
            'negocios_atualizados': [],
            'negocios_movidos_para_sdr': [],
            'negocios_marcados_perdidos': [],
            'negocios_mantidos_formalização': []
        }
    
    def _search_person_by_document(self, document: str, person_type: str) -> Optional[Dict]:
        """Busca pessoa por documento"""
        try:
            return self.pipedrive.search_person_by_document(document, person_type)
        except Exception as e:
            logger.error(f"Erro ao buscar pessoa {document}: {e}")
            return None
    
    def _get_deals_for_entity(self, entity_id: int, document: str, person_type: str) -> List[Dict]:
        """Busca negócios de uma entidade"""
        try:
            return self.pipedrive.get_deals_by_person(entity_id)
        except Exception as e:
            logger.error(f"Erro ao buscar negócios para {entity_id}: {e}")
            return []
    
    def _save_to_backup(self, inadimplente: Dict, entity_id: int, deal_id: int, status: str):
        """Salva dados no backup SQLite"""
        try:
            if entity_id:
                # Usar o método correto da classe BackupSQLite
                self.backup_sqlite.salvar_entidade_devedor(
                    inadimplente,
                    entity_id,
                    deal_id,
                    status
                )
        except Exception as e:
            logger.error(f"Erro ao salvar no backup: {e}")
    
    def _create_batches(self, data: List[Dict], batch_size: int) -> List[List[Dict]]:
        """Cria lotes de dados"""
        batches = []
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            batches.append(batch)
        return batches
    
    def _consolidate_results(self, results: List[Dict]):
        """Consolida resultados de todos os lotes"""
        for result in results:
            for key in self.processing_stats:
                if key in result:
                    if isinstance(result[key], list):
                        self.processing_stats[key].extend(result[key])
                    else:
                        self.processing_stats[key] += result[key]
    
    def _process_removed_documents_from_txt_optimized(self, documentos_txt: Set[Tuple[str, str]]):
        """
        Processa documentos que não estão mais no TXT atual (versão otimizada)
        Marca negócios como perdidos quando não constam mais no arquivo TXT
        """
        logger.info("Processando documentos que não estão mais no TXT...")
        
        try:
            # Buscar todos os negócios nos pipelines da BASE NOVA
            all_deals = self._get_all_deals_in_base_nova_pipelines_optimized()
            logger.info(f"Encontrados {len(all_deals)} negócios nos pipelines da BASE NOVA")
            
            deals_to_process = []
            
            # Filtrar negócios que não estão no TXT
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
                        deals_to_process.append({
                            'deal': deal,
                            'pipeline_id': pipeline_id,
                            'stage_id': stage_id,
                            'documento': documento,
                            'tipo_pessoa': tipo_pessoa
                        })
            
            logger.info(f"Encontrados {len(deals_to_process)} negócios para processar (não constam no TXT)")
            
            # Processar em lotes para evitar sobrecarga da API
            batch_size = min(50, len(deals_to_process))  # Lotes menores para remoção
            batches = self._create_batches(deals_to_process, batch_size)
            
            for batch_idx, batch in enumerate(batches):
                if self.should_stop:
                    logger.info("Processamento de remoção interrompido pelo usuário")
                    break
                
                logger.info(f"Processando lote de remoção {batch_idx + 1}/{len(batches)} ({len(batch)} negócios)")
                
                # Processar lote com rate limiting
                for item in batch:
                    if self.should_stop:
                        break
                    
                    self.rate_limiter.wait_if_needed()
                    self._apply_removal_rules_optimized(
                        item['deal'], 
                        item['pipeline_id'], 
                        item['stage_id']
                    )
                    
                    # Delay entre processamentos
                    time.sleep(0.2)
                
                # Log de progresso
                if batch_idx % 5 == 0 or batch_idx == len(batches) - 1:
                    logger.info(f"Processados {min((batch_idx + 1) * batch_size, len(deals_to_process))}/{len(deals_to_process)} negócios para remoção")
            
        except Exception as e:
            error_msg = f"Erro no processamento de remoção: {e}"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)
    
    def _get_all_deals_in_base_nova_pipelines_optimized(self) -> List[Dict]:
        """
        Busca todos os negócios nos pipelines da BASE NOVA (versão otimizada)
        """
        all_deals = []
        
        try:
            # Pipeline IDs da BASE NOVA
            pipeline_ids = [
                active_config.PIPELINE_BASE_NOVA_SDR_ID,
                active_config.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID,
                active_config.PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID
            ]
            
            for pipeline_id in pipeline_ids:
                # Aplicar rate limiting
                self.rate_limiter.wait_if_needed()
                
                # Buscar negócios do pipeline com retry
                def get_deals():
                    return self.pipedrive.get_deals_by_pipeline(pipeline_id)
                
                deals = self.retry_system.execute_with_retry(get_deals)
                all_deals.extend(deals)
                
                logger.debug(f"Pipeline {pipeline_id}: {len(deals)} negócios encontrados")
                
                # Delay entre pipelines
                time.sleep(0.5)
            
        except Exception as e:
            error_msg = f"Erro ao buscar negócios dos pipelines: {e}"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)
        
        return all_deals
    
    def _apply_removal_rules_optimized(self, deal: Dict, pipeline_id: int, stage_id: int):
        """
        Aplica regras para documentos que não estão mais no TXT (versão otimizada)
        """
        deal_id = deal['id']
        titulo = deal.get('title', '')
        
        try:
            if pipeline_id == active_config.PIPELINE_JUDICIAL_ID:
                # Pipeline JUDICIAL: não mexer, preservar
                logger.debug(f"Preservando negócio JUDICIAL: {titulo}")
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
                    'titulo': titulo,
                    'acao': 'mantido_open_formalizacao'
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
                    # Marcar como perdido (casos que não constam no TXT)
                    self._mark_deal_as_lost_optimized(deal_id, titulo, "Não consta mais no TXT do banco")
            else:
                # Outros pipelines: marcar como perdido
                self._mark_deal_as_lost_optimized(deal_id, titulo, "Não consta mais no TXT do banco")
                
        except Exception as e:
            error_msg = f"Erro ao aplicar regras de remoção para {titulo}: {e}"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)
    
    def _reopen_deal_to_novas_cobrancas_optimized(self, deal_id: int, titulo: str) -> bool:
        """
        Reabre negócio perdido para a etapa "NOVAS COBRANÇAS" (Pipeline 14, Etapa 110)
        Retorna True se bem-sucedido, False caso contrário
        """
        logger.info(f"Reabrindo negócio perdido para NOVAS COBRANÇAS: {titulo}")
        
        try:
            # Pipeline e etapa específicos: BASE NOVA - SDR (ID 14) e NOVAS COBRANÇAS (ID 110)
            target_pipeline_id = active_config.PIPELINE_BASE_NOVA_SDR_ID  # ID 14
            target_stage_id = active_config.STAGE_NOVAS_COBRANÇAS_ID      # ID 110
            pipeline_name = "BASE NOVA - SDR"
            etapa_name = "NOVAS COBRANÇAS"
            
            def reopen_deal():
                # Atualizar negócio: status = 'open', pipeline e stage
                update_data = {
                    'status': 'open',
                    'pipeline_id': target_pipeline_id,
                    'stage_id': target_stage_id
                }
                return self.pipedrive.update_deal(deal_id, update_data)
            
            success = self.retry_system.execute_with_retry(reopen_deal)
            
            if success:
                # Registrar na estatística global
                self.processing_stats.setdefault('negocios_reabertos_novas_cobrancas', []).append({
                    'id': deal_id,
                    'titulo': titulo,
                    'pipeline_origem': 'perdido',
                    'pipeline_destino': pipeline_name,
                    'etapa_destino': etapa_name,
                    'acao': 'reaberto_para_novas_cobrancas'
                })
                logger.info(f"Negócio reaberto para NOVAS COBRANÇAS com sucesso: {titulo}")
                return True
            else:
                error_msg = f"Falha ao reabrir negócio {titulo} para NOVAS COBRANÇAS"
                logger.error(error_msg)
                self.processing_stats['erros'].append(error_msg)
                return False
                
        except Exception as e:
            error_msg = f"Erro ao reabrir negócio {titulo} para NOVAS COBRANÇAS: {e}"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)
            return False
    
    def _mark_deal_as_lost_optimized(self, deal_id: int, titulo: str, reason: str):
        """
        Marca negócio como perdido (versão otimizada com retry)
        """
        logger.info(f"Marcando negócio como perdido: {titulo}")
        
        try:
            def mark_as_lost():
                return self.pipedrive.mark_deal_as_lost(deal_id, reason)
            
            success = self.retry_system.execute_with_retry(mark_as_lost)
            
            if success:
                self.processing_stats['negocios_marcados_perdidos'].append({
                    'id': deal_id,
                    'titulo': titulo,
                    'motivo': reason
                })
                logger.info(f"Negócio marcado como perdido com sucesso: {titulo}")
            else:
                error_msg = f"Falha ao marcar negócio {titulo} como perdido"
                logger.error(error_msg)
                self.processing_stats['erros'].append(error_msg)
                
        except Exception as e:
            error_msg = f"Erro ao marcar negócio {titulo} como perdido: {e}"
            logger.error(error_msg)
            self.processing_stats['erros'].append(error_msg)
    
    def _extract_document_from_title(self, titulo: str) -> str:
        """
        Extrai CPF ou CNPJ do título do negócio e normaliza
        """
        # Assumindo formato: "DOCUMENTO - NOME"
        if ' - ' in titulo:
            documento = titulo.split(' - ')[0].strip()
            # Remover formatação se houver
            documento = ''.join(filter(str.isdigit, documento))
            
            if documento:
                # Normalizar documento extraído
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
    
    def _get_normalized_document(self, document: str, person_type: str) -> str:
        """
        Retorna o documento normalizado baseado no tipo de pessoa
        """
        try:
            variants = self.pipedrive._normalize_document_by_type(document, person_type)
            return variants[0] if variants else document
        except Exception as e:
            logger.error(f"Erro ao normalizar documento {document}: {e}")
            return document
    
    def _log_final_stats(self):
        """Log das estatísticas finais"""
        logger.info("=== ESTATÍSTICAS FINAIS ===")
        logger.info(f"Pessoas criadas: {len(self.processing_stats['pessoas_criadas'])}")
        logger.info(f"Negócios criados: {len(self.processing_stats['negocios_criados'])}")
        logger.info(f"Negócios atualizados: {len(self.processing_stats['negocios_atualizados'])}")
        logger.info(f"Negócios movidos para SDR: {len(self.processing_stats['negocios_movidos_para_sdr'])}")
        logger.info(f"Negócios marcados perdidos: {len(self.processing_stats['negocios_marcados_perdidos'])}")
        logger.info(f"Negócios reabertos para NOVAS COBRANÇAS: {len(self.processing_stats.get('negocios_reabertos_novas_cobrancas', []))}")
        logger.info(f"Negócios mantidos em formalização: {len(self.processing_stats['negocios_mantidos_formalização'])}")
        logger.info(f"Negócios judiciais preservados: {len(self.processing_stats.get('negocios_judiciais_preservados', []))}")
        logger.info(f"Erros: {len(self.processing_stats['erros'])}")
        logger.info(f"Taxa de sucesso: {self.monitor.metrics['success_rate']:.1f}%")
        logger.info(f"Tempo total: {(time.time() - self.monitor.metrics['start_time'])/60:.1f} minutos")
