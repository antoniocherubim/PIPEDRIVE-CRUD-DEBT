"""
Sistema de processamento em background persistente
Permite processamento independente da GUI com recuperação de estado
"""
import json
import os
import signal
import sys
import time
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
import threading
from pathlib import Path

logger = logging.getLogger(__name__)

class BackgroundProcessor:
    """Processador em background com persistência de estado"""
    
    def __init__(self, config_file="processing_state.json", log_file="background_processing.log"):
        self.config_file = config_file
        self.log_file = log_file
        self.processing_state = {}
        self.should_stop = False
        self.current_processor = None
        
        # Configurar logging
        self._setup_logging()
        
        # Configurar handlers para parada graciosa
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Lock para operações thread-safe
        self.state_lock = threading.Lock()
    
    def _setup_logging(self):
        """Configura logging para processamento em background"""
        log_dir = Path("logs/background")
        log_dir.mkdir(parents=True, exist_ok=True)
        
        log_path = log_dir / self.log_file
        
        # Configurar logger
        handler = logging.FileHandler(log_path, encoding='utf-8')
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        bg_logger = logging.getLogger('background_processor')
        bg_logger.addHandler(handler)
        bg_logger.setLevel(logging.INFO)
        
        self.bg_logger = bg_logger
    
    def _signal_handler(self, signum, frame):
        """Handler para parada graciosa"""
        self.bg_logger.info(f"Recebido sinal {signum}. Parando graciosamente...")
        self.should_stop = True
        
        # Parar processador atual se existir
        if self.current_processor:
            self.current_processor.stop_processing()
    
    def process_file_background(self, txt_path: str, db_name: str = None, 
                              processor_class=None) -> Dict:
        """
        Processamento em background que persiste estado
        
        Args:
            txt_path: Caminho do arquivo TXT
            db_name: Nome do banco de dados
            processor_class: Classe do processador a usar
        """
        self.bg_logger.info(f"Iniciando processamento em background: {txt_path}")
        
        try:
            # 1. Carregar estado anterior se existir
            self._load_processing_state(txt_path)
            
            # 2. Verificar se há processamento anterior
            if self._has_incomplete_processing(txt_path):
                resume = self._ask_resume_processing(txt_path)
                if not resume:
                    self._clear_processing_state(txt_path)
            
            # 3. Processar arquivo
            if processor_class is None:
                from src.file_processor import FileProcessor
                processor_class = FileProcessor
            
            file_processor = processor_class()
            inadimplentes_data = file_processor.process_txt_file_direct(txt_path)
            
            if not inadimplentes_data:
                self.bg_logger.error("Nenhum dado encontrado no arquivo TXT")
                return {'error': 'Nenhum dado encontrado'}
            
            self.bg_logger.info(f"Total de itens para processar: {len(inadimplentes_data)}")
            
            # 4. Continuar de onde parou (se aplicável)
            start_index = self.processing_state.get('last_processed_index', 0)
            self.bg_logger.info(f"Continuando processamento do índice {start_index}")
            
            # 5. Configurar processador otimizado
            from src.optimized_business_rules import OptimizedBusinessRulesProcessor
            self.current_processor = OptimizedBusinessRulesProcessor(
                db_name=db_name,
                max_concurrent_requests=3,  # Mais conservador para background
                batch_size=25  # Lotes menores para background
            )
            
            # 6. Processar itens
            results = self._process_items_with_persistence(
                inadimplentes_data[start_index:], 
                start_index,
                os.path.basename(txt_path)
            )
            
            # 7. Limpar estado ao finalizar
            self._clear_processing_state(txt_path)
            
            self.bg_logger.info("Processamento em background concluído")
            return results
            
        except Exception as e:
            self.bg_logger.error(f"Erro no processamento: {e}")
            return {'error': str(e)}
    
    def _process_items_with_persistence(self, items: List[Dict], start_index: int, 
                                      arquivo_nome: str) -> Dict:
        """Processa itens com persistência de estado"""
        
        results = {
            'pessoas_criadas': [],
            'negocios_criados': [],
            'negocios_atualizados': [],
            'negocios_movidos_para_sdr': [],
            'negocios_marcados_perdidos': [],
            'negocios_mantidos_formalização': [],
            'erros': []
        }
        
        total_items = len(items)
        processed_count = 0
        
        try:
            for i, item in enumerate(items):
                # Verificar se deve parar
                if self.should_stop:
                    self.bg_logger.info("Parada solicitada pelo usuário")
                    break
                
                try:
                    # Processar item usando processador otimizado
                    result = self.current_processor._process_single_item_with_retry(
                        item, arquivo_nome
                    )
                    
                    # Consolidar resultados
                    for key in results:
                        if key in result:
                            results[key].extend(result[key])
                    
                    processed_count += 1
                    
                    # Salvar estado a cada 10 itens
                    if processed_count % 10 == 0:
                        current_index = start_index + processed_count
                        self._save_processing_state(
                            self.processing_state.get('txt_path', ''),
                            current_index,
                            results
                        )
                    
                    # Log de progresso
                    if processed_count % 50 == 0 or processed_count == total_items:
                        percentage = (processed_count / total_items) * 100
                        self.bg_logger.info(
                            f"Progresso: {processed_count}/{total_items} "
                            f"({percentage:.1f}%) - Índice: {start_index + processed_count}"
                        )
                    
                except Exception as e:
                    error_msg = f"Erro no item {start_index + i}: {e}"
                    results['erros'].append(error_msg)
                    self.bg_logger.error(error_msg)
            
            # Log final
            self.bg_logger.info(f"Processamento concluído: {processed_count} itens processados")
            self.bg_logger.info(f"Pessoas criadas: {len(results['pessoas_criadas'])}")
            self.bg_logger.info(f"Negócios criados: {len(results['negocios_criados'])}")
            self.bg_logger.info(f"Negócios atualizados: {len(results['negocios_atualizados'])}")
            self.bg_logger.info(f"Erros: {len(results['erros'])}")
            
        except Exception as e:
            self.bg_logger.error(f"Erro no processamento: {e}")
            results['erros'].append(str(e))
        
        return results
    
    def _has_incomplete_processing(self, txt_path: str) -> bool:
        """Verifica se há processamento incompleto"""
        with self.state_lock:
            return (
                os.path.exists(self.config_file) and
                self.processing_state.get('txt_path') == txt_path and
                self.processing_state.get('status') == 'processing'
            )
    
    def _ask_resume_processing(self, txt_path: str) -> bool:
        """Pergunta se deve retomar processamento"""
        # Em background, sempre retomar automaticamente
        # (em GUI, faria pergunta ao usuário)
        self.bg_logger.info("Processamento anterior encontrado. Retomando automaticamente.")
        return True
    
    def _save_processing_state(self, txt_path: str, last_index: int, results: Dict):
        """Salva estado do processamento"""
        with self.state_lock:
            state = {
                'txt_path': txt_path,
                'last_processed_index': last_index,
                'timestamp': datetime.now().isoformat(),
                'status': 'processing',
                'results': results,
                'total_items': results.get('total_items', 0)
            }
            
            try:
                with open(self.config_file, 'w', encoding='utf-8') as f:
                    json.dump(state, f, indent=2, ensure_ascii=False)
                
                self.processing_state = state
                self.bg_logger.debug(f"Estado salvo: índice {last_index}")
                
            except Exception as e:
                self.bg_logger.error(f"Erro ao salvar estado: {e}")
    
    def _load_processing_state(self, txt_path: str):
        """Carrega estado do processamento"""
        with self.state_lock:
            try:
                if os.path.exists(self.config_file):
                    with open(self.config_file, 'r', encoding='utf-8') as f:
                        state = json.load(f)
                    
                    if state.get('txt_path') == txt_path:
                        self.processing_state = state
                        self.bg_logger.info(
                            f"Estado carregado: continuando do índice "
                            f"{state.get('last_processed_index', 0)}"
                        )
                    else:
                        self.bg_logger.info("Arquivo diferente. Iniciando novo processamento.")
                        self.processing_state = {}
                else:
                    self.processing_state = {}
                    
            except Exception as e:
                self.bg_logger.error(f"Erro ao carregar estado: {e}")
                self.processing_state = {}
    
    def _clear_processing_state(self, txt_path: str):
        """Limpa estado do processamento"""
        with self.state_lock:
            try:
                if os.path.exists(self.config_file):
                    with open(self.config_file, 'r', encoding='utf-8') as f:
                        state = json.load(f)
                    
                    if state.get('txt_path') == txt_path:
                        # Marcar como concluído
                        state['status'] = 'completed'
                        state['completion_timestamp'] = datetime.now().isoformat()
                        
                        with open(self.config_file, 'w', encoding='utf-8') as f:
                            json.dump(state, f, indent=2, ensure_ascii=False)
                        
                        self.bg_logger.info("Estado do processamento marcado como concluído")
                        
                        # Remover arquivo após 1 hora
                        threading.Timer(3600, self._cleanup_state_file).start()
                        
            except Exception as e:
                self.bg_logger.error(f"Erro ao limpar estado: {e}")
    
    def _cleanup_state_file(self):
        """Remove arquivo de estado após processamento concluído"""
        try:
            if os.path.exists(self.config_file):
                os.remove(self.config_file)
                self.bg_logger.info("Arquivo de estado removido")
        except Exception as e:
            self.bg_logger.error(f"Erro ao remover arquivo de estado: {e}")
    
    def get_processing_status(self) -> Dict[str, Any]:
        """Retorna status atual do processamento"""
        with self.state_lock:
            if not os.path.exists(self.config_file):
                return {'status': 'idle'}
            
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                
                return {
                    'status': state.get('status', 'unknown'),
                    'txt_path': state.get('txt_path', ''),
                    'last_processed_index': state.get('last_processed_index', 0),
                    'timestamp': state.get('timestamp', ''),
                    'total_items': state.get('total_items', 0)
                }
                
            except Exception as e:
                self.bg_logger.error(f"Erro ao ler status: {e}")
                return {'status': 'error', 'error': str(e)}
    
    def stop_background_processing(self):
        """Para processamento em background"""
        self.bg_logger.info("Solicitando parada do processamento em background")
        self.should_stop = True
        
        if self.current_processor:
            self.current_processor.stop_processing()
    
    def run_as_service(self, txt_path: str, db_name: str = None, 
                      interval_hours: int = 24):
        """
        Executa processamento como serviço em intervalos regulares
        
        Args:
            txt_path: Caminho do arquivo TXT
            db_name: Nome do banco de dados
            interval_hours: Intervalo em horas entre execuções
        """
        self.bg_logger.info(f"Iniciando serviço de processamento a cada {interval_hours}h")
        
        while not self.should_stop:
            try:
                # Verificar se arquivo foi modificado
                if os.path.exists(txt_path):
                    file_mtime = os.path.getmtime(txt_path)
                    last_processed = self.processing_state.get('last_file_mtime', 0)
                    
                    if file_mtime > last_processed:
                        self.bg_logger.info("Arquivo modificado detectado. Iniciando processamento...")
                        
                        # Atualizar timestamp do arquivo
                        self.processing_state['last_file_mtime'] = file_mtime
                        
                        # Processar arquivo
                        results = self.process_file_background(txt_path, db_name)
                        
                        self.bg_logger.info(f"Processamento concluído: {results}")
                    else:
                        self.bg_logger.debug("Arquivo não foi modificado")
                else:
                    self.bg_logger.warning(f"Arquivo não encontrado: {txt_path}")
                
                # Aguardar próximo ciclo
                if not self.should_stop:
                    sleep_seconds = interval_hours * 3600
                    self.bg_logger.info(f"Aguardando {interval_hours}h até próximo processamento...")
                    
                    # Aguardar em pequenos intervalos para permitir parada
                    for _ in range(sleep_seconds // 60):  # Verificar a cada minuto
                        if self.should_stop:
                            break
                        time.sleep(60)
                        
            except Exception as e:
                self.bg_logger.error(f"Erro no serviço: {e}")
                time.sleep(300)  # Aguardar 5 minutos em caso de erro
        
        self.bg_logger.info("Serviço de processamento finalizado")

def main():
    """Função principal para execução em background"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Processador em background do Pipedrive')
    parser.add_argument('txt_path', help='Caminho do arquivo TXT')
    parser.add_argument('--db-name', help='Nome do banco de dados')
    parser.add_argument('--service', action='store_true', help='Executar como serviço')
    parser.add_argument('--interval', type=int, default=24, help='Intervalo em horas para serviço')
    
    args = parser.parse_args()
    
    # Configurar logging básico
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    processor = BackgroundProcessor()
    
    try:
        if args.service:
            processor.run_as_service(args.txt_path, args.db_name, args.interval)
        else:
            results = processor.process_file_background(args.txt_path, args.db_name)
            print(json.dumps(results, indent=2, ensure_ascii=False))
            
    except KeyboardInterrupt:
        processor.stop_background_processing()
        print("Processamento interrompido pelo usuário")

if __name__ == "__main__":
    main()
