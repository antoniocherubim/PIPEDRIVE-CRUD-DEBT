"""
Sistema de Backup SQLite para Atualização via TXT

Este módulo implementa um sistema de backup usando banco SQLite que:
- Salva cada devedor como uma entidade completa
- Sobrescreve dados da entidade anterior em cada atualização
- Mantém histórico de versões para auditoria
- Integra com o processamento de arquivos TXT

Funcionalidades:
- Backup automático durante processamento TXT
- Cada linha = uma entidade de devedor completa
- Histórico de alterações com timestamp
- Consultas e relatórios do backup
- Preparado para API serverless na AWS
"""

import sqlite3
import os
import sys
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import logging

# Adicionar o diretório pai ao path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from src.pipedrive_client import PipedriveClient
from src.config import active_config

logger = logging.getLogger(__name__)

class BackupSQLite:
    """
    Sistema de backup SQLite para processamento TXT
    """
    
    def __init__(self, db_name: str = None):
        self.pipedrive = PipedriveClient()
        
        # Nome do banco de dados
        if db_name:
            self.db_name = db_name
        else:
            timestamp = datetime.now().strftime('%Y%m%d')
            self.db_name = f"backup_devedores_{timestamp}.db"
        
        # Diretório do banco
        self.backup_dir = "backup"
        os.makedirs(self.backup_dir, exist_ok=True)
        
        # Caminho completo do banco
        self.db_path = os.path.join(self.backup_dir, self.db_name)
        
        # Inicializar estrutura do banco
        self._init_database()
        
        logger.info(f"BackupSQLite inicializado: {self.db_path}")
    
    def _init_database(self):
        """
        Inicializa a estrutura do banco de dados SQLite
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Tabela principal: entidades_devedores
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS entidades_devedores (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        documento TEXT NOT NULL,
                        tipo_pessoa TEXT NOT NULL,
                        nome TEXT NOT NULL,
                        
                        -- IDs no Pipedrive
                        pipedrive_person_id INTEGER,
                        pipedrive_org_id INTEGER,
                        pipedrive_deal_id INTEGER,
                        
                        -- Dados financeiros
                        valor_total_divida REAL,
                        valor_total_vencido REAL,
                        valor_total_com_juros REAL,
                        dias_atraso_maximo INTEGER,
                        
                        -- Dados contratuais
                        cooperado TEXT,
                        cooperativa TEXT,
                        numero_contrato TEXT,
                        todos_contratos TEXT,
                        todas_operacoes TEXT,
                        vencimento_mais_antigo TEXT,
                        tipo_acao_carteira TEXT,
                        total_parcelas TEXT,
                        tag_atraso TEXT,
                        contrato_garantinorte TEXT,
                        
                        -- Dados de contato
                        telefones TEXT,  -- JSON array
                        emails TEXT,     -- JSON array
                        endereco_completo TEXT,
                        
                        -- Avalistas
                        avalistas_info TEXT,  -- JSON array
                        
                        -- Dados específicos PF
                        data_nascimento TEXT,
                        rg TEXT,
                        nome_mae TEXT,
                        estado_civil TEXT,
                        
                        -- Condições especiais
                        condicao_cpf TEXT,
                        
                        -- Metadados
                        created_at TEXT NOT NULL DEFAULT (datetime('now')),
                        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                        version INTEGER DEFAULT 1,
                        
                        -- Status da operação
                        status_operacao TEXT,  -- 'criado', 'atualizado', 'removido'
                        pipeline_atual TEXT,   -- Pipeline onde está o negócio
                        stage_atual TEXT,      -- Etapa atual do negócio
                        
                        -- Dados originais do TXT (para auditoria)
                        raw_txt_data TEXT,     -- JSON do registro original do TXT
                        
                        UNIQUE(documento, tipo_pessoa)
                    )
                ''')
                
                # Tabela de histórico: historico_alteracoes
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS historico_alteracoes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        entidade_id INTEGER NOT NULL,
                        documento TEXT NOT NULL,
                        tipo_pessoa TEXT NOT NULL,
                        acao TEXT NOT NULL,  -- 'criado', 'atualizado', 'removido'
                        
                        -- Dados antes da alteração
                        dados_anteriores TEXT,  -- JSON
                        dados_novos TEXT,       -- JSON
                        
                        -- Detalhes da operação
                        origem_arquivo TEXT,    -- Nome do arquivo TXT processado
                        timestamp_operacao TEXT NOT NULL,
                        observacoes TEXT,
                        
                        FOREIGN KEY (entidade_id) REFERENCES entidades_devedores (id)
                    )
                ''')
                
                # Tabela de processamentos: log_processamentos
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS log_processamentos (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        arquivo_txt TEXT NOT NULL,
                        timestamp_inicio TEXT NOT NULL,
                        timestamp_fim TEXT,
                        
                        -- Estatísticas
                        total_registros_txt INTEGER,
                        entidades_criadas INTEGER DEFAULT 0,
                        entidades_atualizadas INTEGER DEFAULT 0,
                        entidades_removidas INTEGER DEFAULT 0,
                        
                        -- Status
                        status TEXT,  -- 'processando', 'concluido', 'erro'
                        mensagem_erro TEXT,
                        
                        -- Configurações usadas
                        configuracao_ativa TEXT  -- JSON das configurações
                    )
                ''')
                
                # Índices para performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_documento ON entidades_devedores(documento)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_tipo_pessoa ON entidades_devedores(tipo_pessoa)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_pipedrive_ids ON entidades_devedores(pipedrive_person_id, pipedrive_org_id, pipedrive_deal_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_updated_at ON entidades_devedores(updated_at)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_historico_entidade ON historico_alteracoes(entidade_id)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_historico_timestamp ON historico_alteracoes(timestamp_operacao)')
                
                conn.commit()
                logger.info("Estrutura do banco SQLite criada com sucesso")
                
        except Exception as e:
            logger.error(f"Erro ao inicializar banco de dados: {e}")
            raise
    
    def salvar_entidade_devedor(self, 
                               inadimplente: Dict, 
                               pipedrive_person_id: int = None,
                               pipedrive_org_id: int = None, 
                               pipedrive_deal_id: int = None,
                               status_operacao: str = 'criado',
                               pipeline_atual: str = None,
                               stage_atual: str = None,
                               origem_arquivo: str = None) -> bool:
        """
        Salva ou atualiza uma entidade de devedor no banco SQLite
        
        Args:
            inadimplente: Dados do devedor do TXT
            pipedrive_person_id: ID da pessoa no Pipedrive (se PF)
            pipedrive_org_id: ID da organização no Pipedrive (se PJ)  
            pipedrive_deal_id: ID do negócio no Pipedrive
            status_operacao: Status da operação ('criado', 'atualizado', 'removido')
            pipeline_atual: Pipeline atual do negócio
            stage_atual: Etapa atual do negócio
            origem_arquivo: Nome do arquivo TXT processado
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                documento = inadimplente.get('cpf_cnpj', '')
                tipo_pessoa = inadimplente.get('tipo_pessoa', '')
                nome = inadimplente.get('nome', '')
                
                if not documento or not tipo_pessoa or not nome:
                    logger.warning("Dados insuficientes para salvar entidade")
                    return False
                
                # Verificar se entidade já existe
                cursor.execute('''
                    SELECT id, version FROM entidades_devedores 
                    WHERE documento = ? AND tipo_pessoa = ?
                ''', (documento, tipo_pessoa))
                
                resultado = cursor.fetchone()
                dados_anteriores = None
                
                if resultado:
                    # Entidade existe - atualizar
                    entidade_id, version_atual = resultado
                    
                    # Buscar dados anteriores para histórico
                    cursor.execute('SELECT * FROM entidades_devedores WHERE id = ?', (entidade_id,))
                    row = cursor.fetchone()
                    if row:
                        columns = [description[0] for description in cursor.description]
                        dados_anteriores = dict(zip(columns, row))
                    
                    # Atualizar entidade
                    cursor.execute('''
                        UPDATE entidades_devedores SET
                            nome = ?,
                            pipedrive_person_id = ?,
                            pipedrive_org_id = ?,
                            pipedrive_deal_id = ?,
                            valor_total_divida = ?,
                            valor_total_vencido = ?,
                            valor_total_com_juros = ?,
                            dias_atraso_maximo = ?,
                            cooperado = ?,
                            cooperativa = ?,
                            numero_contrato = ?,
                            todos_contratos = ?,
                            todas_operacoes = ?,
                            vencimento_mais_antigo = ?,
                            tipo_acao_carteira = ?,
                            total_parcelas = ?,
                            tag_atraso = ?,
                            contrato_garantinorte = ?,
                            telefones = ?,
                            emails = ?,
                            endereco_completo = ?,
                            avalistas_info = ?,
                            data_nascimento = ?,
                            rg = ?,
                            nome_mae = ?,
                            estado_civil = ?,
                            condicao_cpf = ?,
                            updated_at = datetime('now'),
                            version = version + 1,
                            status_operacao = ?,
                            pipeline_atual = ?,
                            stage_atual = ?,
                            raw_txt_data = ?
                        WHERE id = ?
                    ''', (
                        nome,
                        pipedrive_person_id,
                        pipedrive_org_id,
                        pipedrive_deal_id,
                        inadimplente.get('valor_total_divida', 0),
                        inadimplente.get('valor_total_vencido', 0),
                        inadimplente.get('valor_total_com_juros', 0),
                        inadimplente.get('dias_atraso_maximo', 0),
                        inadimplente.get('cooperado', ''),
                        inadimplente.get('cooperativa', 'OURO VERDE'),
                        inadimplente.get('numero_contrato', ''),
                        inadimplente.get('todos_contratos', ''),
                        inadimplente.get('todas_operacoes', ''),
                        inadimplente.get('vencimento_mais_antigo', ''),
                        inadimplente.get('tipo_acao_carteira', ''),
                        inadimplente.get('total_parcelas', ''),
                        inadimplente.get('tag_atraso', ''),
                        inadimplente.get('contrato_garantinorte', ''),
                        json.dumps(inadimplente.get('telefones', [])),
                        json.dumps(inadimplente.get('emails', [])),
                        inadimplente.get('endereco_completo', ''),
                        json.dumps(inadimplente.get('avalistas_info', [])),
                        inadimplente.get('data_nascimento', ''),
                        inadimplente.get('rg', ''),
                        inadimplente.get('nome_mae', ''),
                        inadimplente.get('estado_civil', ''),
                        inadimplente.get('condicao_cpf', ''),
                        status_operacao,
                        pipeline_atual,
                        stage_atual,
                        json.dumps(inadimplente),
                        entidade_id
                    ))
                    
                    acao_historico = 'atualizado'
                    
                else:
                    # Entidade nova - inserir
                    cursor.execute('''
                        INSERT INTO entidades_devedores (
                            documento, tipo_pessoa, nome,
                            pipedrive_person_id, pipedrive_org_id, pipedrive_deal_id,
                            valor_total_divida, valor_total_vencido, valor_total_com_juros,
                            dias_atraso_maximo, cooperado, cooperativa, numero_contrato,
                            todos_contratos, todas_operacoes, vencimento_mais_antigo,
                            tipo_acao_carteira, total_parcelas, tag_atraso, contrato_garantinorte,
                            telefones, emails, endereco_completo, avalistas_info,
                            data_nascimento, rg, nome_mae, estado_civil, condicao_cpf,
                            status_operacao, pipeline_atual, stage_atual, raw_txt_data
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        documento, tipo_pessoa, nome,
                        pipedrive_person_id, pipedrive_org_id, pipedrive_deal_id,
                        inadimplente.get('valor_total_divida', 0),
                        inadimplente.get('valor_total_vencido', 0),
                        inadimplente.get('valor_total_com_juros', 0),
                        inadimplente.get('dias_atraso_maximo', 0),
                        inadimplente.get('cooperado', ''),
                        inadimplente.get('cooperativa', 'OURO VERDE'),
                        inadimplente.get('numero_contrato', ''),
                        inadimplente.get('todos_contratos', ''),
                        inadimplente.get('todas_operacoes', ''),
                        inadimplente.get('vencimento_mais_antigo', ''),
                        inadimplente.get('tipo_acao_carteira', ''),
                        inadimplente.get('total_parcelas', ''),
                        inadimplente.get('tag_atraso', ''),
                        inadimplente.get('contrato_garantinorte', ''),
                        json.dumps(inadimplente.get('telefones', [])),
                        json.dumps(inadimplente.get('emails', [])),
                        inadimplente.get('endereco_completo', ''),
                        json.dumps(inadimplente.get('avalistas_info', [])),
                        inadimplente.get('data_nascimento', ''),
                        inadimplente.get('rg', ''),
                        inadimplente.get('nome_mae', ''),
                        inadimplente.get('estado_civil', ''),
                        inadimplente.get('condicao_cpf', ''),
                        status_operacao,
                        pipeline_atual,
                        stage_atual,
                        json.dumps(inadimplente)
                    ))
                    
                    entidade_id = cursor.lastrowid
                    acao_historico = 'criado'
                
                # Salvar no histórico
                self._salvar_historico(cursor, entidade_id, documento, tipo_pessoa, 
                                     acao_historico, dados_anteriores, inadimplente, origem_arquivo)
                
                conn.commit()
                
                logger.info(f"Entidade {acao_historico}: {nome} ({documento}) - ID: {entidade_id}")
                return True
                
        except Exception as e:
            logger.error(f"Erro ao salvar entidade devedor: {e}")
            return False
    
    def _salvar_historico(self, cursor, entidade_id: int, documento: str, tipo_pessoa: str,
                         acao: str, dados_anteriores: Dict = None, dados_novos: Dict = None,
                         origem_arquivo: str = None):
        """
        Salva registro no histórico de alterações
        """
        try:
            cursor.execute('''
                INSERT INTO historico_alteracoes (
                    entidade_id, documento, tipo_pessoa, acao,
                    dados_anteriores, dados_novos, origem_arquivo, timestamp_operacao
                ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ''', (
                entidade_id, documento, tipo_pessoa, acao,
                json.dumps(dados_anteriores) if dados_anteriores else None,
                json.dumps(dados_novos) if dados_novos else None,
                origem_arquivo
            ))
            
        except Exception as e:
            logger.error(f"Erro ao salvar histórico: {e}")
    
    def buscar_entidade_por_documento(self, documento: str, tipo_pessoa: str) -> Optional[Dict]:
        """
        Busca entidade pelo documento e tipo de pessoa
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row  # Para retornar como dicionário
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM entidades_devedores 
                    WHERE documento = ? AND tipo_pessoa = ?
                    ORDER BY updated_at DESC LIMIT 1
                ''', (documento, tipo_pessoa))
                
                row = cursor.fetchone()
                if row:
                    return dict(row)
                
                return None
                
        except Exception as e:
            logger.error(f"Erro ao buscar entidade: {e}")
            return None
    
    def listar_entidades_por_periodo(self, data_inicio: str = None, data_fim: str = None) -> List[Dict]:
        """
        Lista entidades por período de atualização
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                if data_inicio and data_fim:
                    cursor.execute('''
                        SELECT * FROM entidades_devedores 
                        WHERE updated_at BETWEEN ? AND ?
                        ORDER BY updated_at DESC
                    ''', (data_inicio, data_fim))
                else:
                    cursor.execute('''
                        SELECT * FROM entidades_devedores 
                        ORDER BY updated_at DESC
                    ''')
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Erro ao listar entidades: {e}")
            return []
    
    def obter_estatisticas_backup(self) -> Dict:
        """
        Obtém estatísticas do backup
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                stats = {}
                
                # Total de entidades
                cursor.execute('SELECT COUNT(*) FROM entidades_devedores')
                stats['total_entidades'] = cursor.fetchone()[0]
                
                # Por tipo de pessoa
                cursor.execute('''
                    SELECT tipo_pessoa, COUNT(*) 
                    FROM entidades_devedores 
                    GROUP BY tipo_pessoa
                ''')
                stats['por_tipo'] = dict(cursor.fetchall())
                
                # Por status de operação
                cursor.execute('''
                    SELECT status_operacao, COUNT(*) 
                    FROM entidades_devedores 
                    GROUP BY status_operacao
                ''')
                stats['por_status'] = dict(cursor.fetchall())
                
                # Total de alterações no histórico
                cursor.execute('SELECT COUNT(*) FROM historico_alteracoes')
                stats['total_historico'] = cursor.fetchone()[0]
                
                # Última atualização
                cursor.execute('SELECT MAX(updated_at) FROM entidades_devedores')
                stats['ultima_atualizacao'] = cursor.fetchone()[0]
                
                return stats
                
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {}
    
    def iniciar_processamento(self, arquivo_txt: str, total_registros: int) -> int:
        """
        Registra início de processamento de arquivo TXT
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO log_processamentos (
                        arquivo_txt, timestamp_inicio, total_registros_txt, 
                        status, configuracao_ativa
                    ) VALUES (?, datetime('now'), ?, 'processando', ?)
                ''', (
                    arquivo_txt, 
                    total_registros,
                    json.dumps({
                        'pipeline_sdr': active_config.PIPELINE_BASE_NOVA_SDR_ID,
                        'pipeline_neg': active_config.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID,
                        'pipeline_form': active_config.PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID
                    })
                ))
                
                processamento_id = cursor.lastrowid
                conn.commit()
                
                logger.info(f"Processamento iniciado - ID: {processamento_id}")
                return processamento_id
                
        except Exception as e:
            logger.error(f"Erro ao iniciar processamento: {e}")
            return 0
    
    def finalizar_processamento(self, processamento_id: int, 
                              entidades_criadas: int = 0,
                              entidades_atualizadas: int = 0, 
                              entidades_removidas: int = 0,
                              status: str = 'concluido',
                              mensagem_erro: str = None):
        """
        Finaliza registro de processamento
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    UPDATE log_processamentos SET
                        timestamp_fim = datetime('now'),
                        entidades_criadas = ?,
                        entidades_atualizadas = ?,
                        entidades_removidas = ?,
                        status = ?,
                        mensagem_erro = ?
                    WHERE id = ?
                ''', (
                    entidades_criadas, entidades_atualizadas, entidades_removidas,
                    status, mensagem_erro, processamento_id
                ))
                
                conn.commit()
                logger.info(f"Processamento finalizado - ID: {processamento_id}")
                
        except Exception as e:
            logger.error(f"Erro ao finalizar processamento: {e}")

    def gerar_relatorio_backup(self, formato: str = 'texto') -> str:
        """
        Gera relatório do backup
        """
        try:
            stats = self.obter_estatisticas_backup()
            
            if formato == 'texto':
                relatorio = []
                relatorio.append("=== RELATÓRIO DO BACKUP SQLITE ===")
                relatorio.append(f"Banco: {self.db_name}")
                relatorio.append(f"Localização: {self.db_path}")
                relatorio.append("")
                relatorio.append("ESTATÍSTICAS:")
                relatorio.append(f"- Total de entidades: {stats.get('total_entidades', 0)}")
                relatorio.append("")
                
                por_tipo = stats.get('por_tipo', {})
                if por_tipo:
                    relatorio.append("Por tipo de pessoa:")
                    for tipo, qtd in por_tipo.items():
                        relatorio.append(f"  - {tipo}: {qtd}")
                    relatorio.append("")
                
                por_status = stats.get('por_status', {})
                if por_status:
                    relatorio.append("Por status:")
                    for status, qtd in por_status.items():
                        relatorio.append(f"  - {status}: {qtd}")
                    relatorio.append("")
                
                relatorio.append(f"Total de alterações no histórico: {stats.get('total_historico', 0)}")
                relatorio.append(f"Última atualização: {stats.get('ultima_atualizacao', 'N/A')}")
                
                return "\n".join(relatorio)
                
            elif formato == 'json':
                return json.dumps(stats, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logger.error(f"Erro ao gerar relatório: {e}")
            return f"Erro ao gerar relatório: {e}" 