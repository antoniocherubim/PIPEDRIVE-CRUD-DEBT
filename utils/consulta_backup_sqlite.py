"""
Ferramenta de Consulta e Relatórios do Backup SQLite

Esta ferramenta permite consultar e gerar relatórios do sistema de backup SQLite
para análise de dados e auditoria.
"""

import sqlite3
import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

# Adicionar o diretório pai ao path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from backup_sqlite import BackupSQLite

logger = logging.getLogger(__name__)

class ConsultaBackupSQLite:
    """
    Ferramenta para consulta e relatórios do backup SQLite
    """
    
    def __init__(self, db_path: str = None):
        if db_path:
            self.db_path = db_path
        else:
            # Buscar banco mais recente
            self.db_path = self._find_latest_backup_db()
        
        if not self.db_path or not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Banco de dados não encontrado: {self.db_path}")
        
        logger.info(f"Conectado ao banco: {self.db_path}")
    
    def _find_latest_backup_db(self) -> str:
        """
        Encontra o banco de backup mais recente
        """
        backup_dir = "backup"
        if not os.path.exists(backup_dir):
            return ""
        
        db_files = [f for f in os.listdir(backup_dir) if f.endswith('.db') and 'backup_devedores' in f]
        
        if not db_files:
            return ""
        
        # Ordenar por data (assumindo formato backup_devedores_YYYYMMDD.db)
        db_files.sort(reverse=True)
        return os.path.join(backup_dir, db_files[0])
    
    def listar_bancos_disponiveis(self) -> List[str]:
        """
        Lista todos os bancos de backup disponíveis
        """
        backup_dir = "backup"
        if not os.path.exists(backup_dir):
            return []
        
        db_files = [f for f in os.listdir(backup_dir) if f.endswith('.db') and 'backup_devedores' in f]
        return sorted(db_files, reverse=True)
    
    def obter_estatisticas_gerais(self) -> Dict:
        """
        Obtém estatísticas gerais do backup
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                stats = {}
                
                # Informações básicas
                cursor.execute('SELECT COUNT(*) FROM entidades_devedores')
                stats['total_entidades'] = cursor.fetchone()[0]
                
                # Por tipo de pessoa
                cursor.execute('''
                    SELECT tipo_pessoa, COUNT(*) 
                    FROM entidades_devedores 
                    GROUP BY tipo_pessoa
                ''')
                stats['por_tipo_pessoa'] = dict(cursor.fetchall())
                
                # Por status de operação
                cursor.execute('''
                    SELECT status_operacao, COUNT(*) 
                    FROM entidades_devedores 
                    GROUP BY status_operacao
                ''')
                stats['por_status_operacao'] = dict(cursor.fetchall())
                
                # Por pipeline atual
                cursor.execute('''
                    SELECT pipeline_atual, COUNT(*) 
                    FROM entidades_devedores 
                    WHERE pipeline_atual IS NOT NULL
                    GROUP BY pipeline_atual
                ''')
                stats['por_pipeline'] = dict(cursor.fetchall())
                
                # Valores financeiros
                cursor.execute('''
                    SELECT 
                        SUM(valor_total_divida) as total_divida,
                        SUM(valor_total_vencido) as total_vencido,
                        SUM(valor_total_com_juros) as total_com_juros,
                        AVG(dias_atraso_maximo) as media_dias_atraso
                    FROM entidades_devedores
                ''')
                financeiro = cursor.fetchone()
                stats['financeiro'] = {
                    'total_divida': financeiro[0] or 0,
                    'total_vencido': financeiro[1] or 0,
                    'total_com_juros': financeiro[2] or 0,
                    'media_dias_atraso': round(financeiro[3] or 0, 2)
                }
                
                # Período de dados
                cursor.execute('SELECT MIN(created_at), MAX(updated_at) FROM entidades_devedores')
                periodo = cursor.fetchone()
                stats['periodo'] = {
                    'primeira_entrada': periodo[0],
                    'ultima_atualizacao': periodo[1]
                }
                
                # Total de alterações no histórico
                cursor.execute('SELECT COUNT(*) FROM historico_alteracoes')
                stats['total_historico'] = cursor.fetchone()[0]
                
                return stats
                
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {}
    
    def buscar_entidade(self, documento: str = None, nome: str = None, 
                       pipedrive_id: int = None) -> List[Dict]:
        """
        Busca entidades por diferentes critérios
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                conditions = []
                params = []
                
                if documento:
                    conditions.append("documento LIKE ?")
                    params.append(f"%{documento}%")
                
                if nome:
                    conditions.append("nome LIKE ?")
                    params.append(f"%{nome}%")
                
                if pipedrive_id:
                    conditions.append("(pipedrive_person_id = ? OR pipedrive_org_id = ? OR pipedrive_deal_id = ?)")
                    params.extend([pipedrive_id, pipedrive_id, pipedrive_id])
                
                if not conditions:
                    # Se não há filtros, retornar as 10 mais recentes
                    cursor.execute('''
                        SELECT * FROM entidades_devedores 
                        ORDER BY updated_at DESC LIMIT 10
                    ''')
                else:
                    where_clause = " AND ".join(conditions)
                    cursor.execute(f'''
                        SELECT * FROM entidades_devedores 
                        WHERE {where_clause}
                        ORDER BY updated_at DESC
                    ''', params)
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Erro ao buscar entidade: {e}")
            return []
    
    def obter_historico_entidade(self, documento: str, tipo_pessoa: str) -> List[Dict]:
        """
        Obtém histórico de alterações de uma entidade específica
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM historico_alteracoes 
                    WHERE documento = ? AND tipo_pessoa = ?
                    ORDER BY timestamp_operacao DESC
                ''', (documento, tipo_pessoa))
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Erro ao obter histórico: {e}")
            return []
    
    def listar_entidades_por_periodo(self, data_inicio: str = None, data_fim: str = None,
                                   status_operacao: str = None, pipeline: str = None) -> List[Dict]:
        """
        Lista entidades filtradas por período e outros critérios
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                conditions = []
                params = []
                
                if data_inicio:
                    conditions.append("updated_at >= ?")
                    params.append(data_inicio)
                
                if data_fim:
                    conditions.append("updated_at <= ?")
                    params.append(data_fim)
                
                if status_operacao:
                    conditions.append("status_operacao = ?")
                    params.append(status_operacao)
                
                if pipeline:
                    conditions.append("pipeline_atual = ?")
                    params.append(pipeline)
                
                if conditions:
                    where_clause = " AND ".join(conditions)
                    query = f'''
                        SELECT * FROM entidades_devedores 
                        WHERE {where_clause}
                        ORDER BY updated_at DESC
                    '''
                else:
                    query = '''
                        SELECT * FROM entidades_devedores 
                        ORDER BY updated_at DESC
                    '''
                
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Erro ao listar entidades: {e}")
            return []
    
    def gerar_relatorio_financeiro(self) -> Dict:
        """
        Gera relatório financeiro detalhado
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                relatorio = {}
                
                # Por tipo de pessoa
                cursor.execute('''
                    SELECT 
                        tipo_pessoa,
                        COUNT(*) as quantidade,
                        SUM(valor_total_divida) as total_divida,
                        SUM(valor_total_vencido) as total_vencido,
                        SUM(valor_total_com_juros) as total_com_juros,
                        AVG(dias_atraso_maximo) as media_dias_atraso
                    FROM entidades_devedores
                    GROUP BY tipo_pessoa
                ''')
                
                relatorio['por_tipo_pessoa'] = []
                for row in cursor.fetchall():
                    relatorio['por_tipo_pessoa'].append({
                        'tipo': row[0],
                        'quantidade': row[1],
                        'total_divida': row[2] or 0,
                        'total_vencido': row[3] or 0,
                        'total_com_juros': row[4] or 0,
                        'media_dias_atraso': round(row[5] or 0, 2)
                    })
                
                # Por pipeline
                cursor.execute('''
                    SELECT 
                        pipeline_atual,
                        COUNT(*) as quantidade,
                        SUM(valor_total_divida) as total_divida,
                        SUM(valor_total_com_juros) as total_com_juros
                    FROM entidades_devedores
                    WHERE pipeline_atual IS NOT NULL
                    GROUP BY pipeline_atual
                ''')
                
                relatorio['por_pipeline'] = []
                for row in cursor.fetchall():
                    relatorio['por_pipeline'].append({
                        'pipeline': row[0],
                        'quantidade': row[1],
                        'total_divida': row[2] or 0,
                        'total_com_juros': row[3] or 0
                    })
                
                # Faixas de atraso
                cursor.execute('''
                    SELECT 
                        CASE 
                            WHEN dias_atraso_maximo <= 30 THEN '0-30 dias'
                            WHEN dias_atraso_maximo <= 60 THEN '31-60 dias'
                            WHEN dias_atraso_maximo <= 90 THEN '61-90 dias'
                            WHEN dias_atraso_maximo <= 180 THEN '91-180 dias'
                            ELSE '180+ dias'
                        END as faixa_atraso,
                        COUNT(*) as quantidade,
                        SUM(valor_total_com_juros) as total_valor
                    FROM entidades_devedores
                    WHERE dias_atraso_maximo > 0
                    GROUP BY 
                        CASE 
                            WHEN dias_atraso_maximo <= 30 THEN '0-30 dias'
                            WHEN dias_atraso_maximo <= 60 THEN '31-60 dias'
                            WHEN dias_atraso_maximo <= 90 THEN '61-90 dias'
                            WHEN dias_atraso_maximo <= 180 THEN '91-180 dias'
                            ELSE '180+ dias'
                        END
                    ORDER BY 
                        CASE 
                            WHEN dias_atraso_maximo <= 30 THEN 1
                            WHEN dias_atraso_maximo <= 60 THEN 2
                            WHEN dias_atraso_maximo <= 90 THEN 3
                            WHEN dias_atraso_maximo <= 180 THEN 4
                            ELSE 5
                        END
                ''')
                
                relatorio['por_faixa_atraso'] = []
                for row in cursor.fetchall():
                    relatorio['por_faixa_atraso'].append({
                        'faixa': row[0],
                        'quantidade': row[1],
                        'total_valor': row[2] or 0
                    })
                
                return relatorio
                
        except Exception as e:
            logger.error(f"Erro ao gerar relatório financeiro: {e}")
            return {}
    
    def gerar_relatorio_processamentos(self) -> List[Dict]:
        """
        Gera relatório dos processamentos realizados
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM log_processamentos 
                    ORDER BY timestamp_inicio DESC
                ''')
                
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logger.error(f"Erro ao gerar relatório de processamentos: {e}")
            return []
    
    def exportar_entidades_csv(self, arquivo_saida: str, filtros: Dict = None) -> bool:
        """
        Exporta entidades para arquivo CSV
        """
        try:
            import csv
            
            entidades = self.listar_entidades_por_periodo(
                data_inicio=filtros.get('data_inicio') if filtros else None,
                data_fim=filtros.get('data_fim') if filtros else None,
                status_operacao=filtros.get('status_operacao') if filtros else None,
                pipeline=filtros.get('pipeline') if filtros else None
            )
            
            if not entidades:
                logger.warning("Nenhuma entidade encontrada para exportar")
                return False
            
            with open(arquivo_saida, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = entidades[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for entidade in entidades:
                    writer.writerow(entidade)
            
            logger.info(f"Entidades exportadas para: {arquivo_saida}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao exportar CSV: {e}")
            return False
    
    def imprimir_relatorio_completo(self):
        """
        Imprime relatório completo no console
        """
        print("=" * 80)
        print("RELATÓRIO COMPLETO DO BACKUP SQLITE")
        print("=" * 80)
        
        # Estatísticas gerais
        stats = self.obter_estatisticas_gerais()
        
        print("\n📊 ESTATÍSTICAS GERAIS:")
        print(f"   Total de entidades: {stats.get('total_entidades', 0)}")
        print(f"   Período: {stats.get('periodo', {}).get('primeira_entrada', 'N/A')} até {stats.get('periodo', {}).get('ultima_atualizacao', 'N/A')}")
        print(f"   Total de alterações: {stats.get('total_historico', 0)}")
        
        # Por tipo de pessoa
        print("\n👥 POR TIPO DE PESSOA:")
        por_tipo = stats.get('por_tipo_pessoa', {})
        for tipo, qtd in por_tipo.items():
            print(f"   {tipo}: {qtd}")
        
        # Por status
        print("\n📋 POR STATUS DE OPERAÇÃO:")
        por_status = stats.get('por_status_operacao', {})
        for status, qtd in por_status.items():
            print(f"   {status}: {qtd}")
        
        # Por pipeline
        print("\n🔄 POR PIPELINE:")
        por_pipeline = stats.get('por_pipeline', {})
        for pipeline, qtd in por_pipeline.items():
            print(f"   {pipeline}: {qtd}")
        
        # Financeiro
        print("\n💰 RESUMO FINANCEIRO:")
        financeiro = stats.get('financeiro', {})
        print(f"   Total da dívida: R$ {financeiro.get('total_divida', 0):,.2f}")
        print(f"   Total vencido: R$ {financeiro.get('total_vencido', 0):,.2f}")
        print(f"   Total com juros: R$ {financeiro.get('total_com_juros', 0):,.2f}")
        print(f"   Média dias atraso: {financeiro.get('media_dias_atraso', 0)} dias")
        
        # Relatório financeiro detalhado
        print("\n" + "=" * 50)
        print("RELATÓRIO FINANCEIRO DETALHADO")
        print("=" * 50)
        
        rel_financeiro = self.gerar_relatorio_financeiro()
        
        print("\n💰 POR TIPO DE PESSOA:")
        for item in rel_financeiro.get('por_tipo_pessoa', []):
            print(f"   {item['tipo']}:")
            print(f"     Quantidade: {item['quantidade']}")
            print(f"     Total dívida: R$ {item['total_divida']:,.2f}")
            print(f"     Total com juros: R$ {item['total_com_juros']:,.2f}")
            print(f"     Média dias atraso: {item['media_dias_atraso']} dias")
        
        print("\n📈 POR FAIXA DE ATRASO:")
        for item in rel_financeiro.get('por_faixa_atraso', []):
            print(f"   {item['faixa']}: {item['quantidade']} entidades - R$ {item['total_valor']:,.2f}")


def main():
    """Função principal para uso via linha de comando"""
    parser = argparse.ArgumentParser(description='Consulta e relatórios do backup SQLite')
    
    parser.add_argument('--db', help='Caminho do banco SQLite específico')
    parser.add_argument('--listar-bancos', action='store_true', help='Listar bancos disponíveis')
    parser.add_argument('--stats', action='store_true', help='Mostrar estatísticas gerais')
    parser.add_argument('--relatorio-completo', action='store_true', help='Relatório completo')
    parser.add_argument('--buscar-documento', help='Buscar por documento')
    parser.add_argument('--buscar-nome', help='Buscar por nome')
    parser.add_argument('--buscar-id', type=int, help='Buscar por ID do Pipedrive')
    parser.add_argument('--exportar-csv', help='Exportar para arquivo CSV')
    
    args = parser.parse_args()
    
    try:
        consulta = ConsultaBackupSQLite(args.db)
        
        if args.listar_bancos:
            bancos = consulta.listar_bancos_disponiveis()
            print("Bancos disponíveis:")
            for banco in bancos:
                print(f"  - {banco}")
        
        elif args.stats:
            stats = consulta.obter_estatisticas_gerais()
            print(json.dumps(stats, indent=2, ensure_ascii=False))
        
        elif args.relatorio_completo:
            consulta.imprimir_relatorio_completo()
        
        elif args.buscar_documento or args.buscar_nome or args.buscar_id:
            entidades = consulta.buscar_entidade(
                documento=args.buscar_documento,
                nome=args.buscar_nome,
                pipedrive_id=args.buscar_id
            )
            print(f"Encontradas {len(entidades)} entidades:")
            for entidade in entidades:
                print(f"  - {entidade['nome']} ({entidade['documento']}) - {entidade['tipo_pessoa']}")
        
        elif args.exportar_csv:
            sucesso = consulta.exportar_entidades_csv(args.exportar_csv)
            if sucesso:
                print(f"Dados exportados para: {args.exportar_csv}")
            else:
                print("Erro ao exportar dados")
        
        else:
            parser.print_help()
    
    except Exception as e:
        print(f"Erro: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main() 