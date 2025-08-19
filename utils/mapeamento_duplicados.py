"""
Utilitário para Mapeamento de Casos Duplicados no Pipedrive

Este script identifica e mapeia casos duplicados baseados em CPF/CNPJ,
gerando relatórios detalhados para avaliação manual.

Funcionalidades:
- Identifica pessoas duplicadas pelo documento
- Identifica negócios duplicados pelo documento
- Mapeia relacionamentos entre pessoas e negócios
- Gera relatórios Excel para análise
- Sugere ações de consolidação

USO: python utils_mapeamento_duplicados.py
"""

import sys
import os
import re
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple
from collections import defaultdict

# Adicionar o diretório pai ao path para importar módulos do projeto
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src'))

from src.pipedrive_client import PipedriveClient
from src.config import active_config
import logging

# Configurar logging com arquivo de log com timestamp
from src.config import active_config
logger = active_config.setup_logging('mapeamento_duplicados')

class DuplicateMapper:
    """
    Classe para mapear casos duplicados no Pipedrive
    """
    
    def __init__(self):
        self.pipedrive = PipedriveClient()
        self.stats = {
            'pessoas_analisadas': 0,
            'pessoas_duplicadas': 0,
            'grupos_pessoas_duplicadas': 0,
            'negocios_analisados': 0,
            'negocios_duplicados': 0,
            'grupos_negocios_duplicados': 0,
            'documentos_problematicos': 0,
            'erros': []
        }
        
        # Estruturas para armazenar dados
        self.pessoas_por_documento = defaultdict(list)
        self.negocios_por_documento = defaultdict(list)
        self.documentos_problematicos = []
        self.relatorios_gerados = []
    
    def executar_mapeamento_completo(self):
        """
        Executa mapeamento completo de duplicados
        """
        logger.info("=== INICIANDO MAPEAMENTO DE DUPLICADOS ===")
        
        # Criar diretórios necessários
        os.makedirs('logs', exist_ok=True)
        os.makedirs('relatorios', exist_ok=True)
        
        try:
            # 1. Mapear pessoas duplicadas
            logger.info("Etapa 1: Mapeando pessoas duplicadas...")
            self.mapear_pessoas_duplicadas()
            
            # 2. Mapear negócios duplicados
            logger.info("Etapa 2: Mapeando negócios duplicados...")
            self.mapear_negocios_duplicados()
            
            # 3. Gerar relatórios
            logger.info("Etapa 3: Gerando relatórios...")
            self.gerar_relatorios()
            
            # 4. Relatório final
            self.gerar_relatorio_final()
            
        except Exception as e:
            logger.error(f"Erro durante mapeamento: {e}")
            self.stats['erros'].append(str(e))
    
    def mapear_pessoas_duplicadas(self):
        """
        Mapeia pessoas duplicadas baseadas no documento
        """
        logger.info("Buscando todas as pessoas...")
        
        pessoas = self.pipedrive.get_all_persons()
        
        if not pessoas:
            logger.warning("Nenhuma pessoa encontrada")
            return
        
        logger.info(f"Encontradas {len(pessoas)} pessoas para análise")
        
        for pessoa in pessoas:
            self.stats['pessoas_analisadas'] += 1
            
            try:
                documento = self._extrair_documento_pessoa(pessoa)
                
                if documento:
                    # Normalizar documento
                    documento_normalizado = self._normalizar_documento(documento)
                    
                    if documento_normalizado:
                        # Adicionar informações detalhadas da pessoa
                        pessoa_info = {
                            'id': pessoa.get('id'),
                            'nome': pessoa.get('name', 'N/A'),
                            'documento_original': documento,
                            'documento_normalizado': documento_normalizado,
                            'email': pessoa.get('email', ''),
                            'telefone': pessoa.get('phone', ''),
                            'data_criacao': pessoa.get('add_time', ''),
                            'data_atualizacao': pessoa.get('update_time', ''),
                            'proprietario': pessoa.get('owner_name', ''),
                            'ativo': pessoa.get('active_flag', True),
                            'campos_personalizados': self._extrair_campos_personalizados_pessoa(pessoa)
                        }
                        
                        self.pessoas_por_documento[documento_normalizado].append(pessoa_info)
                    else:
                        self.documentos_problematicos.append({
                            'tipo': 'pessoa',
                            'id': pessoa.get('id'),
                            'nome': pessoa.get('name', 'N/A'),
                            'documento_original': documento,
                            'problema': 'documento_nao_normalizavel'
                        })
                        
            except Exception as e:
                error_msg = f"Erro ao processar pessoa {pessoa.get('id', 'N/A')}: {e}"
                logger.error(error_msg)
                self.stats['erros'].append(error_msg)
        
        # Contar duplicados
        for documento, pessoas_grupo in self.pessoas_por_documento.items():
            if len(pessoas_grupo) > 1:
                self.stats['grupos_pessoas_duplicadas'] += 1
                self.stats['pessoas_duplicadas'] += len(pessoas_grupo)
                
                logger.info(f"Documento {documento}: {len(pessoas_grupo)} pessoas duplicadas")
    
    def mapear_negocios_duplicados(self):
        """
        Mapeia negócios duplicados baseados no documento
        """
        logger.info("Buscando todos os negócios...")
        
        # Buscar negócios dos funis BASE NOVA
        negocios = []
        
        funis_para_verificar = [
            active_config.PIPELINE_BASE_NOVA_SDR_ID,
            active_config.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID,
            active_config.PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID
        ]
        
        for funil_id in funis_para_verificar:
            negocios_funil = self.pipedrive.get_deals_by_pipeline(funil_id)
            if negocios_funil:
                negocios.extend(negocios_funil)
        
        if not negocios:
            logger.warning("Nenhum negócio encontrado")
            return
        
        logger.info(f"Encontrados {len(negocios)} negócios para análise")
        
        for negocio in negocios:
            self.stats['negocios_analisados'] += 1
            
            try:
                documento = self._extrair_documento_negocio(negocio)
                
                if documento:
                    # Normalizar documento
                    documento_normalizado = self._normalizar_documento(documento)
                    
                    if documento_normalizado:
                        # Adicionar informações detalhadas do negócio
                        negocio_info = {
                            'id': negocio.get('id'),
                            'titulo': negocio.get('title', 'N/A'),
                            'documento_original': documento,
                            'documento_normalizado': documento_normalizado,
                            'valor': negocio.get('value', 0),
                            'status': negocio.get('status', ''),
                            'funil_id': negocio.get('pipeline_id'),
                            'funil_nome': self._get_pipeline_name(negocio.get('pipeline_id')),
                            'etapa_id': negocio.get('stage_id'),
                            'etapa_nome': negocio.get('stage_name', ''),
                            'pessoa_id': negocio.get('person_id'),
                            'pessoa_nome': negocio.get('person_name', ''),
                            'data_criacao': negocio.get('add_time', ''),
                            'data_atualizacao': negocio.get('update_time', ''),
                            'proprietario': negocio.get('owner_name', ''),
                            'campos_personalizados': self._extrair_campos_personalizados_negocio(negocio)
                        }
                        
                        self.negocios_por_documento[documento_normalizado].append(negocio_info)
                    else:
                        self.documentos_problematicos.append({
                            'tipo': 'negocio',
                            'id': negocio.get('id'),
                            'titulo': negocio.get('title', 'N/A'),
                            'documento_original': documento,
                            'problema': 'documento_nao_normalizavel'
                        })
                        
            except Exception as e:
                error_msg = f"Erro ao processar negócio {negocio.get('id', 'N/A')}: {e}"
                logger.error(error_msg)
                self.stats['erros'].append(error_msg)
        
        # Contar duplicados
        for documento, negocios_grupo in self.negocios_por_documento.items():
            if len(negocios_grupo) > 1:
                self.stats['grupos_negocios_duplicados'] += 1
                self.stats['negocios_duplicados'] += len(negocios_grupo)
                
                logger.info(f"Documento {documento}: {len(negocios_grupo)} negócios duplicados")
    
    def _extrair_documento_pessoa(self, pessoa: Dict) -> Optional[str]:
        """
        Extrai documento de uma pessoa
        """
        # Campos que podem conter documento
        campos_documento = [
            'cpf_cnpj',
            'custom_field_cpf_cnpj',
            'cpf',
            'cnpj',
            'documento'
        ]
        
        for campo in campos_documento:
            valor = pessoa.get(campo)
            if valor:
                return str(valor).strip()
        
        return None
    
    def _extrair_documento_negocio(self, negocio: Dict) -> Optional[str]:
        """
        Extrai documento de um negócio
        """
        # 1. Tentar extrair dos campos personalizados
        campos_documento = [
            'ID_CPF_CNPJ',
            'cpf_cnpj',
            'custom_field_cpf_cnpj'
        ]
        
        for campo in campos_documento:
            valor = negocio.get(campo)
            if valor:
                return str(valor).strip()
        
        # 2. Tentar extrair do título
        titulo = negocio.get('title', '')
        if titulo:
            documento_titulo = self._extrair_documento_titulo(titulo)
            if documento_titulo:
                return documento_titulo
        
        return None
    
    def _extrair_documento_titulo(self, titulo: str) -> Optional[str]:
        """
        Extrai documento do título
        """
        if not titulo:
            return None
        
        # Buscar padrões de CPF/CNPJ no título
        cpf_pattern = r'\b\d{8,11}\b'
        cnpj_pattern = r'\b\d{12,14}\b'
        
        # Buscar CNPJ primeiro (mais específico)
        cnpj_match = re.search(cnpj_pattern, titulo)
        if cnpj_match:
            return cnpj_match.group()
        
        # Buscar CPF
        cpf_match = re.search(cpf_pattern, titulo)
        if cpf_match:
            return cpf_match.group()
        
        return None
    
    def _normalizar_documento(self, documento: str) -> Optional[str]:
        """
        Normaliza documento removendo formatação e adicionando zeros à esquerda
        """
        if not documento:
            return None
        
        # Limpar documento
        documento_limpo = re.sub(r'[^\d]', '', documento)
        
        if not documento_limpo:
            return None
        
        # Determinar se é CPF ou CNPJ e normalizar
        if len(documento_limpo) <= 11:
            # CPF
            return documento_limpo.zfill(11)
        elif len(documento_limpo) <= 14:
            # CNPJ
            return documento_limpo.zfill(14)
        
        return documento_limpo
    
    def _extrair_campos_personalizados_pessoa(self, pessoa: Dict) -> Dict:
        """
        Extrai campos personalizados de uma pessoa
        """
        campos_relevantes = {}
        
        # Lista de campos que podem ser relevantes
        campos_interesse = [
            'telefone_cadastro', 'data_nascimento', 'idade', 'estado_civil',
            'condicao_cpf', 'rg', 'nome_mae', 'conjuge', 'nacionalidade'
        ]
        
        for campo in campos_interesse:
            valor = pessoa.get(campo)
            if valor:
                campos_relevantes[campo] = str(valor)
        
        return campos_relevantes
    
    def _extrair_campos_personalizados_negocio(self, negocio: Dict) -> Dict:
        """
        Extrai campos personalizados de um negócio
        """
        campos_relevantes = {}
        
        # Lista de campos que podem ser relevantes
        campos_interesse = [
            'COOPERADO', 'COOPERATIVA', 'TODOS_CONTRATOS', 'CONTRATO GARANTINORTE',
            'DIAS_DE_ATRASO', 'VALOR_TOTAL_DA_DIVIDA', 'CONDICAO_CPF', 'Avalistas'
        ]
        
        for campo in campos_interesse:
            valor = negocio.get(campo)
            if valor:
                campos_relevantes[campo] = str(valor)
        
        return campos_relevantes
    
    def _get_pipeline_name(self, pipeline_id: int) -> str:
        """
        Obtém nome do funil baseado no ID
        """
        if pipeline_id == active_config.PIPELINE_BASE_NOVA_SDR_ID:
            return "BASE NOVA - SDR"
        elif pipeline_id == active_config.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID:
            return "BASE NOVA - NEGOCIAÇÃO"
        elif pipeline_id == active_config.PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID:
            return "BASE NOVA - FORMALIZAÇÃO/PAGAMENTO"
        else:
            return f"Funil ID {pipeline_id}"
    
    def gerar_relatorios(self):
        """
        Gera relatórios Excel detalhados
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Relatório de pessoas duplicadas
        if self.pessoas_por_documento:
            self._gerar_relatorio_pessoas_duplicadas(timestamp)
        
        # 2. Relatório de negócios duplicados
        if self.negocios_por_documento:
            self._gerar_relatorio_negocios_duplicados(timestamp)
        
        # 3. Relatório de documentos problemáticos
        if self.documentos_problematicos:
            self._gerar_relatorio_documentos_problematicos(timestamp)
        
        # 4. Relatório consolidado
        self._gerar_relatorio_consolidado(timestamp)
    
    def _gerar_relatorio_pessoas_duplicadas(self, timestamp: str):
        """
        Gera relatório de pessoas duplicadas
        """
        dados_pessoas = []
        
        for documento, pessoas_grupo in self.pessoas_por_documento.items():
            if len(pessoas_grupo) > 1:
                for i, pessoa in enumerate(pessoas_grupo):
                    dados_pessoas.append({
                        'Documento': documento,
                        'Grupo': f"Grupo_{documento}",
                        'Ordem_no_Grupo': i + 1,
                        'Total_no_Grupo': len(pessoas_grupo),
                        'ID_Pessoa': pessoa['id'],
                        'Nome': pessoa['nome'],
                        'Email': pessoa['email'],
                        'Telefone': pessoa['telefone'],
                        'Data_Criacao': pessoa['data_criacao'],
                        'Data_Atualizacao': pessoa['data_atualizacao'],
                        'Proprietario': pessoa['proprietario'],
                        'Ativo': pessoa['ativo'],
                        'Documento_Original': pessoa['documento_original'],
                        'Campos_Personalizados': str(pessoa['campos_personalizados']),
                        'Sugestao_Acao': 'MANTER' if i == 0 else 'AVALIAR_MERGE'
                    })
        
        if dados_pessoas:
            df = pd.DataFrame(dados_pessoas)
            arquivo_path = f'relatorios/pessoas_duplicadas_{timestamp}.xlsx'
            df.to_excel(arquivo_path, index=False)
            self.relatorios_gerados.append(arquivo_path)
            logger.info(f"Relatório de pessoas duplicadas salvo: {arquivo_path}")
    
    def _gerar_relatorio_negocios_duplicados(self, timestamp: str):
        """
        Gera relatório de negócios duplicados
        """
        dados_negocios = []
        
        for documento, negocios_grupo in self.negocios_por_documento.items():
            if len(negocios_grupo) > 1:
                for i, negocio in enumerate(negocios_grupo):
                    dados_negocios.append({
                        'Documento': documento,
                        'Grupo': f"Grupo_{documento}",
                        'Ordem_no_Grupo': i + 1,
                        'Total_no_Grupo': len(negocios_grupo),
                        'ID_Negocio': negocio['id'],
                        'Titulo': negocio['titulo'],
                        'Valor': negocio['valor'],
                        'Status': negocio['status'],
                        'Funil': negocio['funil_nome'],
                        'Etapa': negocio['etapa_nome'],
                        'Pessoa_ID': negocio['pessoa_id'],
                        'Pessoa_Nome': negocio['pessoa_nome'],
                        'Data_Criacao': negocio['data_criacao'],
                        'Data_Atualizacao': negocio['data_atualizacao'],
                        'Proprietario': negocio['proprietario'],
                        'Documento_Original': negocio['documento_original'],
                        'Campos_Personalizados': str(negocio['campos_personalizados']),
                        'Sugestao_Acao': 'MANTER' if i == 0 else 'AVALIAR_MERGE_OU_LOST'
                    })
        
        if dados_negocios:
            df = pd.DataFrame(dados_negocios)
            arquivo_path = f'relatorios/negocios_duplicados_{timestamp}.xlsx'
            df.to_excel(arquivo_path, index=False)
            self.relatorios_gerados.append(arquivo_path)
            logger.info(f"Relatório de negócios duplicados salvo: {arquivo_path}")
    
    def _gerar_relatorio_documentos_problematicos(self, timestamp: str):
        """
        Gera relatório de documentos problemáticos
        """
        if self.documentos_problematicos:
            df = pd.DataFrame(self.documentos_problematicos)
            arquivo_path = f'relatorios/documentos_problematicos_{timestamp}.xlsx'
            df.to_excel(arquivo_path, index=False)
            self.relatorios_gerados.append(arquivo_path)
            logger.info(f"Relatório de documentos problemáticos salvo: {arquivo_path}")
    
    def _gerar_relatorio_consolidado(self, timestamp: str):
        """
        Gera relatório consolidado com estatísticas
        """
        dados_consolidado = []
        
        # Estatísticas gerais
        dados_consolidado.append({
            'Metrica': 'Pessoas Analisadas',
            'Valor': self.stats['pessoas_analisadas']
        })
        
        dados_consolidado.append({
            'Metrica': 'Grupos de Pessoas Duplicadas',
            'Valor': self.stats['grupos_pessoas_duplicadas']
        })
        
        dados_consolidado.append({
            'Metrica': 'Total de Pessoas Duplicadas',
            'Valor': self.stats['pessoas_duplicadas']
        })
        
        dados_consolidado.append({
            'Metrica': 'Negócios Analisados',
            'Valor': self.stats['negocios_analisados']
        })
        
        dados_consolidado.append({
            'Metrica': 'Grupos de Negócios Duplicados',
            'Valor': self.stats['grupos_negocios_duplicados']
        })
        
        dados_consolidado.append({
            'Metrica': 'Total de Negócios Duplicados',
            'Valor': self.stats['negocios_duplicados']
        })
        
        dados_consolidado.append({
            'Metrica': 'Documentos Problemáticos',
            'Valor': len(self.documentos_problematicos)
        })
        
        df = pd.DataFrame(dados_consolidado)
        arquivo_path = f'relatorios/relatorio_consolidado_{timestamp}.xlsx'
        df.to_excel(arquivo_path, index=False)
        self.relatorios_gerados.append(arquivo_path)
        logger.info(f"Relatório consolidado salvo: {arquivo_path}")
    
    def gerar_relatorio_final(self):
        """
        Gera relatório final do mapeamento
        """
        logger.info("=== RELATÓRIO FINAL DO MAPEAMENTO ===")
        logger.info(f"Pessoas analisadas: {self.stats['pessoas_analisadas']}")
        logger.info(f"Grupos de pessoas duplicadas: {self.stats['grupos_pessoas_duplicadas']}")
        logger.info(f"Total de pessoas duplicadas: {self.stats['pessoas_duplicadas']}")
        logger.info(f"Negócios analisados: {self.stats['negocios_analisados']}")
        logger.info(f"Grupos de negócios duplicados: {self.stats['grupos_negocios_duplicados']}")
        logger.info(f"Total de negócios duplicados: {self.stats['negocios_duplicados']}")
        logger.info(f"Documentos problemáticos: {len(self.documentos_problematicos)}")
        logger.info(f"Erros encontrados: {len(self.stats['erros'])}")
        
        if self.relatorios_gerados:
            logger.info("Relatórios gerados:")
            for relatorio in self.relatorios_gerados:
                logger.info(f"  - {relatorio}")
        
        if self.stats['erros']:
            logger.info("Erros:")
            for erro in self.stats['erros']:
                logger.info(f"  - {erro}")

def main():
    """
    Função principal
    """
    print("=== MAPEAMENTO DE CASOS DUPLICADOS NO PIPEDRIVE ===")
    print("Este script identifica e mapeia casos duplicados baseados em CPF/CNPJ,")
    print("gerando relatórios Excel detalhados para avaliação manual.\n")
    
    resposta = input("Deseja continuar? (s/n): ").strip().lower()
    
    if resposta != 's':
        print("Operação cancelada.")
        return
    
    mapper = DuplicateMapper()
    mapper.executar_mapeamento_completo()
    
    print("\n=== MAPEAMENTO FINALIZADO ===")
    print("Verifique os relatórios Excel gerados na pasta 'relatorios/' para análise detalhada.")

if __name__ == "__main__":
    main() 