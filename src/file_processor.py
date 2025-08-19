"""
Processador de arquivos para o sistema Pipedrive
Inclui processamento direto do TXT do banco
"""
import os
import re
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from config import active_config
from custom_fields_config import CustomFieldsConfig

logger = logging.getLogger(__name__)

class FileProcessor:
    def __init__(self):
        self.ensure_directories()
        
        # Estrutura dos campos do TXT do banco
        self.txt_fields = {
            '01': {
                'Código do registro': {'begin': 1, 'end': 2},
                'CPF/CNPJ': {'begin': 3, 'end': 15},
                'Nome': {'begin': 18, 'end': 80},
                'Data de nascimento': {'begin': 98, 'end': 8, 'format': lambda x: self._format_date(x)},
                'Endereço': {'begin': 106, 'end': 70},
                'Bairro': {'begin': 176, 'end': 30},
                'Município': {'begin': 206, 'end': 30},
                'UF': {'begin': 236, 'end': 2},
                'CEP': {'begin': 238, 'end': 9},
                'DDD1': {'begin': 247, 'end': 4},
                'Telefone1': {'begin': 251, 'end': 9},
                'DDD2': {'begin': 260, 'end': 4},
                'Telefone2': {'begin': 264, 'end': 9},
                'DDD3': {'begin': 273, 'end': 4},
                'Telefone3': {'begin': 277, 'end': 9},
                'DDD4': {'begin': 286, 'end': 4},
                'Telefone4': {'begin': 290, 'end': 9},
                'Email1': {'begin': 299, 'end': 45},
                'Email2': {'begin': 344, 'end': 45},
                'RG': {'begin': 389, 'end': 30},
                'Data_Emissao_RG': {'begin': 419, 'end': 8, 'format': lambda x: self._format_date(x)},
                'Orgao_Emissor_RG': {'begin': 427, 'end': 20},
                'UF_RG': {'begin': 447, 'end': 2},
                'Nome_Mae': {'begin': 449, 'end': 100},
                'Estado_Civil': {'begin': 549, 'end': 60},
                'Nome_Conjuge': {'begin': 609, 'end': 100},
                'Nacionalidade': {'begin': 709, 'end': 100},
                'ID_PAC': {'begin': 809, 'end': 10},
                'Tipo_Pessoa': {'begin': 819, 'end': 2},
                'Sequencia': {'begin': 1494, 'end': 7}
            },
            '05': {
                'Código do registro': {'begin': 1, 'end': 2},
                'Nome_Referencia': {'begin': 3, 'end': 100},
                'Tipo_Referencia': {'begin': 103, 'end': 50},
                'DDD_Ref': {'begin': 153, 'end': 4},
                'Telefone_Ref': {'begin': 157, 'end': 9},
                'Sequencia': {'begin': 1494, 'end': 7}
            },
            '10': {
                'Código do registro': {'begin': 1, 'end': 2},
                'Numero_Contrato': {'begin': 3, 'end': 12, 'format': lambda x: self._format_contract_number(x)},
                'Responsabilidade': {'begin': 15, 'end': 50},
                'Produto': {'begin': 65, 'end': 50},
                'Carteira': {'begin': 115, 'end': 50},
                'Valor_Operacao': {'begin': 165, 'end': 15, 'format': lambda x: self._format_money(x)},
                'Valor_IOF': {'begin': 180, 'end': 15, 'format': lambda x: self._format_money(x)},
                'Restricao_Serasa': {'begin': 195, 'end': 1},
                'Restricao_SPC': {'begin': 196, 'end': 1},
                'Restricao_Boa_Vista': {'begin': 197, 'end': 1},
                'Taxa_Juros': {'begin': 198, 'end': 11, 'format': lambda x: self._format_percent(x)},
                'Taxa_Mora': {'begin': 209, 'end': 11, 'format': lambda x: self._format_percent(x)},
                'Taxa_Multa': {'begin': 220, 'end': 11, 'format': lambda x: self._format_percent(x)},
                'Valor_Contabil': {'begin': 231, 'end': 17, 'format': lambda x: self._format_money(x)},
                'Total_Parcelas': {'begin': 248, 'end': 4},
                'Data_Contratacao': {'begin': 252, 'end': 8, 'format': lambda x: self._format_date(x)},
                'ID_Operacao': {'begin': 260, 'end': 19},
                'ID_Operacao_Cobranca': {'begin': 279, 'end': 20},
                'ID_Operacao_Anterior': {'begin': 299, 'end': 20},
                'Sequencia': {'begin': 1494, 'end': 7}
            },
            '15': {
                'Código do registro': {'begin': 1, 'end': 2},
                'Numero_Parcela': {'begin': 3, 'end': 4},
                'Data_Vencimento': {'begin': 7, 'end': 8, 'format': lambda x: self._format_date(x)},
                'Dias_Atraso': {'begin': 15, 'end': 4},
                'Valor_Parcela': {'begin': 19, 'end': 15, 'format': lambda x: self._format_money(x)},
                'Valor_Multa': {'begin': 34, 'end': 15, 'format': lambda x: self._format_money(x)},
                'Valor_Mora': {'begin': 49, 'end': 15, 'format': lambda x: self._format_money(x)},
                'Valor_Outros_Contratado': {'begin': 64, 'end': 15, 'format': lambda x: self._format_money(x)},
                'Valor_Outros_Atraso': {'begin': 79, 'end': 15, 'format': lambda x: self._format_money(x)},
                'Valor_Juros_Contratado': {'begin': 94, 'end': 15, 'format': lambda x: self._format_money(x)},
                'Valor_Juros_Atraso': {'begin': 109, 'end': 15, 'format': lambda x: self._format_money(x)},
                'Valor_Atualizado': {'begin': 124, 'end': 15, 'format': lambda x: self._format_money(x)},
                'Restricao_Serasa': {'begin': 139, 'end': 1},
                'Restricao_SPC': {'begin': 140, 'end': 1},
                'Restricao_Boa_Vista': {'begin': 141, 'end': 1},
                'ID_Operacao': {'begin': 142, 'end': 20},
                'Situacao_Parcela': {'begin': 162, 'end': 2},
                'Sequencia': {'begin': 1494, 'end': 7}
            },
            '20': {
                'Código do registro': {'begin': 1, 'end': 2},
                'CPF_CNPJ_Participante': {'begin': 3, 'end': 15},
                'Nome_Participante': {'begin': 18, 'end': 80},
                'Data_Nascimento_Participante': {'begin': 98, 'end': 8, 'format': lambda x: self._format_date(x)},
                'Endereco_Participante': {'begin': 106, 'end': 70},
                'Bairro_Participante': {'begin': 176, 'end': 30},
                'Municipio_Participante': {'begin': 206, 'end': 30},
                'UF_Participante': {'begin': 236, 'end': 2},
                'CEP_Participante': {'begin': 238, 'end': 9},
                'DDD1_Participante': {'begin': 247, 'end': 4},
                'Telefone1_Participante': {'begin': 251, 'end': 9},
                'DDD2_Participante': {'begin': 260, 'end': 4},
                'Telefone2_Participante': {'begin': 264, 'end': 9},
                'DDD3_Participante': {'begin': 273, 'end': 4},
                'Telefone3_Participante': {'begin': 277, 'end': 9},
                'DDD4_Participante': {'begin': 286, 'end': 4},
                'Telefone4_Participante': {'begin': 290, 'end': 9},
                'Email1_Participante': {'begin': 299, 'end': 45},
                'Email2_Participante': {'begin': 344, 'end': 45},
                'Responsabilidade_Participante': {'begin': 389, 'end': 50},
                'Restricao_Serasa_Participante': {'begin': 439, 'end': 1},
                'Restricao_SPC_Participante': {'begin': 440, 'end': 1},
                'Restricao_Boa_Vista_Participante': {'begin': 441, 'end': 1},
                'Tipo_Pessoa_Participante': {'begin': 442, 'end': 2},
                'Sequencia': {'begin': 1494, 'end': 7}
            }
        }
    
    def ensure_directories(self):
        """Cria diretórios necessários se não existirem"""
        directories = [
            active_config.TXT_INPUT_FOLDER,
            active_config.EXCEL_OUTPUT_FOLDER,
            active_config.GARANTINORTE_FOLDER,
            active_config.BACKUP_FOLDER,
            './logs'
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
    
    def process_txt_file_direct(self, txt_file_path: str) -> List[Dict]:
        """
        Processa arquivo TXT diretamente extraindo dados para o Pipedrive
        Inclui processamento da planilha GARANTINORTE para cruzamento de dados
        Retorna lista de devedores com todas as informações consolidadas
        """
        logger.info(f"Processando arquivo TXT: {txt_file_path}")
        
        try:
            # 1. Carregar dados da GARANTINORTE
            garantinorte_data = self.load_garantinorte_data()
            
            # 2. Ler arquivo TXT
            with open(txt_file_path, 'r', encoding='latin-1') as file:
                lines = file.readlines()
            
            # 3. Processar blocos
            blocks = self._parse_txt_blocks(lines)
            
            # 4. Consolidar dados para Pipedrive (incluindo dados GARANTINORTE)
            consolidated_data = self._consolidate_blocks_for_pipedrive(blocks, garantinorte_data)
            
            logger.info(f"Processamento concluído. {len(consolidated_data)} devedores encontrados.")
            return consolidated_data
            
        except Exception as e:
            logger.error(f"Erro ao processar arquivo TXT: {e}")
            raise
    
    def _parse_txt_blocks(self, lines: List[str]) -> List[Dict]:
        """
        Divide o arquivo TXT em blocos por devedor
        """
        blocks = []
        current_block = []
        
        for line in lines:
            line = line.rstrip('\n\r')
            
            # Pular linhas de header/footer
            if line.startswith('00') or line.startswith('99'):
                continue
                
            # Início de novo bloco (registro 01)
            if line.startswith('01'):
                if current_block:
                    # Processar bloco anterior
                    block_data = self._parse_single_block(current_block)
                    if block_data:
                        blocks.append(block_data)
                
                # Iniciar novo bloco
                current_block = [line]
            else:
                # Adicionar linha ao bloco atual
                if current_block:
                    current_block.append(line)
        
        # Processar último bloco
        if current_block:
            block_data = self._parse_single_block(current_block)
            if block_data:
                blocks.append(block_data)
        
        return blocks
    
    def _parse_single_block(self, block_lines: List[str]) -> Dict:
        """
        Processa um bloco individual (um devedor)
        """
        block_data = {
            '01': [],  # Devedor principal
            '05': [],  # Referências
            '10': [],  # Operações
            '15': [],  # Parcelas
            '20': [],  # Participantes/Avalistas
            '25': []   # Garantias
        }
        
        devedor_cpf_cnpj = None
        
        for line in block_lines:
            record_type = line[:2]
            
            if record_type in self.txt_fields:
                parsed_record = self._parse_line(line, record_type)
                
                # Capturar CPF/CNPJ do devedor principal
                if record_type == '01':
                    devedor_cpf_cnpj = parsed_record.get('CPF/CNPJ', '')
                
                # Adicionar CPF/CNPJ do devedor a todos os registros
                parsed_record['devedor_cpf_cnpj'] = devedor_cpf_cnpj
                
                block_data[record_type].append(parsed_record)
        
        return block_data
    
    def _parse_line(self, line: str, record_type: str) -> Dict:
        """
        Extrai campos de uma linha específica
        """
        parsed_data = {}
        fields = self.txt_fields[record_type]
        
        for field_name, field_config in fields.items():
            start_pos = field_config['begin'] - 1
            end_pos = start_pos + field_config['end']
            
            raw_value = line[start_pos:end_pos].strip()
            
            # Aplicar formatação se especificada
            if 'format' in field_config and raw_value:
                try:
                    formatted_value = field_config['format'](raw_value)
                    parsed_data[field_name] = formatted_value
                except:
                    parsed_data[field_name] = raw_value
            else:
                parsed_data[field_name] = raw_value
        
        return parsed_data
    
    def _consolidate_blocks_for_pipedrive(self, blocks: List[Dict], garantinorte_data: Dict[str, str] = None) -> List[Dict]:
        """
        Consolida dados dos blocos para formato do Pipedrive
        Inclui informações de contrato GARANTINORTE quando disponíveis
        
        Args:
            blocks: Lista de blocos de dados processados
            garantinorte_data: Dicionário com dados da GARANTINORTE {cpf_cnpj: contrato}
        """
        pipedrive_data = []
        
        if garantinorte_data is None:
            garantinorte_data = {}
        
        for block in blocks:
            # Dados do devedor principal (registro 01)
            if not block['01']:
                continue
                
            devedor = block['01'][0]
            
            # CORREÇÃO: Processar apenas devedores principais (registros tipo '01')
            # Não processar avalistas (registros tipo '20') como entidades separadas
            tipo_pessoa_codigo = devedor.get('Tipo_Pessoa', '').strip()
            tipo_pessoa = self._map_tipo_pessoa_from_txt(tipo_pessoa_codigo)
            
            # Verificar se é um devedor principal válido
            if not tipo_pessoa or tipo_pessoa == 'INDEFINIDO':
                logger.warning(f"Tipo de pessoa inválido para devedor: {tipo_pessoa_codigo}")
                continue
            
            # Buscar contrato GARANTINORTE para este devedor
            cpf_cnpj_devedor_original = devedor.get('CPF/CNPJ', '')
            cpf_cnpj_devedor_limpo = self._clean_document(cpf_cnpj_devedor_original)
            
            # Normalizar documento baseado no tipo de pessoa
            cpf_cnpj_devedor_normalizado = self._normalize_document_by_type(cpf_cnpj_devedor_original, tipo_pessoa)
            
            # CORREÇÃO: Verificar se o documento é válido
            if not cpf_cnpj_devedor_normalizado:
                logger.warning(f"Documento inválido para devedor: {cpf_cnpj_devedor_original}")
                continue
            
            contrato_garantinorte = self.get_garantinorte_contract(cpf_cnpj_devedor_limpo, garantinorte_data)
            
            # Consolidar informações para o Pipedrive
            consolidated = {
                # Dados básicos - usar documento normalizado
                'cpf_cnpj': cpf_cnpj_devedor_normalizado,
                'nome': devedor.get('Nome', ''),
                'tipo_pessoa': tipo_pessoa,
                
                # Dados de contato
                'telefones': self._extract_phones(devedor),
                'emails': self._extract_emails(devedor),
                'endereco_completo': self._build_address(devedor),
                
                # Dados financeiros consolidados
                'valor_total_divida': self._calculate_total_debt(block),
                'valor_total_vencido': self._calculate_overdue_debt(block),
                'valor_total_com_juros': self._calculate_total_with_interest(block),
                'dias_atraso_maximo': self._calculate_max_overdue_days(block),
                'vencimento_mais_antigo': self._find_oldest_due_date(block),
                
                # Dados contratuais
                'todos_contratos': self._extract_all_contracts(block),
                'todas_operacoes': self._extract_all_operations(block),
                'numero_contrato': self._extract_main_contract(block),
                'tipo_acao_carteira': self._extract_portfolio_type(block),
                'total_parcelas': self._extract_total_installments(block),
                
                # Situação de crédito
                'condicao_cpf': self._determine_credit_condition(block),
                'tag_atraso': self._determine_delay_tag(block),
                
                # Campos específicos solicitados
                'cooperado': devedor.get('Nome', ''),  # Nome do devedor principal
                'cooperativa': 'OURO VERDE',  # Valor fixo
                'id_cpf_cnpj': int(cpf_cnpj_devedor_normalizado) if cpf_cnpj_devedor_normalizado else 0,
                
                # Contrato GARANTINORTE
                'contrato_garantinorte': contrato_garantinorte,
                
                # Avalistas (será processado separadamente)
                'avalistas_info': self._extract_avalistas_info(block),
                
                # Dados adicionais para pessoa
                'data_nascimento': devedor.get('Data de nascimento', ''),
                'nome_mae': devedor.get('Nome_Mae', ''),
                'estado_civil': devedor.get('Estado_Civil', ''),
                'rg': devedor.get('RG', ''),
                'nacionalidade': devedor.get('Nacionalidade', ''),
                
                # Dados brutos para acesso completo aos campos
                'raw_block': block
            }
            
            pipedrive_data.append(consolidated)
        
        return pipedrive_data
    
    def _clean_document(self, document: str) -> str:
        """Remove formatação do documento"""
        if not document:
            return ''
        return ''.join(filter(str.isdigit, document))
    
    def _normalize_document_by_type(self, document: str, person_type: str) -> str:
        """
        Normaliza documento baseado no tipo de pessoa
        
        Args:
            document: Documento original (15 dígitos com zeros à esquerda)
            person_type: 'PF' ou 'PJ'
            
        Returns:
            Documento normalizado (11 dígitos para PF, 14 para PJ)
        """
        # Limpar documento (só números)
        clean_doc = self._clean_document(document)
        
        if not clean_doc:
            return ''
        
        # Determinar tamanho correto baseado no tipo
        if person_type == 'PF':
            # CPF deve ter 11 dígitos
            target_length = 11
        elif person_type == 'PJ':
            # CNPJ deve ter 14 dígitos
            target_length = 14
        else:
            # Tipo indefinido, usar documento limpo
            return clean_doc
        
        # Extrair documento com tamanho correto
        if len(clean_doc) >= target_length:
            # Pegar os últimos N dígitos (tamanho correto)
            return clean_doc[-target_length:]
        else:
            # Documento muito curto, preencher com zeros à esquerda
            return clean_doc.zfill(target_length)
    
    def _determine_person_type(self, cpf_cnpj: str) -> str:
        """Determina se é PF ou PJ baseado no tamanho do documento"""
        clean_doc = self._clean_document(cpf_cnpj)
        if len(clean_doc) == 11:
            return 'PF'
        elif len(clean_doc) == 14:
            return 'PJ'
        return 'INDEFINIDO'
    
    def _map_tipo_pessoa_from_txt(self, tipo_pessoa_codigo: str) -> str:
        """
        Mapeia código do campo Tipo_Pessoa do TXT para PF/PJ
        
        Args:
            tipo_pessoa_codigo: Código do campo Tipo_Pessoa (ex: '01', '02', 'PF', 'PJ')
            
        Returns:
            'PF', 'PJ' ou 'INDEFINIDO'
        """
        # Mapeamento para códigos numéricos e strings literais
        mapeamento = {
            # Códigos numéricos
            '01': 'PF',  # Pessoa Física
            '02': 'PJ',  # Pessoa Jurídica
            '1': 'PF',   # Variação sem zero à esquerda
            '2': 'PJ',   # Variação sem zero à esquerda
            # Strings literais (já no formato correto)
            'PF': 'PF',  # Pessoa Física literal
            'PJ': 'PJ',  # Pessoa Jurídica literal
            'F': 'PF',   # Variação abreviada
            'J': 'PJ',   # Variação abreviada
        }
        
        codigo_limpo = tipo_pessoa_codigo.strip().upper()
        tipo = mapeamento.get(codigo_limpo, 'INDEFINIDO')
        
        # Log para debug quando código não é reconhecido
        if tipo == 'INDEFINIDO' and codigo_limpo:
            logger.warning(f"Código de tipo pessoa não reconhecido: '{codigo_limpo}'")
        
        return tipo
    
    def _extract_phones(self, devedor: Dict) -> List[str]:
        """Extrai todos os telefones válidos"""
        phones = []
        for i in range(1, 5):
            ddd = devedor.get(f'DDD{i}', '').strip()
            phone = devedor.get(f'Telefone{i}', '').strip()
            if ddd and phone and ddd != '0000' and phone != '000000000':
                # CORREÇÃO: Remover zeros à esquerda do DDD e telefone
                ddd_limpo = ddd.lstrip('0') or '0'
                phone_limpo = phone.lstrip('0') or '0'
                phones.append(f"({ddd_limpo}) {phone_limpo}")
        return phones
    
    def _extract_emails(self, devedor: Dict) -> List[str]:
        """Extrai emails válidos"""
        emails = []
        for i in range(1, 3):
            email = devedor.get(f'Email{i}', '').strip()
            if email and '@' in email:
                emails.append(email)
        return emails
    
    def _build_address(self, devedor: Dict) -> str:
        """Constrói endereço completo"""
        endereco = devedor.get('Endereço', '').strip()
        bairro = devedor.get('Bairro', '').strip()
        municipio = devedor.get('Município', '').strip()
        uf = devedor.get('UF', '').strip()
        cep = devedor.get('CEP', '').strip()
        
        address_parts = [p for p in [endereco, bairro, municipio, uf, cep] if p]
        return ', '.join(address_parts)
    
    def _calculate_total_debt(self, block: Dict) -> float:
        """Calcula valor total da dívida"""
        total = 0.0
        for operacao in block['10']:
            valor = operacao.get('Valor_Contabil', 0)
            if isinstance(valor, (int, float)):
                total += valor
        return total
    
    def _calculate_overdue_debt(self, block: Dict) -> float:
        """Calcula valor total vencido"""
        total = 0.0
        today = datetime.now()
        
        for parcela in block['15']:
            data_venc = parcela.get('Data_Vencimento', '')
            if data_venc:
                try:
                    venc_date = datetime.strptime(data_venc, '%d/%m/%Y')
                    if venc_date < today:
                        valor = parcela.get('Valor_Atualizado', 0)
                        if isinstance(valor, (int, float)):
                            total += valor
                except:
                    pass
        return total
    
    def _calculate_total_with_interest(self, block: Dict) -> float:
        """Calcula valor total com juros"""
        total = 0.0
        for parcela in block['15']:
            valor = parcela.get('Valor_Atualizado', 0)
            if isinstance(valor, (int, float)):
                total += valor
        return total
    
    def _calculate_max_overdue_days(self, block: Dict) -> int:
        """Calcula maior número de dias em atraso"""
        max_days = 0
        for parcela in block['15']:
            dias = parcela.get('Dias_Atraso', 0)
            if isinstance(dias, (int, str)):
                try:
                    dias_int = int(dias)
                    max_days = max(max_days, dias_int)
                except:
                    pass
        return max_days
    
    def _find_oldest_due_date(self, block: Dict) -> str:
        """Encontra data de vencimento mais antiga"""
        oldest_date = None
        
        for parcela in block['15']:
            data_venc = parcela.get('Data_Vencimento', '')
            if data_venc:
                try:
                    venc_date = datetime.strptime(data_venc, '%d/%m/%Y')
                    if oldest_date is None or venc_date < oldest_date:
                        oldest_date = venc_date
                except:
                    pass
        
        return oldest_date.strftime('%d/%m/%Y') if oldest_date else ''
    
    def _extract_all_contracts(self, block: Dict) -> str:
        """Extrai todos os contratos"""
        contracts = []
        for operacao in block['10']:
            contrato = operacao.get('Numero_Contrato', '').strip()
            if contrato:
                # CORREÇÃO: Remover zeros à esquerda do número do contrato
                contrato_limpo = contrato.lstrip('0') or '0'
                contracts.append(contrato_limpo)
        return '; '.join(contracts)
    
    def _extract_all_operations(self, block: Dict) -> str:
        """Extrai todas as operações"""
        operations = []
        for operacao in block['10']:
            op_id = operacao.get('ID_Operacao', '').strip()
            if op_id:
                # CORREÇÃO: Remover zeros à esquerda do ID da operação
                op_id_limpo = op_id.lstrip('0') or '0'
                operations.append(op_id_limpo)
        return '; '.join(operations)
    
    def _extract_main_contract(self, block: Dict) -> str:
        """Extrai contrato principal"""
        if block['10']:
            contrato = block['10'][0].get('Numero_Contrato', '').strip()
            # CORREÇÃO: Remover zeros à esquerda do número do contrato
            return contrato.lstrip('0') or '0' if contrato else ''
        return ''
    
    def _extract_portfolio_type(self, block: Dict) -> str:
        """Extrai tipo de carteira"""
        if block['10']:
            return block['10'][0].get('Carteira', '').strip()
        return ''
    
    def _extract_total_installments(self, block: Dict) -> str:
        """Extrai total de parcelas"""
        if block['10']:
            return str(block['10'][0].get('Total_Parcelas', ''))
        return ''
    
    def _determine_credit_condition(self, block: Dict) -> str:
        """Determina condição do CPF"""
        has_restriction = False
        
        # Verificar restrições nas operações
        for operacao in block['10']:
            if (operacao.get('Restricao_Serasa') == '1' or 
                operacao.get('Restricao_SPC') == '1' or 
                operacao.get('Restricao_Boa_Vista') == '1'):
                has_restriction = True
                break
        
        # Verificar restrições nas parcelas
        if not has_restriction:
            for parcela in block['15']:
                if (parcela.get('Restricao_Serasa') == '1' or 
                    parcela.get('Restricao_SPC') == '1' or 
                    parcela.get('Restricao_Boa_Vista') == '1'):
                    has_restriction = True
                    break
        
        return 'RESTRITO' if has_restriction else 'LIMPO'
    
    def _determine_delay_tag(self, block: Dict) -> int:
        """Determina tag de atraso (campo de múltipla escolha)"""
        max_days = self._calculate_max_overdue_days(block)
        # Retorna IDs corretos para campo de múltipla escolha:
        # ID 121: EVITAR INAD (ATRASO < 90 DIAS)
        # ID 122: EVITAR PREJU (ATRASO > 90 DIAS)
        return 122 if max_days > 90 else 121
    
    def _extract_avalistas_info(self, block: Dict) -> List[Dict]:
        """Extrai informações dos avalistas"""
        avalistas = []
        
        for participante in block['20']:
            cpf_cnpj = self._clean_document(participante.get('CPF_CNPJ_Participante', ''))
            nome = participante.get('Nome_Participante', '').strip()
            
            if cpf_cnpj and nome:
                avalistas.append({
                    'cpf_cnpj': cpf_cnpj,
                    'nome': nome,
                    'responsabilidade': participante.get('Responsabilidade_Participante', '').strip(),
                    'tipo_pessoa': self._map_tipo_pessoa_from_txt(participante.get('Tipo_Pessoa_Participante', ''))
                })
        
        return avalistas
    
    def _format_contract_number(self, contract_str: str) -> str:
        """Remove zeros à esquerda dos números de contrato"""
        if not contract_str:
            return ''
        # Remove zeros à esquerda, mas preserva pelo menos um dígito
        return contract_str.lstrip('0') or '0'
    
    def _format_date(self, date_str: str) -> str:
        """Formata data de AAAAMMDD para DD/MM/AAAA"""
        if not date_str or len(date_str) != 8:
            return ''
        try:
            date_obj = datetime.strptime(date_str, '%Y%m%d')
            return date_obj.strftime('%d/%m/%Y')
        except:
            return date_str
    
    def _format_money(self, money_str: str) -> float:
        """Formata valor monetário"""
        if not money_str:
            return 0.0
        try:
            # Remove zeros à esquerda e adiciona ponto decimal
            clean_str = money_str.lstrip('0') or '0'
            if len(clean_str) >= 2:
                return float(clean_str[:-2] + '.' + clean_str[-2:])
            return float(clean_str)
        except:
            return 0.0
    
    def _format_percent(self, percent_str: str) -> float:
        """Formata percentual"""
        if not percent_str:
            return 0.0
        try:
            clean_str = percent_str.lstrip('0') or '0'
            if len(clean_str) >= 4:
                return float(clean_str[:-4] + '.' + clean_str[-4:])
            return float(clean_str)
        except:
            return 0.0
    
    # Métodos existentes mantidos para compatibilidade
    def find_latest_txt_file(self, folder_path: str = None) -> Optional[str]:
        """Encontra o arquivo TXT mais recente"""
        folder = folder_path or active_config.TXT_INPUT_FOLDER
        
        try:
            txt_files = [f for f in os.listdir(folder) if f.endswith('.txt')]
            if not txt_files:
                return None
            
            # Ordenar por data de modificação (mais recente primeiro)
            txt_files.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
            
            return os.path.join(folder, txt_files[0])
        except Exception as e:
            logger.error(f"Erro ao buscar arquivo TXT: {e}")
            return None
    
    def validate_cpf(self, cpf: str) -> bool:
        """Valida CPF"""
        if not cpf:
            return False
        
        # Limpar CPF
        cpf_clean = self._clean_document(cpf)
        
        # Verificar se tem 11 dígitos
        if len(cpf_clean) != 11:
            return False
        
        # Verificar se não são todos números iguais
        if cpf_clean == cpf_clean[0] * 11:
            return False
        
        return True
    
    def _clean_cpf(self, cpf: str) -> str:
        """Limpa formatação do CPF"""
        return self._clean_document(cpf)
    
    def backup_file(self, file_path: str) -> str:
        """Cria backup de um arquivo"""
        try:
            filename = os.path.basename(file_path)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"{timestamp}_{filename}"
            backup_path = os.path.join(active_config.BACKUP_FOLDER, backup_filename)
            
            # Copiar arquivo
            with open(file_path, 'rb') as src, open(backup_path, 'wb') as dst:
                dst.write(src.read())
            
            logger.info(f"Backup criado: {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"Erro ao criar backup: {e}")
            return ""
    
    def create_processing_report(self, stats: Dict) -> str:
        """Cria relatório de processamento"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_filename = f"relatorio_processamento_{timestamp}.txt"
            report_path = os.path.join('./logs', report_filename)
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(f"RELATÓRIO DE PROCESSAMENTO - {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                f.write("=" * 60 + "\n\n")
                
                f.write("ESTATÍSTICAS GERAIS:\n")
                f.write(f"- Pessoas criadas: {len(stats.get('pessoas_criadas', []))}\n")
                f.write(f"- Negócios criados: {len(stats.get('negocios_criados', []))}\n")
                f.write(f"- Negócios atualizados: {len(stats.get('negocios_atualizados', []))}\n")
                f.write(f"- Negócios movidos para SDR: {len(stats.get('negocios_movidos_para_sdr', []))}\n")
                f.write(f"- Negócios marcados como perdidos: {len(stats.get('negocios_marcados_perdidos', []))}\n")
                f.write(f"- Erros: {len(stats.get('erros', []))}\n\n")
                
                if stats.get('erros'):
                    f.write("ERROS ENCONTRADOS:\n")
                    for erro in stats['erros']:
                        f.write(f"- {erro}\n")
            
            return report_path
        except Exception as e:
            logger.error(f"Erro ao criar relatório: {e}")
            return "" 

    def _build_person_data_from_txt(self, inadimplente: Dict) -> Dict:
        """
        Constrói dados da pessoa a partir do TXT com mapeamento completo dos campos
        """
        data = {}
        
        # === CAMPOS PADRÃO ===
        
        # Nome completo
        nome_completo = inadimplente.get('nome', '').strip()
        if nome_completo:
            # Dividir nome em primeiro nome e sobrenome
            partes_nome = nome_completo.split()
            if len(partes_nome) >= 2:
                data['first_name'] = partes_nome[0]
                data['last_name'] = ' '.join(partes_nome[1:])
            else:
                data['first_name'] = nome_completo
                data['last_name'] = ''
        
        # Telefone principal
        telefones = inadimplente.get('telefones', [])
        if telefones:
            data['phone'] = telefones[0]  # Telefone padrão
        
        # Email principal
        emails = inadimplente.get('emails', [])
        if emails:
            data['email'] = emails[0]  # Email padrão
        
        # Endereço postal
        endereco = inadimplente.get('endereco_completo', '')
        if endereco:
            data['address'] = endereco  # Endereço postal padrão
        
        # Proprietário
        if hasattr(active_config, 'PIPEDRIVE_OWNER_ID') and active_config.PIPEDRIVE_OWNER_ID:
            data['owner_id'] = active_config.PIPEDRIVE_OWNER_ID
        
        # === CAMPOS PERSONALIZADOS ===
        # Usar nomes de campos para que as funções do pipedrive_client façam o mapeamento correto
        
        # CPF (para PF)
        if inadimplente.get('tipo_pessoa') == 'PF':
            cpf_cnpj = inadimplente.get('cpf_cnpj', '')
            if cpf_cnpj:
                data['CPF'] = cpf_cnpj
        
        # Data de Nascimento
        data_nascimento = inadimplente.get('data_nascimento', '')
        if data_nascimento:
            data['DATA_NASCIMENTO'] = data_nascimento
            
            # Calcular idade se possível
            try:
                from datetime import datetime
                if '/' in data_nascimento:
                    nascimento = datetime.strptime(data_nascimento, '%d/%m/%Y')
                    hoje = datetime.now()
                    idade = hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
                    data['IDADE'] = idade
            except:
                pass
        
        # Estado Civil
        estado_civil = inadimplente.get('raw_block', {}).get('01', [{}])[0].get('Estado_Civil', '').strip()
        if estado_civil:
            data['ESTADO_CIVIL'] = estado_civil
        
        # Condição CPF (baseado nas restrições)
        condicao_cpf = inadimplente.get('condicao_cpf', '')
        if condicao_cpf:
            data['CONDICAO_CPF'] = condicao_cpf
        
        # ENDEREÇO (campo personalizado, diferente do endereço postal)
        if endereco:
            # CORREÇÃO: Converter endereço para objeto
            address_obj = self._format_address_for_custom_field(endereco)
            data['ENDERECO'] = address_obj
        

        
        # === ANOTAÇÕES (campos adicionais) ===
        anotacoes = []
        
        # Extrair dados adicionais do raw_block
        raw_devedor = inadimplente.get('raw_block', {}).get('01', [{}])[0]
        
        # RG
        rg = raw_devedor.get('RG', '').strip()
        if rg:
            # CORREÇÃO: Remover zeros à esquerda do RG
            rg_limpo = rg.lstrip('0') if rg.lstrip('0') else '0'
            data_emissao_rg = raw_devedor.get('Data_Emissao_RG', '')
            orgao_emissor_rg = raw_devedor.get('Orgao_Emissor_RG', '').strip()
            uf_rg = raw_devedor.get('UF_RG', '').strip()
            anotacoes.append(f"RG: {rg_limpo} (Emitido em: {data_emissao_rg}) - {orgao_emissor_rg}/{uf_rg}")
        
        # Nome da mãe
        nome_mae = raw_devedor.get('Nome_Mae', '').strip()
        if nome_mae:
            anotacoes.append(f"Nome da mãe: {nome_mae}")
        
        # Cônjuge
        nome_conjuge = raw_devedor.get('Nome_Conjuge', '').strip()
        if nome_conjuge:
            anotacoes.append(f"Cônjuge: {nome_conjuge}")
        
        # Nacionalidade
        nacionalidade = raw_devedor.get('Nacionalidade', '').strip()
        if nacionalidade:
            anotacoes.append(f"Nacionalidade: {nacionalidade}")
        
        # Tipo de pessoa
        tipo_pessoa = inadimplente.get('tipo_pessoa', '')
        if tipo_pessoa:
            anotacoes.append(f"Tipo: {tipo_pessoa}")
        
        # Adicionar anotações ao campo notes se houver
        if anotacoes:
            data['notes'] = '\n'.join(anotacoes)
        
        return data 

    def _format_address_for_custom_field(self, address_string: str) -> Dict:
        """
        Converte string de endereço para o formato de objeto esperado pelo campo personalizado
        
        Args:
            address_string: String do endereço (ex: "Rua das Flores, 123, Centro, São Paulo, SP, 01234-567")
            
        Returns:
            Objeto de endereço no formato do campo personalizado
        """
        if not address_string or not isinstance(address_string, str):
            return {
                "address": "",
                "city": "",
                "state": "",
                "zip": ""
            }
        
        # Limpar e dividir o endereço
        parts = [part.strip() for part in address_string.split(',') if part.strip()]

        # Manter "address" como a string COMPLETA para não perder bairro/cidade/UF/CEP
        address_obj = {
            "address": address_string,
            "city": "",     # Município
            "state": "",    # UF
            "zip": ""       # CEP
        }

        # Preencher campos auxiliares sem alterar o campo "address" completo
        if len(parts) >= 5:
            # Padrão: "Endereço, Bairro, Município, UF, CEP"
            address_obj["city"] = parts[2]
            address_obj["state"] = parts[3]
            address_obj["zip"] = parts[4]
        elif len(parts) >= 4:
            # Padrão: "Endereço, Bairro, Município, UF"
            address_obj["city"] = parts[2]
            address_obj["state"] = parts[3]
        elif len(parts) >= 3:
            # Padrão: "Endereço, Bairro, Município"
            address_obj["city"] = parts[2]

        return address_obj

    def _build_organization_data_from_txt(self, inadimplente: Dict) -> Dict:
        """
        Constrói dados da organização a partir do TXT com mapeamento completo dos campos
        """
        data = {}
        
        # === CAMPOS PADRÃO ===
        
        # Nome da organização
        nome_organizacao = inadimplente.get('nome', '').strip()
        if nome_organizacao:
            data['name'] = nome_organizacao
        
        # Proprietário
        if hasattr(active_config, 'PIPEDRIVE_OWNER_ID') and active_config.PIPEDRIVE_OWNER_ID:
            data['owner_id'] = active_config.PIPEDRIVE_OWNER_ID
        
        # === CAMPOS PERSONALIZADOS ===
        # Usar nomes de campos para que as funções do pipedrive_client façam o mapeamento correto
        
        # CNPJ
        cnpj = inadimplente.get('cpf_cnpj', '')
        if cnpj:
            data['CPF_CNPJ'] = cnpj
        
        # Telefone principal
        telefones = inadimplente.get('telefones', [])
        if telefones:
            data['TELEFONE'] = telefones[0]
            # Telefone HOT (mesmo que telefone principal)
            data['TELEFONE_HOT'] = telefones[0]
        
        # Endereço
        endereco = inadimplente.get('endereco_completo', '')
        if endereco:
            # Converter string de endereço para objeto
            def endereco_para_objeto(endereco_str):
                # Esperado: 'Endereço, Bairro, Município, UF, CEP'
                partes = [p.strip() for p in endereco_str.split(',')]
                obj = {}
                if len(partes) >= 5:
                    obj['address'] = partes[0]  # Endereço
                    obj['city'] = partes[2]     # Município
                    obj['state'] = partes[3]    # UF
                    obj['zip'] = partes[4]      # CEP
                elif len(partes) == 4:
                    obj['address'] = partes[0]  # Endereço
                    obj['city'] = partes[2]     # Município
                    obj['state'] = partes[3]    # UF
                    obj['zip'] = ''
                elif len(partes) == 3:
                    obj['address'] = partes[0]  # Endereço
                    obj['city'] = partes[2]     # Município
                    obj['state'] = ''
                    obj['zip'] = ''
                elif len(partes) == 2:
                    obj['address'] = partes[0]  # Endereço
                    obj['city'] = ''
                    obj['state'] = ''
                    obj['zip'] = ''
                else:
                    obj['address'] = endereco_str
                    obj['city'] = ''
                    obj['state'] = ''
                    obj['zip'] = ''
                return obj
            data['ENDERECO'] = endereco_para_objeto(endereco)
        
        # E-mails
        emails = inadimplente.get('emails', [])
        if emails:
            data['EMAILS'] = '; '.join(emails)
        
        # Nome da Empresa (mesmo que nome da organização)
        if nome_organizacao:
            data['NOME_EMPRESA'] = nome_organizacao
        
        # Dados adicionais do raw_block (se disponíveis)
        raw_devedor = inadimplente.get('raw_block', {}).get('01', [{}])[0]
        
        # Extrair dados específicos de PJ (se disponíveis no TXT)
        # Nota: Estes campos podem não estar disponíveis no TXT atual
        
        # Campos que podem ser preenchidos com valores padrão ou vazios
        # Nome Fantasia (usar nome da organização como padrão)
        data['NOME_FANTASIA'] = nome_organizacao
        
        # Sócios (processar avalistas como sócios potenciais)
        avalistas_info = inadimplente.get('avalistas_info', [])
        if avalistas_info:
            socios_text = []
            for avalista in avalistas_info:
                if avalista.get('tipo_pessoa') == 'PF':  # Pessoa física pode ser sócio
                    nome_avalista = avalista.get('nome', '')
                    cpf_avalista = avalista.get('cpf_cnpj', '')
                    if nome_avalista:
                        socios_text.append(f"{nome_avalista} (CPF: {cpf_avalista})")
            
            if socios_text:
                data['SOCIOS'] = '; '.join(socios_text)
        
        # Campos que podem não estar disponíveis no TXT (deixar vazios por enquanto)
        # CNAE, Quantidade de Funcionários, Data da Abertura podem ser preenchidos manualmente
        
        # === ANOTAÇÕES REMOVIDAS ===
        # CORREÇÃO: API v2 não permite campo 'notes' para organizações
        # As informações serão mantidas apenas nos campos personalizados apropriados
        
        logger.debug(f"Dados construídos para organização: {data}")
        return data 

    def find_latest_garantinorte_file(self, folder_path: str = None) -> Optional[str]:
        """Encontra o arquivo Excel mais recente da GARANTINORTE"""
        folder = folder_path or active_config.GARANTINORTE_FOLDER
        
        try:
            excel_files = []
            for ext in ['*.xlsx', '*.xls']:
                excel_files.extend([f for f in os.listdir(folder) if f.lower().endswith(ext.replace('*', ''))])
            
            if not excel_files:
                logger.warning(f"Nenhum arquivo Excel encontrado em {folder}")
                return None
            
            # Ordenar por data de modificação (mais recente primeiro)
            excel_files.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)), reverse=True)
            
            return os.path.join(folder, excel_files[0])
        except Exception as e:
            logger.error(f"Erro ao buscar arquivo GARANTINORTE: {e}")
            return None
    
    def load_garantinorte_data(self, excel_file_path: str = None) -> Dict[str, str]:
        """
        Carrega dados da planilha GARANTINORTE e retorna dicionário CPF/CNPJ -> Contrato
        
        Args:
            excel_file_path: Caminho do arquivo Excel. Se None, busca o mais recente
            
        Returns:
            Dicionário {cpf_cnpj_limpo: numero_contrato}
        """
        logger.info("Carregando dados da planilha GARANTINORTE...")
        
        try:
            # Encontrar arquivo se não especificado
            if excel_file_path is None:
                excel_file_path = self.find_latest_garantinorte_file()
                if not excel_file_path:
                    logger.warning("Nenhum arquivo GARANTINORTE encontrado, continuando sem dados de garantia")
                    return {}
            
            logger.info(f"Carregando arquivo GARANTINORTE: {excel_file_path}")
            
            # Ler planilha Excel
            df = pd.read_excel(excel_file_path)
            
            # Verificar se as colunas necessárias existem
            colunas_necessarias = ['CPF/CNPJ', 'Contrato']
            colunas_encontradas = df.columns.tolist()
            
            # Buscar colunas com nomes similares (case insensitive)
            mapeamento_colunas = {}
            for coluna_necessaria in colunas_necessarias:
                coluna_encontrada = None
                for col in colunas_encontradas:
                    if coluna_necessaria.lower() in str(col).lower():
                        coluna_encontrada = col
                        break
                
                if coluna_encontrada:
                    mapeamento_colunas[coluna_necessaria] = coluna_encontrada
                else:
                    logger.error(f"Coluna '{coluna_necessaria}' não encontrada na planilha GARANTINORTE")
                    logger.info(f"Colunas disponíveis: {colunas_encontradas}")
                    return {}
            
            logger.info(f"Mapeamento de colunas GARANTINORTE: {mapeamento_colunas}")
            
            # Processar dados
            garantinorte_data = {}
            
            for index, row in df.iterrows():
                try:
                    # Extrair CPF/CNPJ
                    cpf_cnpj_raw = str(row[mapeamento_colunas['CPF/CNPJ']]).strip()
                    if cpf_cnpj_raw.lower() in ['nan', 'none', '']:
                        continue
                    
                    # Limpar CPF/CNPJ (remover formatação)
                    cpf_cnpj_limpo = self._clean_document(cpf_cnpj_raw)
                    if not cpf_cnpj_limpo or len(cpf_cnpj_limpo) < 11:
                        continue
                    
                    # Normalizar CPF/CNPJ para formato padrão
                    if len(cpf_cnpj_limpo) >= 14:
                        # CNPJ: usar últimos 14 dígitos
                        cpf_cnpj_normalizado = cpf_cnpj_limpo[-14:]
                    elif len(cpf_cnpj_limpo) >= 11:
                        # CPF: usar últimos 11 dígitos
                        cpf_cnpj_normalizado = cpf_cnpj_limpo[-11:]
                    else:
                        # Documento muito curto, pular
                        continue
                    
                    # Extrair Número do Contrato
                    numero_contrato = str(row[mapeamento_colunas['Contrato']]).strip()
                    if numero_contrato.lower() in ['nan', 'none', '']:
                        continue
                    
                    # Adicionar ao dicionário com documento normalizado
                    garantinorte_data[cpf_cnpj_normalizado] = numero_contrato
                    
                    # IMPORTANTE: Também adicionar variantes comuns para garantir compatibilidade
                    # Adicionar versão sem zeros à esquerda se diferente
                    cpf_cnpj_sem_zeros = cpf_cnpj_normalizado.lstrip('0')
                    if cpf_cnpj_sem_zeros and cpf_cnpj_sem_zeros != cpf_cnpj_normalizado:
                        garantinorte_data[cpf_cnpj_sem_zeros] = numero_contrato
                    
                    # Adicionar versão original se diferente (para compatibilidade)
                    if cpf_cnpj_limpo != cpf_cnpj_normalizado:
                        garantinorte_data[cpf_cnpj_limpo] = numero_contrato
                    
                except Exception as e:
                    logger.warning(f"Erro ao processar linha {index + 1} da GARANTINORTE: {e}")
                    continue
            
            logger.info(f"Dados GARANTINORTE carregados: {len(garantinorte_data)} registros")
            return garantinorte_data
            
        except Exception as e:
            logger.error(f"Erro ao carregar planilha GARANTINORTE: {e}")
            return {}
    
    def get_garantinorte_contract(self, cpf_cnpj: str, garantinorte_data: Dict[str, str]) -> str:
        """
        Busca contrato GARANTINORTE para um CPF/CNPJ específico com múltiplas tentativas
        
        Args:
            cpf_cnpj: CPF/CNPJ a ser buscado
            garantinorte_data: Dicionário com dados da GARANTINORTE
            
        Returns:
            Número do contrato ou string vazia
        """
        if not garantinorte_data:
            return ""
        
        # Gerar variantes do documento para busca
        cpf_cnpj_limpo = self._clean_document(cpf_cnpj)
        
        # Variantes para tentar (ordem de prioridade)
        variantes = [
            cpf_cnpj_limpo,                    # Documento original limpo
            cpf_cnpj_limpo.lstrip('0'),        # Sem zeros à esquerda  
            cpf_cnpj_limpo.zfill(14),          # Preencher até 14 dígitos (CNPJ)
            cpf_cnpj_limpo.zfill(11),          # Preencher até 11 dígitos (CPF)
        ]
        
        # Se documento tem mais de 14 dígitos (como TXT com 15 dígitos), extrair formatos válidos
        if len(cpf_cnpj_limpo) > 14:
            variantes.append(cpf_cnpj_limpo[-14:])  # Últimos 14 dígitos (CNPJ)
            variantes.append(cpf_cnpj_limpo[-11:])  # Últimos 11 dígitos (CPF)
        
        # Tentar encontrar em cada variante
        for i, variante in enumerate(variantes):
            if variante and variante in garantinorte_data:
                contrato = garantinorte_data[variante]
                logger.info(f"Contrato GARANTINORTE encontrado para {cpf_cnpj} usando variante '{variante}' (tentativa {i+1}): {contrato}")
                return contrato
        
        # Se não encontrou nenhuma correspondência
        logger.debug(f"Contrato GARANTINORTE não encontrado para {cpf_cnpj}. Variantes testadas: {variantes}")
        return "" 