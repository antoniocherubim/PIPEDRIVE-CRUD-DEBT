"""
Configuração dos campos personalizados do Pipedrive
Mapeamento dos IDs dos custom fields para facilitar manutenção
"""
from typing import Dict

class CustomFieldsConfig:
    """
    Configuração dos campos personalizados do Pipedrive
    
    IMPORTANTE: Substitua os valores abaixo pelos IDs reais dos seus campos personalizados
    Para encontrar os IDs, use o utilitário: python utils/listar_campos_personalizados.py
    """
    
    # ========== CAMPOS DE PESSOAS ==========
    PERSON_FIELDS = {
        # Campo CPF - ID informado pelo usuário
        'CPF': 'ac4ea7c903fd3d84ef190aaa8509bc6f434ee962',
        
        # Outros campos de pessoa (substitua pelos IDs reais)
        'DATA_NASCIMENTO': 'd3d7dd2caa163160d6cdbff2c617f6738448ff0d',
        'IDADE': 'd1fecbdc8fa0689e360345309b89379f3749fa6d',
        'ESTADO_CIVIL': '0cf27f47d8342c3350d37b18c4fbcf89168373d0',
        'CONDICAO_CPF': '9e0ef70534638fc82f23bd9c8ad7d22800d75c14',
        'ENDERECO': '48d9c16bbdb25755d815c04c45c1ac40572eb5c4',
    }
    
    # ========== CAMPOS DE NEGÓCIOS ==========
    DEAL_FIELDS = {
        # Campo ID CPF/CNPJ - ID informado pelo usuário
        'ID_CPF_CNPJ': '3b43edba171a600eb2c69ced34d891e60de187a7',
        
        # Outros campos de negócio (substitua pelos IDs reais)
        'COOPERADO': '5c5f2884a72d912898063bb53b4f62d30ccdd255',
        'COOPERATIVA': '1be5a64601075dc8602bc9867a39f6e01356da69',
        'TODOS_CONTRATOS': '0670adefd6c56a515805ca43afed18a92a21d904',
        'TODAS_OPERACOES': '2c69e6793769a47b0d8fa916d5435e3a373de093',
        'VENCIMENTO_MAIS_ANTIGO': '6838c45f241fd09caa062f01ab89b1488d5ddd2d',
        'NUMERO_CONTRATO': 'dcbf51de8734ca3b2cf9bcfae9587182a58506c3',
        'TIPO_ACAO_CARTEIRA': 'b3f90915abe2ff28556047284fdeaf267219364a',
        'AVALISTAS': '259f14a43e77f08c4ff1331f7660aeb9ce134345',
        'DIAS_DE_ATRASO': 'f8d37230fd1da5d2bb43bd1eb2b3173bb19e40c3',
        'VALOR_TOTAL_DA_DIVIDA': '0ee90449f3ac1708d7fd2fd381ede7e2b460e30b',
        'VALOR_TOTAL_VENCIDO': '8095b40894cd61a4e734e2b2c2c4871883ea5c85',
        'CONDICAO_CPF': '2eb287bf057fc6a6c5fb39c468701472c0563783',
        'VALOR_TOTAL_COM_JUROS': '8385b7273b78aaa86f580c88c8afb236a9f953c0',
        'TOTAL_DE_PARCELAS': '050d8dd8c88679f619ae9e478df76abccd55e382',
        'TAG_ATRASO': '42bf243df5543148ce0e51269501bb075b95b6cc',
        'CONTRATO_GARANTINORTE': '2fde5bd00849b24b1bcb119cbe0fe1accb2656f7',
    }
    
    # ========== CAMPOS DE ORGANIZAÇÕES ==========
    ORGANIZATION_FIELDS = {
        # Campos de organização (substitua pelos IDs reais)
        'TELEFONE': '0f1c441527979b5ef94c63ebfececdaf8fabcd19',
        'ENDERECO': '4fffd1287ec8ecea08bdadf4ee0d830077fd2ee8', 
        'CPF_CNPJ': 'c473c1a3c8b6c5c7906b50ee2a56054195ec9c11',
        'NOME_FANTASIA': '1833d83605328d3004650dfed15cdbc1fbee3d0a',
        'CNAE': '5b88c2c6110f994edd2c2329a3769adfbe1788be',
        'QUANTIDADE_FUNCIONARIOS': '1c8625cca1983ac3439402103437bec1a6bfd2c5',
        'SOCIOS': '7f1add66f6c20d26f71d89ca0f97dc49c95ecd0a',
        'DATA_ABERTURA': '9927a1bec4ddc1ca5a14cb10f706320fd4afdfbd',
        'EMAILS': '582ef3145b6859dab95fbb880585bce8ff932669',
        'TELEFONE_HOT': '5672775b048007603955220bb77cc2781b924edd',
        'NOME_EMPRESA': '0755edd87bb60dd349295dde31ba2571b433ee64',
    }
    
    # ========== MÉTODOS AUXILIARES ==========
    
    @classmethod
    def get_person_field_id(cls, field_name: str) -> str:
        """
        Retorna o ID do campo de pessoa
        
        Args:
            field_name: Nome do campo (ex: 'CPF', 'TELEFONE_CADASTRO_HOT')
            
        Returns:
            ID do campo personalizado
        """
        return cls.PERSON_FIELDS.get(field_name, field_name)
    
    @classmethod
    def get_deal_field_id(cls, field_name: str) -> str:
        """
        Retorna o ID do campo de negócio
        
        Args:
            field_name: Nome do campo (ex: 'ID_CPF_CNPJ', 'COOPERADO')
            
        Returns:
            ID do campo personalizado
        """
        return cls.DEAL_FIELDS.get(field_name, field_name)
    
    @classmethod
    def get_organization_field_id(cls, field_name: str) -> str:
        """
        Retorna o ID do campo de organização
        
        Args:
            field_name: Nome do campo (ex: 'TELEFONE', 'ENDERECO')
            
        Returns:
            ID do campo personalizado
        """
        return cls.ORGANIZATION_FIELDS.get(field_name, field_name)
    
    @classmethod
    def get_all_person_field_ids(cls) -> Dict[str, str]:
        """Retorna todos os IDs dos campos de pessoa"""
        return cls.PERSON_FIELDS.copy()
    
    @classmethod
    def get_all_deal_field_ids(cls) -> Dict[str, str]:
        """Retorna todos os IDs dos campos de negócio"""
        return cls.DEAL_FIELDS.copy()
    
    @classmethod
    def get_all_organization_field_ids(cls) -> Dict[str, str]:
        """Retorna todos os IDs dos campos de organização"""
        return cls.ORGANIZATION_FIELDS.copy()
    
    @classmethod
    def validate_field_ids(cls) -> bool:
        """
        Valida se os IDs dos campos foram configurados
        
        Returns:
            True se todos os campos estão configurados
        """
        errors = []
        
        # Verificar campos de pessoa
        for field_name, field_id in cls.PERSON_FIELDS.items():
            if field_id == 'SUBSTITUIR_PELO_ID_REAL':
                errors.append(f"Campo de pessoa '{field_name}' não configurado")
        
        # Verificar campos de negócio
        for field_name, field_id in cls.DEAL_FIELDS.items():
            if field_id == 'SUBSTITUIR_PELO_ID_REAL':
                errors.append(f"Campo de negócio '{field_name}' não configurado")
        
        # Verificar campos de organização
        for field_name, field_id in cls.ORGANIZATION_FIELDS.items():
            if field_id == 'SUBSTITUIR_PELO_ID_REAL':
                errors.append(f"Campo de organização '{field_name}' não configurado")
        
        if errors:
            print("AVISO: CAMPOS PERSONALIZADOS NAO CONFIGURADOS:")
            for error in errors:
                print(f"   - {error}")
            print("\nPARA CONFIGURAR:")
            print("   1. Execute: python utils/listar_campos_personalizados.py")
            print("   2. Atualize os IDs em src/custom_fields_config.py")
            return False
        
        return True
    
    @classmethod
    def print_configuration_summary(cls):
        """Imprime resumo da configuração atual"""
        print("CONFIGURACAO ATUAL DOS CAMPOS PERSONALIZADOS")
        print("=" * 60)
        
        print("\nCAMPOS DE PESSOAS:")
        for field_name, field_id in cls.PERSON_FIELDS.items():
            status = "OK" if field_id != 'SUBSTITUIR_PELO_ID_REAL' else "ERRO"
            print(f"   {status} {field_name}: {field_id}")
        
        print("\nCAMPOS DE NEGOCIOS:")
        for field_name, field_id in cls.DEAL_FIELDS.items():
            status = "OK" if field_id != 'SUBSTITUIR_PELO_ID_REAL' else "ERRO"
            print(f"   {status} {field_name}: {field_id}")
        
        print("\nCAMPOS DE ORGANIZACOES:")
        for field_name, field_id in cls.ORGANIZATION_FIELDS.items():
            status = "OK" if field_id != 'SUBSTITUIR_PELO_ID_REAL' else "ERRO"
            print(f"   {status} {field_name}: {field_id}")
        
        print("\nPROXIMOS PASSOS:")
        if not cls.validate_field_ids():
            print("   Execute: python utils/listar_campos_personalizados.py")
            print("   Atualize os IDs em src/custom_fields_config.py")
        else:
            print("   OK: Todos os campos estao configurados!") 