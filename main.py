"""
Sistema CRUD Pipedrive - Arquivo Principal

Sistema refinado para gerenciamento de inadimplentes no Pipedrive
com lógica específica para os funis BASE NOVA.

🚀 COM BACKUP SQLITE AUTOMÁTICO!

MODOS DE USO:
==============

1. PRINCIPAL (com backup SQLite automático):
   python main.py                        # Modo completo (padrão)
   python main.py --modo completo        # Mesmo que acima
   python main.py --modo txt-pipedrive   # Apenas TXT → Pipedrive

2. UTILITÁRIOS:
   python main.py --modo legado          # Ferramentas e utilitários
   python main.py --config-test          # Testar configurações

3. PERSONALIZAÇÃO:
   python main.py --txt-file arquivo.txt --db-name meu_backup.db

ARQUIVOS NECESSÁRIOS:
- TXT: input/escritorio_cobranca/*.txt
- GARANTINORTE: input/garantinorte/*.xlsx
- Token Pipedrive: arquivo .env
"""

import sys
import os
import argparse
from datetime import datetime

# Adicionar pasta src e utils ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))

import logging

# Imports do sistema principal (com backup SQLite)
from src.business_rules import BusinessRulesProcessor
from src.pipedrive_client import PipedriveClient
from src.file_processor import FileProcessor
from src.config import active_config

# Configurar logging com arquivo de log com timestamp
logger = active_config.setup_logging('main_system')

def main():
    """
    Função principal do sistema
    """
    # Mostrar banner de boas-vindas
    print("🚀 " + "="*70)
    print("   SISTEMA CRUD PIPEDRIVE - INADIMPLENTES")
    print("   COM BACKUP SQLITE AUTOMÁTICO! 💾")
    print("="*73)
    
    parser = argparse.ArgumentParser(
        description='Sistema CRUD Pipedrive - Inadimplentes (com backup SQLite)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXEMPLOS DE USO:
  python main.py                           # Processar com backup SQLite (padrão)
  python main.py --config-test             # Testar configurações
  python main.py --txt-file arquivo.txt    # Arquivo TXT específico
  python main.py --modo legado             # Utilitários e ferramentas
        """
    )
    
    # Modos de operação
    parser.add_argument('--modo', 
                        choices=['completo', 'txt-pipedrive', 'legado'], 
                        default='completo', 
                        help='Modo: completo=backup SQLite (padrão), legado=utilitários')
    
    # Arquivos específicos
    parser.add_argument('--txt-file', help='Arquivo TXT específico')
    
    # Configurações do backup SQLite
    parser.add_argument('--db-name', help='Nome customizado para o banco SQLite')
    
    # Testes
    parser.add_argument('--config-test', action='store_true', help='Testar configurações do sistema')
    
    args = parser.parse_args()
    
    # Mostrar modo selecionado
    if not args.config_test:
        if args.modo in ['completo', 'txt-pipedrive']:
            print(f"🎯 Modo selecionado: {args.modo.upper()} (com backup SQLite)")
        else:
            print(f"🔧 Modo selecionado: {args.modo.upper()}")
        print()
    
    # Processar baseado no modo
    try:
        if args.config_test:
            test_configuration()
        elif args.modo == 'completo':
            processo_completo_com_sqlite(args.txt_file, args.db_name)
        elif args.modo == 'txt-pipedrive':
            processo_txt_para_pipedrive_com_sqlite(args.txt_file, args.db_name)
        elif args.modo == 'legado':
            processo_legado()
    except KeyboardInterrupt:
        print("\n⏹️  Processamento interrompido pelo usuário")
    except Exception as e:
        logger.error(f"❌ Erro inesperado: {e}")
        print(f"\n❌ Erro inesperado: {e}")
    
    print("\n" + "="*73)
    print("🏁 Execução finalizada!")
    print("="*73)

def test_configuration():
    """
    Testa configurações do sistema
    """
    logger.info("=== TESTE DE CONFIGURAÇÃO ===")
    
    # Testar token
    if not active_config.PIPEDRIVE_API_TOKEN or active_config.PIPEDRIVE_API_TOKEN == "SEU_TOKEN_AQUI":
        logger.error("❌ Token do Pipedrive não configurado")
        return False
    
    # Testar conexão
    try:
        pipedrive = PipedriveClient()
        if pipedrive.test_connection():
            logger.info("✅ Conexão com Pipedrive estabelecida")
        else:
            logger.error("❌ Falha na conexão com Pipedrive")
            return False
    except Exception as e:
        logger.error(f"❌ Erro na conexão: {e}")
        return False
    
    # Testar IDs dos funis
    funis = [
        ("BASE NOVA - SDR", active_config.PIPELINE_BASE_NOVA_SDR_ID),
        ("BASE NOVA - NEGOCIAÇÃO", active_config.PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID),
        ("BASE NOVA - FORMALIZAÇÃO", active_config.PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID)
    ]
    
    for nome, id_funil in funis:
        if id_funil and id_funil != 1:  # Não é placeholder
            logger.info(f"✅ {nome}: ID {id_funil}")
        else:
            logger.error(f"❌ {nome}: ID não configurado")
            return False
    
    logger.info("✅ Todas as configurações estão corretas!")
    return True

def processo_completo_com_sqlite(txt_file_path=None, db_name=None):
    """
    Processo completo: TXT → Pipedrive (com backup SQLite automático)
    ESTE É O PROCESSO PRINCIPAL!
    """
    logger.info("=== PROCESSO COMPLETO COM BACKUP SQLITE ===")
    
    # Etapa 1: Encontrar arquivo TXT
    processor = FileProcessor()
    txt_file = txt_file_path or processor.find_latest_txt_file()
    
    if not txt_file:
        logger.error("❌ Nenhum arquivo TXT encontrado")
        return
    
    logger.info(f"📄 Arquivo TXT: {txt_file}")
    
    # Etapa 2: Processar TXT direto para Pipedrive COM BACKUP SQLITE
    business_processor = BusinessRulesProcessor(db_name=db_name)
    logger.info(f"💾 Banco SQLite: {business_processor.backup_sqlite.db_path}")
    
    result = business_processor.process_inadimplentes_from_txt(txt_file)
    
    logger.info("✅ Processo completo finalizado!")
    logger.info(f"Resumo: {business_processor.get_processing_summary()}")

    # Relatório adicional do backup SQLite
    logger.info("\n" + "="*50)
    logger.info("📊 RELATÓRIO DO BACKUP SQLITE:")
    logger.info(business_processor.gerar_relatorio_backup_sqlite())

def processo_txt_para_pipedrive_com_sqlite(txt_file_path=None, db_name=None):
    """
    Processo: TXT → Pipedrive (com backup SQLite automático)
    """
    logger.info("=== PROCESSO TXT -> PIPEDRIVE COM BACKUP SQLITE ===")
    
    if not txt_file_path:
        processor = FileProcessor()
        txt_file_path = processor.find_latest_txt_file()
    
    if not txt_file_path:
        logger.error("❌ Nenhum arquivo TXT encontrado")
        return
    
    logger.info(f"📄 Arquivo TXT: {txt_file_path}")
    
    # Processar com backup SQLite
    business_processor = BusinessRulesProcessor(db_name=db_name)
    logger.info(f"💾 Banco SQLite: {business_processor.backup_sqlite.db_path}")
    
    result = business_processor.process_inadimplentes_from_txt(txt_file_path)
    
    logger.info("✅ Processo TXT -> Pipedrive finalizado!")
    logger.info(f"Resumo: {business_processor.get_processing_summary()}")
    
    # Relatório adicional do backup SQLite
    logger.info("\n" + "="*50)
    logger.info("📊 RELATÓRIO DO BACKUP SQLITE:")
    logger.info(business_processor.gerar_relatorio_backup_sqlite())

def processo_legado():
    """
    Processo legado - utilitários e ferramentas complementares
    """
    logger.info("=== MODO LEGADO - UTILITÁRIOS ===")
    
    print("\n🔧 UTILITÁRIOS DISPONÍVEIS:")
    print("1. 📊 Consultar backup SQLite")
    print("2. 🔍 Listar campos personalizados")
    print("3. 📋 Menu principal de utilitários")
    
    try:
        from utils.consulta_backup_sqlite import ConsultaBackupSQLite
        print("\n💾 Bancos SQLite disponíveis:")
        consulta = ConsultaBackupSQLite()
        bancos = consulta.listar_bancos_disponiveis()
        
        if bancos:
            for i, banco in enumerate(bancos[:5], 1):
                print(f"   {i}. {banco}")
        else:
            print("   Nenhum backup SQLite encontrado")
            
    except Exception as e:
        logger.warning(f"Não foi possível listar backups SQLite: {e}")
    
    print("\n💡 Para usar os utilitários:")
    print("   python utils/menu_principal.py")
    print("   python examples/exemplo_backup_sqlite.py")
    print("   python utils/consulta_backup_sqlite.py --relatorio-completo")

if __name__ == "__main__":
    main() 