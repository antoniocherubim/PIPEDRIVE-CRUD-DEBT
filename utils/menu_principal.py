"""
Menu principal de utilitários do sistema Pipedrive
"""

import os
import sys
from datetime import datetime

# Adicionar o diretório raiz ao path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import active_config

def executar_listagem_campos():
    """Executa listagem de campos personalizados"""
    print("\n=== LISTAGEM DE CAMPOS PERSONALIZADOS ===")
    
    try:
        from .listar_campos_personalizados import ListadorCamposPersonalizados
        
        listador = ListadorCamposPersonalizados()
        listador.listar_campos_personalizados()
        
        print("✅ Listagem concluída com sucesso!")
        
        except Exception as e:
        print(f"❌ Erro ao executar listagem: {e}")

def executar_backup_sqlite():
    """Executa backup SQLite"""
    print("\n=== BACKUP SQLITE ===")
    
    try:
        from .backup_sqlite import BackupSQLite
        
        backup_system = BackupSQLite()
        backup_system.executar_backup_completo()
        
        print("✅ Backup SQLite concluído com sucesso!")
            
        except Exception as e:
        print(f"❌ Erro ao executar backup SQLite: {e}")

def executar_consulta_backup_sqlite():
    """Executa consulta ao backup SQLite"""
    print("\n=== CONSULTA BACKUP SQLITE ===")
    
    try:
        from .consulta_backup_sqlite import ConsultaBackupSQLite
        
        consulta = ConsultaBackupSQLite()
        consulta.menu_consulta()
        
        print("✅ Consulta concluída com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao executar consulta: {e}")

def executar_mapeamento_duplicados():
    """Executa mapeamento de duplicados"""
    print("\n=== MAPEAMENTO DE DUPLICADOS ===")
    
    try:
        from .mapeamento_duplicados import MapeadorDuplicados
        
        mapeador = MapeadorDuplicados()
        mapeador.executar_mapeamento()
        
        print("✅ Mapeamento de duplicados concluído com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro ao executar mapeamento: {e}")

def executar_processamento_completo():
    """Executa processamento completo com backup SQLite"""
    print("\n=== PROCESSAMENTO COMPLETO COM BACKUP SQLITE ===")
    
    try:
        from src.business_rules import BusinessRulesProcessor
        
        processor = BusinessRulesProcessor()
        resultado = processor.process_inadimplentes_from_txt("input/escritorio_cobranca/escritorio_cobranca.txt")
        
        print("✅ Processamento completo concluído com sucesso!")
        print(f"📊 Resumo: {resultado}")
            
    except Exception as e:
        print(f"❌ Erro ao executar processamento: {e}")

def mostrar_menu():
    """Exibe o menu principal"""
    print("\n" + "="*60)
    print("           SISTEMA PIPEDRIVE - UTILITÁRIOS")
    print("="*60)
    print("1. Listar campos personalizados")
    print("2. Executar backup SQLite")
    print("3. Consultar backup SQLite")
    print("4. Mapear duplicados")
    print("5. Processamento completo com backup SQLite")
    print("0. Sair")
    print("="*60)

def executar_menu():
    """Executa o menu principal"""
    while True:
        mostrar_menu()
        
        try:
            opcao = input("\nEscolha uma opção: ").strip()
            
            if opcao == "1":
                executar_listagem_campos()
            elif opcao == "2":
                executar_backup_sqlite()
            elif opcao == "3":
                executar_consulta_backup_sqlite()
            elif opcao == "4":
                executar_mapeamento_duplicados()
            elif opcao == "5":
                executar_processamento_completo()
            elif opcao == "0":
                print("\n👋 Saindo do sistema...")
                break
            else:
                print("❌ Opção inválida!")
                
        except KeyboardInterrupt:
            print("\n\n👋 Interrompido pelo usuário. Saindo...")
            break
        except Exception as e:
            print(f"❌ Erro inesperado: {e}")

if __name__ == "__main__":
    executar_menu() 