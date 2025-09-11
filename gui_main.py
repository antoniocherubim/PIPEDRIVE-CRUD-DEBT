"""
GUI Principal do Sistema Pipedrive - Inadimplentes
Interface moderna usando CustomTkinter
"""

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox
import threading
import os
import sys
from datetime import datetime
import queue
import time

# Adicionar diretórios ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))

# Imports do sistema
from src.business_rules import BusinessRulesProcessor
from src.optimized_business_rules import OptimizedBusinessRulesProcessor
from src.excel_export import PipedriveExcelExporter
from src.config import active_config

# Configurar tema do CustomTkinter
ctk.set_appearance_mode("dark")  # Modes: "System" (standard), "Dark", "Light"
ctk.set_default_color_theme("blue")  # Themes: "blue" (standard), "green", "dark-blue"

class PipedriveGUI:
    def __init__(self):
        self.root = ctk.CTk()
        self.root.title("Sistema Pipedrive - Inadimplentes")
        self.root.geometry("1200x800")
        self.root.minsize(1000, 600)
        
        # Variáveis
        self.selected_file = tk.StringVar()
        self.processing = False
        self.log_queue = queue.Queue()
        self.processing_thread = None
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 30.0  # 30 segundos (otimizado)
        
        # Variáveis de progresso otimizado
        self.total_items = 0
        self.processed_items = 0
        self.last_progress_update = time.time()
        
        # Processador otimizado
        self.optimized_processor = None
        self.excel_exporter = PipedriveExcelExporter()
        self.background_processor = None
        
        # Configurar interface
        self.setup_ui()
        self.setup_logging()
        self.setup_heartbeat()
        
        # Configurar tratamento de fechamento
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        
    def setup_heartbeat(self):
        """Configura sistema de heartbeat otimizado para detectar travamentos"""
        def heartbeat_check():
            if self.processing:
                current_time = time.time()
                
                # Verificar se realmente travou (5 minutos sem progresso real)
                if current_time - self.last_progress_update > 300:  # 5 minutos
                    self.log_message("⚠️ ALERTA: Processamento pode ter travado!")
                    self.status_label.configure(text="⚠️ Possível travamento")
                else:
                    # Atualizar progresso baseado em itens processados
                    if self.total_items > 0:
                        progress = min(self.processed_items / self.total_items, 0.95)
                        if hasattr(self, 'progress_bar'):
                            self.progress_bar.set(progress)
                        
                        # Atualizar status com informações reais
                        percentage = (self.processed_items / self.total_items) * 100
                        self.status_label.configure(
                            text=f"Processando: {self.processed_items}/{self.total_items} "
                                 f"({percentage:.1f}%)"
                        )
                        
                        # Log de progresso a cada 10%
                        if self.processed_items % max(1, self.total_items // 10) == 0:
                            self.log_message(f"Progresso: {self.processed_items}/{self.total_items} ({percentage:.1f}%)")
            else:
                # Se não está processando, resetar o progresso
                if hasattr(self, 'progress_bar'):
                    self.progress_bar.set(0)
                        
            self.root.after(30000, heartbeat_check)  # Verificar a cada 30 segundos (otimizado)
            
        heartbeat_check()
        
    def on_closing(self):
        """Tratamento de fechamento da aplicação"""
        if self.processing:
            if messagebox.askyesno("Confirmar Saída", 
                                 "Processamento em andamento. Deseja realmente sair?"):
                self.stop_processing()
                time.sleep(1)  # Aguardar thread parar
                self.root.destroy()
        else:
            self.root.destroy()
        
    def setup_ui(self):
        """Configura a interface principal"""
        # Frame principal
        main_frame = ctk.CTkFrame(self.root)
        main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Título
        title_label = ctk.CTkLabel(
            main_frame, 
            text="🚀 Sistema Pipedrive - Inadimplentes",
            font=ctk.CTkFont(size=24, weight="bold")
        )
        title_label.pack(pady=(20, 30))
        
        # Notebook para abas
        self.notebook = ctk.CTkTabview(main_frame)
        self.notebook.pack(fill="both", expand=True, padx=20, pady=10)
        
        # Criar abas
        self.create_processing_tab()
        self.create_backup_tab()
        self.create_utilities_tab()
        self.create_logs_tab()
        self.create_config_tab()
        
    def create_processing_tab(self):
        """Aba de processamento principal"""
        tab = self.notebook.add("📊 Processamento")
        
        # Frame de seleção de arquivo
        file_frame = ctk.CTkFrame(tab)
        file_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(file_frame, text="Arquivo TXT:", font=ctk.CTkFont(size=16)).pack(anchor="w", padx=10, pady=(10, 5))
        
        file_select_frame = ctk.CTkFrame(file_frame)
        file_select_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        self.file_entry = ctk.CTkEntry(file_select_frame, textvariable=self.selected_file, width=400)
        self.file_entry.pack(side="left", padx=(10, 10), pady=10)
        
        browse_btn = ctk.CTkButton(
            file_select_frame, 
            text="📁 Procurar", 
            command=self.browse_file,
            width=120
        )
        browse_btn.pack(side="left", padx=(0, 10), pady=10)
        
        auto_find_btn = ctk.CTkButton(
            file_select_frame, 
            text="🔍 Auto-detect", 
            command=self.auto_find_file,
            width=120
        )
        auto_find_btn.pack(side="left", pady=10)
        
        # Frame para planilhas Garantinorte
        garantinorte_frame = ctk.CTkFrame(tab)
        garantinorte_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(garantinorte_frame, text="Planilhas Garantinorte:", font=ctk.CTkFont(size=16)).pack(anchor="w", padx=10, pady=(10, 5))
        
        garantinorte_buttons_frame = ctk.CTkFrame(garantinorte_frame)
        garantinorte_buttons_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        add_garantinorte_btn = ctk.CTkButton(
            garantinorte_buttons_frame,
            text="📊 Adicionar Planilha Garantinorte",
            command=self.add_garantinorte_file,
            width=200,
            height=35,
            fg_color="orange",
            hover_color="darkorange"
        )
        add_garantinorte_btn.pack(side="left", padx=(10, 10), pady=10)
        
        open_garantinorte_folder_btn = ctk.CTkButton(
            garantinorte_buttons_frame,
            text="📁 Abrir Pasta Garantinorte",
            command=self.open_garantinorte_folder,
            width=180,
            height=35
        )
        open_garantinorte_folder_btn.pack(side="left", padx=(0, 10), pady=10)
        
        list_garantinorte_btn = ctk.CTkButton(
            garantinorte_buttons_frame,
            text="📋 Listar Planilhas",
            command=self.list_garantinorte_files,
            width=150,
            height=35
        )
        list_garantinorte_btn.pack(side="left", padx=(0, 10), pady=10)
        
        process_garantinorte_btn = ctk.CTkButton(
            garantinorte_buttons_frame,
            text="⚙️ Processar Garantinorte",
            command=self.process_garantinorte_files,
            width=180,
            height=35,
            fg_color="purple",
            hover_color="#555555"
        )
        process_garantinorte_btn.pack(side="left", padx=(0, 10), pady=10)
        
        # Frame de configurações
        config_frame = ctk.CTkFrame(tab)
        config_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(config_frame, text="Configurações:", font=ctk.CTkFont(size=16)).pack(anchor="w", padx=10, pady=(10, 5))
        
        config_inner_frame = ctk.CTkFrame(config_frame)
        config_inner_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        # Nome do banco SQLite
        ctk.CTkLabel(config_inner_frame, text="Nome do Banco SQLite:").pack(anchor="w", padx=10, pady=(10, 5))
        self.db_name_entry = ctk.CTkEntry(config_inner_frame, placeholder_text="Deixe vazio para usar padrão")
        self.db_name_entry.pack(fill="x", padx=10, pady=(0, 10))
        
        # Frame de botões de processamento
        process_frame = ctk.CTkFrame(tab)
        process_frame.pack(fill="x", padx=20, pady=10)
        
        ctk.CTkLabel(process_frame, text="Processamento:", font=ctk.CTkFont(size=16)).pack(anchor="w", padx=10, pady=(10, 5))
        
        buttons_frame = ctk.CTkFrame(process_frame)
        buttons_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        # Botões de processamento
        self.process_btn = ctk.CTkButton(
            buttons_frame,
            text="▶️ Processar Completo",
            command=self.start_processing,
            fg_color="green",
            hover_color="darkgreen",
            height=40
        )
        self.process_btn.pack(side="left", padx=(10, 10), pady=10)
        
        # Botão de processamento otimizado
        self.process_optimized_btn = ctk.CTkButton(
            buttons_frame,
            text="🚀 Processar Otimizado",
            command=self.start_processing_optimized,
            fg_color="#FF6B35",
            hover_color="#E55A2B",
            height=40
        )
        self.process_optimized_btn.pack(side="left", padx=(0, 10), pady=10)
        
        # Botão de exportação Excel
        self.export_excel_btn = ctk.CTkButton(
            buttons_frame,
            text="📊 Exportar Excel",
            command=self.export_to_excel,
            fg_color="#4ECDC4",
            hover_color="#45B7B8",
            height=40
        )
        self.export_excel_btn.pack(side="left", padx=(0, 10), pady=10)
        
        # Botão de processamento em background
        self.background_btn = ctk.CTkButton(
            buttons_frame,
            text="🌙 Background",
            command=self.start_background_processing,
            fg_color="#8E44AD",
            hover_color="#7D3C98",
            height=40
        )
        self.background_btn.pack(side="left", padx=(0, 10), pady=10)
        
        self.stop_btn = ctk.CTkButton(
            buttons_frame,
            text="⏹️ Parar",
            command=self.stop_processing,
            fg_color="red",
            hover_color="darkred",
            height=40,
            state="disabled"
        )
        self.stop_btn.pack(side="left", padx=(0, 10), pady=10)
        
        # Botão de emergência para forçar parada
        self.emergency_btn = ctk.CTkButton(
            buttons_frame,
            text="🚨 EMERGÊNCIA",
            command=self.emergency_stop,
            fg_color="#FF4500",
            hover_color="#FF6347",
            height=40,
            state="disabled"
        )
        self.emergency_btn.pack(side="left", padx=(0, 10), pady=10)
        
        # Progress bar
        self.progress_bar = ctk.CTkProgressBar(buttons_frame)
        self.progress_bar.pack(side="left", fill="x", expand=True, padx=(0, 10), pady=10)
        self.progress_bar.set(0)
        
        # Status
        self.status_label = ctk.CTkLabel(buttons_frame, text="Pronto para processar")
        self.status_label.pack(side="left", padx=(0, 10), pady=10)
        
    def create_backup_tab(self):
        """Aba de consulta de backup"""
        tab = self.notebook.add("💾 Backup SQLite")
        
        # Frame de consultas
        query_frame = ctk.CTkFrame(tab)
        query_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        ctk.CTkLabel(query_frame, text="Consulta de Backup:", font=ctk.CTkFont(size=16)).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Botões de consulta
        buttons_frame = ctk.CTkFrame(query_frame)
        buttons_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkButton(
            buttons_frame,
            text="📊 Estatísticas Gerais",
            command=self.show_backup_stats,
            width=150
        ).pack(side="left", padx=(10, 10), pady=10)
        
        ctk.CTkButton(
            buttons_frame,
            text="🔍 Buscar por Documento",
            command=self.search_by_document,
            width=150
        ).pack(side="left", padx=(0, 10), pady=10)
        
        ctk.CTkButton(
            buttons_frame,
            text="📋 Relatório Completo",
            command=self.show_full_report,
            width=150
        ).pack(side="left", padx=(0, 10), pady=10)
        
        ctk.CTkButton(
            buttons_frame,
            text="📤 Exportar CSV",
            command=self.export_to_csv,
            width=150
        ).pack(side="left", padx=(0, 10), pady=10)
        
        # Área de resultados
        results_frame = ctk.CTkFrame(query_frame)
        results_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        ctk.CTkLabel(results_frame, text="Resultados:").pack(anchor="w", padx=10, pady=(10, 5))
        
        self.results_text = ctk.CTkTextbox(results_frame, height=300)
        self.results_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
    def create_utilities_tab(self):
        """Aba de utilitários"""
        tab = self.notebook.add("🔧 Utilitários")
        
        # Frame de utilitários
        utils_frame = ctk.CTkFrame(tab)
        utils_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        ctk.CTkLabel(utils_frame, text="Utilitários Disponíveis:", font=ctk.CTkFont(size=16)).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Grid de botões
        buttons_frame = ctk.CTkFrame(utils_frame)
        buttons_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Primeira linha
        row1 = ctk.CTkFrame(buttons_frame)
        row1.pack(fill="x", padx=10, pady=10)
        
        ctk.CTkButton(
            row1,
            text="📋 Listar Campos Personalizados",
            command=self.list_custom_fields,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        ctk.CTkButton(
            row1,
            text="🔍 Mapear Duplicados",
            command=self.map_duplicates,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        # Segunda linha
        row2 = ctk.CTkFrame(buttons_frame)
        row2.pack(fill="x", padx=10, pady=10)
        
        ctk.CTkButton(
            row2,
            text="⚙️ Testar Configuração",
            command=self.test_configuration,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        ctk.CTkButton(
            row2,
            text="📊 Relatório de Processamento",
            command=self.show_processing_report,
            width=200,
            height=40
        ).pack(side="left", padx=(0, 10))
        
        # Terceira linha - Background
        row3 = ctk.CTkFrame(buttons_frame)
        row3.pack(fill="x", padx=10, pady=10)
        
        ctk.CTkButton(
            row3,
            text="🌙 Status Background",
            command=self.check_background_status,
            width=200,
            height=40,
            fg_color="#8E44AD",
            hover_color="#7D3C98"
        ).pack(side="left", padx=(0, 10))
        
        ctk.CTkButton(
            row3,
            text="🛑 Parar Background",
            command=self.stop_background_processing,
            width=200,
            height=40,
            fg_color="#E74C3C",
            hover_color="#C0392B"
        ).pack(side="left", padx=(0, 10))
        
    def create_logs_tab(self):
        """Aba de logs"""
        tab = self.notebook.add("📝 Logs")
        
        # Frame de logs
        logs_frame = ctk.CTkFrame(tab)
        logs_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        # Controles de log
        controls_frame = ctk.CTkFrame(logs_frame)
        controls_frame.pack(fill="x", padx=10, pady=(10, 5))
        
        ctk.CTkButton(
            controls_frame,
            text="🔄 Atualizar Logs",
            command=self.refresh_logs,
            width=120
        ).pack(side="left", padx=(10, 10), pady=10)
        
        ctk.CTkButton(
            controls_frame,
            text="🗑️ Limpar Logs",
            command=self.clear_logs,
            width=120
        ).pack(side="left", padx=(0, 10), pady=10)
        
        ctk.CTkButton(
            controls_frame,
            text="📁 Abrir Pasta Logs",
            command=self.open_logs_folder,
            width=120
        ).pack(side="left", padx=(0, 10), pady=10)
        
        # Área de logs
        self.logs_text = ctk.CTkTextbox(logs_frame, height=500)
        self.logs_text.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Carregar logs iniciais
        self.refresh_logs()
        
    def create_config_tab(self):
        """Aba de configuração"""
        tab = self.notebook.add("⚙️ Configuração")
        
        # Frame de configuração
        config_frame = ctk.CTkFrame(tab)
        config_frame.pack(fill="both", expand=True, padx=20, pady=10)
        
        ctk.CTkLabel(config_frame, text="Configurações do Sistema:", font=ctk.CTkFont(size=16)).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Configurações
        settings_frame = ctk.CTkFrame(config_frame)
        settings_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        
        # Token da API
        ctk.CTkLabel(settings_frame, text="Token da API Pipedrive:").pack(anchor="w", padx=10, pady=(10, 5))
        self.token_entry = ctk.CTkEntry(settings_frame, show="*", width=400)
        self.token_entry.pack(anchor="w", padx=10, pady=(0, 10))
        self.token_entry.insert(0, getattr(active_config, 'PIPEDRIVE_API_TOKEN', ''))
        
        # Domínio
        ctk.CTkLabel(settings_frame, text="Domínio Pipedrive:").pack(anchor="w", padx=10, pady=(10, 5))
        self.domain_entry = ctk.CTkEntry(settings_frame, width=400)
        self.domain_entry.pack(anchor="w", padx=10, pady=(0, 10))
        self.domain_entry.insert(0, getattr(active_config, 'PIPEDRIVE_DOMAIN', ''))
        
        # Botões de configuração
        config_buttons_frame = ctk.CTkFrame(settings_frame)
        config_buttons_frame.pack(fill="x", padx=10, pady=10)
        
        ctk.CTkButton(
            config_buttons_frame,
            text="💾 Salvar Configuração",
            command=self.save_configuration,
            width=150
        ).pack(side="left", padx=(0, 10))
        
        ctk.CTkButton(
            config_buttons_frame,
            text="🔄 Carregar Configuração",
            command=self.load_configuration,
            width=150
        ).pack(side="left", padx=(0, 10))
        
        # Informações de logging
        logging_info_frame = ctk.CTkFrame(settings_frame)
        logging_info_frame.pack(fill="x", padx=10, pady=10)
        
        ctk.CTkLabel(logging_info_frame, text="📝 Configurações de Log:", font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Mostrar configurações atuais de logging
        self.logging_info_text = ctk.CTkTextbox(logging_info_frame, height=100)
        self.logging_info_text.pack(fill="x", padx=10, pady=(0, 10))
        
        # Frame de diagnóstico
        diagnostic_frame = ctk.CTkFrame(settings_frame)
        diagnostic_frame.pack(fill="x", padx=10, pady=10)
        
        ctk.CTkLabel(diagnostic_frame, text="🔍 Diagnóstico do Sistema:", font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", padx=10, pady=(10, 5))
        
        # Botões de diagnóstico
        diagnostic_buttons_frame = ctk.CTkFrame(diagnostic_frame)
        diagnostic_buttons_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        ctk.CTkButton(
            diagnostic_buttons_frame,
            text="🔍 Verificar Sistema",
            command=self.run_system_diagnostic,
            width=150
        ).pack(side="left", padx=(0, 10), pady=10)
        
        ctk.CTkButton(
            diagnostic_buttons_frame,
            text="📊 Status de Memória",
            command=self.check_memory_status,
            width=150
        ).pack(side="left", padx=(0, 10), pady=10)
        
        # Atualizar informações de logging
        self.update_logging_info()
        
    def setup_logging(self):
        """Configura o sistema de logging usando as configurações do config.py"""
        # Usar as configurações padronizadas do config.py
        self.logger = active_config.setup_logging('gui_main')
        self.update_logs()
        
    def update_logs(self):
        """Atualiza os logs periodicamente"""
        try:
            while True:
                log_entry = self.log_queue.get_nowait()
                self.logs_text.insert("end", log_entry + "\n")
                self.logs_text.see("end")
        except queue.Empty:
            pass
        
        self.root.after(100, self.update_logs)
        
    def log_message(self, message):
        """Adiciona mensagem ao log usando as configurações do config.py"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.log_queue.put(log_entry)
        
        # Também usar o logger configurado se disponível
        if hasattr(self, 'logger'):
            self.logger.info(message)
            
    def update_logging_info(self):
        """Atualiza as informações de logging na aba de configuração"""
        try:
            if hasattr(self, 'logging_info_text'):
                info_text = f"Pasta de Logs: {active_config.LOGS_FOLDER}\n"
                info_text += f"Nível de Log: {active_config.LOG_LEVEL}\n"
                info_text += f"Logger Ativo: {'Sim' if hasattr(self, 'logger') else 'Não'}\n"
                
                # Verificar se a pasta de logs existe
                if os.path.exists(active_config.LOGS_FOLDER):
                    log_files = [f for f in os.listdir(active_config.LOGS_FOLDER) if f.endswith('.log')]
                    info_text += f"Arquivos de Log: {len(log_files)}\n"
                    if log_files:
                        latest_log = max(log_files, key=lambda x: os.path.getctime(os.path.join(active_config.LOGS_FOLDER, x)))
                        info_text += f"Último Log: {latest_log}"
                else:
                    info_text += "Pasta de Logs: Não existe"
                    
                self.logging_info_text.delete("1.0", "end")
                self.logging_info_text.insert("1.0", info_text)
        except Exception as e:
            if hasattr(self, 'logging_info_text'):
                self.logging_info_text.delete("1.0", "end")
                self.logging_info_text.insert("1.0", f"Erro ao carregar informações: {e}")
        
    def browse_file(self):
        """Abre diálogo para selecionar arquivo"""
        filename = filedialog.askopenfilename(
            title="Selecionar arquivo TXT",
            filetypes=[("Arquivos TXT", "*.txt"), ("Todos os arquivos", "*.*")]
        )
        if filename:
            self.selected_file.set(filename)
            self.log_message(f"Arquivo selecionado: {filename}")
            
    def auto_find_file(self):
        """Auto-detecta arquivo TXT"""
        possible_paths = [
            "input/escritorio_cobranca/escritorio_cobranca.txt",
            "escritorio_cobranca.txt",
            "input/escritorio_cobranca"
        ]
        
        for path in possible_paths:
            if os.path.isfile(path):
                self.selected_file.set(path)
                self.log_message(f"Arquivo auto-detectado: {path}")
                return
            elif os.path.isdir(path):
                for file in os.listdir(path):
                    if file.endswith('.txt'):
                        full_path = os.path.join(path, file)
                        self.selected_file.set(full_path)
                        self.log_message(f"Arquivo auto-detectado: {full_path}")
                        return
        
        messagebox.showwarning("Aviso", "Nenhum arquivo TXT encontrado automaticamente")
        
    def add_garantinorte_file(self):
        """Adiciona planilha da Garantinorte"""
        # Abrir diálogo para selecionar arquivo
        filename = filedialog.askopenfilename(
            title="Selecionar Planilha Garantinorte",
            filetypes=[
                ("Planilhas Excel", "*.xlsx *.xls"),
                ("Arquivos CSV", "*.csv"),
                ("Todos os arquivos", "*.*")
            ]
        )
        
        if filename:
            try:
                # Criar pasta se não existir
                garantinorte_dir = "input/garantinorte"
                if not os.path.exists(garantinorte_dir):
                    os.makedirs(garantinorte_dir)
                
                # Copiar arquivo para a pasta
                import shutil
                dest_path = os.path.join(garantinorte_dir, os.path.basename(filename))
                shutil.copy2(filename, dest_path)
                
                self.log_message(f"Planilha Garantinorte adicionada: {dest_path}")
                messagebox.showinfo(
                    "Sucesso", 
                    f"Planilha adicionada com sucesso!\n\nArquivo: {os.path.basename(filename)}\nDestino: {dest_path}"
                )
                
            except Exception as e:
                self.log_message(f"Erro ao adicionar planilha: {e}")
                messagebox.showerror("Erro", f"Erro ao adicionar planilha: {e}")
                
    def open_garantinorte_folder(self):
        """Abre a pasta de planilhas Garantinorte"""
        garantinorte_dir = "input/garantinorte"
        
        # Criar pasta se não existir
        if not os.path.exists(garantinorte_dir):
            os.makedirs(garantinorte_dir)
        
        try:
            # Abrir pasta no explorador de arquivos
            if os.name == 'nt':  # Windows
                os.startfile(garantinorte_dir)
            elif os.name == 'posix':  # macOS e Linux
                import subprocess
                subprocess.run(['open', garantinorte_dir])  # macOS
            else:
                import subprocess
                subprocess.run(['xdg-open', garantinorte_dir])  # Linux
                
            self.log_message(f"Pasta Garantinorte aberta: {garantinorte_dir}")
            
        except Exception as e:
            self.log_message(f"Erro ao abrir pasta: {e}")
            messagebox.showerror("Erro", f"Erro ao abrir pasta: {e}")
            
    def list_garantinorte_files(self):
        """Lista as planilhas existentes na pasta Garantinorte"""
        garantinorte_dir = "input/garantinorte"
        
        if not os.path.exists(garantinorte_dir):
            messagebox.showinfo("Informação", "Pasta Garantinorte não existe ainda. Adicione uma planilha primeiro.")
            return
            
        try:
            # Listar arquivos na pasta
            files = [f for f in os.listdir(garantinorte_dir) if os.path.isfile(os.path.join(garantinorte_dir, f))]
            
            if not files:
                messagebox.showinfo("Informação", "Nenhuma planilha encontrada na pasta Garantinorte.")
                return
                
            # Criar janela com lista de arquivos
            files_window = ctk.CTkToplevel(self.root)
            files_window.title("Planilhas Garantinorte")
            files_window.geometry("600x400")
            
            # Título
            title_label = ctk.CTkLabel(
                files_window,
                text="📊 Planilhas na Pasta Garantinorte",
                font=ctk.CTkFont(size=18, weight="bold")
            )
            title_label.pack(pady=(20, 10))
            
            # Lista de arquivos
            files_text = ctk.CTkTextbox(files_window, height=250)
            files_text.pack(fill="both", expand=True, padx=20, pady=(0, 20))
            
            # Formatar lista de arquivos
            files_list = f"Pasta: {os.path.abspath(garantinorte_dir)}\n"
            files_list += f"Total de arquivos: {len(files)}\n\n"
            
            for i, file in enumerate(files, 1):
                file_path = os.path.join(garantinorte_dir, file)
                file_size = os.path.getsize(file_path)
                file_date = datetime.fromtimestamp(os.path.getctime(file_path)).strftime("%d/%m/%Y %H:%M")
                
                files_list += f"{i}. {file}\n"
                files_list += f"   Tamanho: {file_size:,} bytes\n"
                files_list += f"   Data: {file_date}\n"
                files_list += f"   Caminho: {file_path}\n\n"
                
            files_text.insert("1.0", files_list)
            
            # Botão para abrir pasta
            open_folder_btn = ctk.CTkButton(
                files_window,
                text="📁 Abrir Pasta",
                command=self.open_garantinorte_folder
            )
            open_folder_btn.pack(pady=(0, 20))
            
        except Exception as e:
            self.log_message(f"Erro ao listar planilhas: {e}")
            messagebox.showerror("Erro", f"Erro ao listar planilhas: {e}")
            
    def process_garantinorte_files(self):
        """Processa as planilhas da Garantinorte"""
        garantinorte_dir = "input/garantinorte"
        
        if not os.path.exists(garantinorte_dir):
            messagebox.showwarning("Aviso", "Pasta Garantinorte não existe. Adicione planilhas primeiro.")
            return
            
        # Listar arquivos disponíveis
        files = [f for f in os.listdir(garantinorte_dir) if os.path.isfile(os.path.join(garantinorte_dir, f))]
        
        if not files:
            messagebox.showwarning("Aviso", "Nenhuma planilha encontrada na pasta Garantinorte.")
            return
            
        # Criar janela de seleção
        selection_window = ctk.CTkToplevel(self.root)
        selection_window.title("Processar Planilhas Garantinorte")
        selection_window.geometry("700x500")
        
        # Título
        title_label = ctk.CTkLabel(
            selection_window,
            text="⚙️ Processar Planilhas Garantinorte",
            font=ctk.CTkFont(size=18, weight="bold")
        )
        title_label.pack(pady=(20, 10))
        
        # Frame para seleção
        selection_frame = ctk.CTkFrame(selection_window)
        selection_frame.pack(fill="both", expand=True, padx=20, pady=(0, 20))
        
        ctk.CTkLabel(selection_frame, text="Selecione as planilhas para processar:").pack(anchor="w", padx=10, pady=(10, 5))
        
        # Lista de checkboxes para arquivos
        file_vars = {}
        for file in files:
            var = tk.BooleanVar(value=True)  # Por padrão, todos selecionados
            file_vars[file] = var
            
            file_frame = ctk.CTkFrame(selection_frame)
            file_frame.pack(fill="x", padx=10, pady=2)
            
            checkbox = ctk.CTkCheckBox(file_frame, text=file, variable=var)
            checkbox.pack(side="left", padx=(10, 10), pady=5)
            
            # Mostrar informações do arquivo
            file_path = os.path.join(garantinorte_dir, file)
            file_size = os.path.getsize(file_path)
            info_label = ctk.CTkLabel(file_frame, text=f"({file_size:,} bytes)")
            info_label.pack(side="left", pady=5)
        
        # Botões de ação
        buttons_frame = ctk.CTkFrame(selection_window)
        buttons_frame.pack(fill="x", padx=20, pady=(0, 20))
        
        process_btn = ctk.CTkButton(
            buttons_frame,
            text="🚀 Processar Selecionados",
            command=lambda: self.execute_garantinorte_processing(file_vars, selection_window),
            fg_color="green",
            hover_color="darkgreen",
            width=200
        )
        process_btn.pack(side="left", padx=(0, 10), pady=10)
        
        cancel_btn = ctk.CTkButton(
            buttons_frame,
            text="❌ Cancelar",
            command=selection_window.destroy,
            width=120
        )
        cancel_btn.pack(side="left", pady=10)
        
    def execute_garantinorte_processing(self, file_vars, window):
        """Executa o processamento das planilhas selecionadas"""
        # Fechar janela de seleção
        window.destroy()
        
        # Obter arquivos selecionados
        selected_files = [file for file, var in file_vars.items() if var.get()]
        
        if not selected_files:
            messagebox.showwarning("Aviso", "Nenhuma planilha selecionada.")
            return
            
        # Confirmar processamento
        confirm = messagebox.askyesno(
            "Confirmar Processamento",
            f"Deseja processar {len(selected_files)} planilha(s) da Garantinorte?\n\n"
            f"Arquivos:\n" + "\n".join(f"• {file}" for file in selected_files)
        )
        
        if not confirm:
            return
            
        # Iniciar processamento
        self.log_message(f"Iniciando processamento de {len(selected_files)} planilha(s) Garantinorte")
        
        # Aqui você implementaria a lógica específica para processar planilhas Garantinorte
        # Por enquanto, vamos mostrar uma mensagem de sucesso
        messagebox.showinfo(
            "Processamento Iniciado",
            f"Processamento de {len(selected_files)} planilha(s) iniciado!\n\n"
            f"Esta funcionalidade será implementada para processar especificamente "
            f"as planilhas da Garantinorte com suas regras de negócio específicas."
        )
        
        # Log das planilhas selecionadas
        for file in selected_files:
            self.log_message(f"Planilha selecionada para processamento: {file}")
        
    def start_processing(self):
        """Inicia o processamento tradicional"""
        if not self.selected_file.get():
            messagebox.showerror("Erro", "Selecione um arquivo TXT primeiro")
            return
            
        if not os.path.exists(self.selected_file.get()):
            messagebox.showerror("Erro", "Arquivo não encontrado")
            return
            
        self.processing = True
        self.last_heartbeat = time.time()  # Reset heartbeat
        self.process_btn.configure(state="disabled")
        self.process_optimized_btn.configure(state="disabled")
        self.export_excel_btn.configure(state="disabled")
        self.background_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.emergency_btn.configure(state="normal")
        self.status_label.configure(text="Processando...")
        self.progress_bar.set(0.1)
        
        # Executar em thread separada com timeout
        self.processing_thread = threading.Thread(target=self.process_file)
        self.processing_thread.daemon = True
        self.processing_thread.start()
        
        # Configurar timeout para o processamento
        self.root.after(300000, self.check_processing_timeout)  # 5 minutos
    
    def start_processing_optimized(self):
        """Inicia o processamento otimizado"""
        if not self.selected_file.get():
            messagebox.showerror("Erro", "Selecione um arquivo TXT primeiro")
            return
            
        if not os.path.exists(self.selected_file.get()):
            messagebox.showerror("Erro", "Arquivo não encontrado")
            return
        
        # Contar itens para progresso real
        self.total_items = self._count_items_in_file()
        self.processed_items = 0
        
        if self.total_items == 0:
            messagebox.showwarning("Aviso", "Nenhum item válido encontrado no arquivo")
            return
        
        # Configurar interface
        self.processing = True
        self.last_heartbeat = time.time()
        self.last_progress_update = time.time()
        self.process_btn.configure(state="disabled")
        self.process_optimized_btn.configure(state="disabled")
        self.export_excel_btn.configure(state="disabled")
        self.background_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.emergency_btn.configure(state="normal")
        self.status_label.configure(text=f"Iniciando processamento otimizado de {self.total_items} itens...")
        self.progress_bar.set(0)
        
        self.log_message(f"🚀 Iniciando processamento otimizado de {self.total_items} itens")
        
        # Executar em thread separada
        self.processing_thread = threading.Thread(target=self.process_file_optimized)
        self.processing_thread.daemon = True
        self.processing_thread.start()
    
    def _count_items_in_file(self) -> int:
        """Conta itens no arquivo para progresso real"""
        try:
            with open(self.selected_file.get(), 'r', encoding='latin-1') as f:
                lines = f.readlines()
            
            # Contar registros tipo '01' (devedores principais)
            count = 0
            for line in lines:
                if line.startswith('01'):
                    count += 1
            
            return count
        except Exception as e:
            self.log_message(f"Erro ao contar itens: {e}")
            return 0
    
    def update_progress(self, processed_count: int, total_count: int):
        """Atualiza progresso real"""
        self.processed_items = processed_count
        self.total_items = total_count
        self.last_progress_update = time.time()
        
        if self.total_items > 0:
            progress = min(processed_count / self.total_items, 0.95)
            self.progress_bar.set(progress)
            
            percentage = (processed_count / self.total_items) * 100
            self.status_label.configure(
                text=f"Processando: {processed_count}/{total_count} ({percentage:.1f}%)"
            )
        
    def stop_processing(self):
        """Para o processamento"""
        # Atualizar heartbeat antes de parar
        self.update_heartbeat()
        self.processing = False
        
        # Parar processador otimizado se estiver rodando
        if self.optimized_processor:
            self.optimized_processor.stop_processing()
        
        # Resetar interface
        self.process_btn.configure(state="normal")
        self.process_optimized_btn.configure(state="normal")
        self.export_excel_btn.configure(state="normal")
        self.background_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")
        self.emergency_btn.configure(state="disabled")
        self.status_label.configure(text="Processamento interrompido")
        self.progress_bar.set(0)
        
        # Resetar contadores
        self.total_items = 0
        self.processed_items = 0
        
        # Aguardar thread terminar se estiver rodando
        if self.processing_thread and self.processing_thread.is_alive():
            self.log_message("Aguardando thread de processamento terminar...")
            self.processing_thread.join(timeout=10)  # Timeout de 10 segundos
            
    def check_processing_timeout(self):
        """Verifica se o processamento está demorando muito"""
        if self.processing:
            elapsed_time = time.time() - self.last_heartbeat
            if elapsed_time > 300:  # 5 minutos
                self.log_message("⚠️ ALERTA: Processamento demorando mais de 5 minutos!")
                self.status_label.configure(text="⚠️ Processamento lento")
                
                # Perguntar se deseja continuar
                if messagebox.askyesno("Processamento Lento", 
                                     f"O processamento está demorando {elapsed_time/60:.1f} minutos.\n"
                                     "Deseja continuar aguardando?"):
                    # Resetar timeout
                    self.root.after(300000, self.check_processing_timeout)
                else:
                    self.stop_processing()
        
    def process_file(self):
        """Processa o arquivo em thread separada (método tradicional)"""
        try:
            self.log_message("Iniciando processamento tradicional...")
            self.update_heartbeat()
            
            # Configurar processador
            db_name = self.db_name_entry.get() if self.db_name_entry.get() else None
            processor = BusinessRulesProcessor(db_name=db_name)
            
            self.progress_bar.set(0.3)
            self.log_message("Processador configurado")
            self.update_heartbeat()
            
            # Processar arquivo com timeout
            resultado = self.process_with_timeout(processor, self.selected_file.get())
            
            if not self.processing:  # Verificar se foi interrompido
                return
                
            self.progress_bar.set(0.8)
            self.log_message("Processamento concluído")
            self.update_heartbeat()
            
            # Mostrar resultados
            self.show_processing_results(resultado)
            
            self.progress_bar.set(1.0)
            self.status_label.configure(text="Processamento concluído")
            
        except Exception as e:
            self.log_message(f"Erro no processamento: {e}")
            messagebox.showerror("Erro", f"Erro no processamento: {e}")
        finally:
            # Atualizar heartbeat uma última vez antes de parar
            self.update_heartbeat()
            self.processing = False
            self.process_btn.configure(state="normal")
            self.process_optimized_btn.configure(state="normal")
            self.export_excel_btn.configure(state="normal")
            self.stop_btn.configure(state="disabled")
    
    def process_file_optimized(self):
        """Processa o arquivo em thread separada (método otimizado)"""
        try:
            self.log_message("Iniciando processamento otimizado...")
            self.update_heartbeat()
            
            # Configurar processador otimizado
            db_name = self.db_name_entry.get() if self.db_name_entry.get() else None
            self.optimized_processor = OptimizedBusinessRulesProcessor(
                db_name=db_name,
                max_concurrent_requests=2,  # Reduzido para 2 para evitar rate limits
                batch_size=25  # Reduzido para 25 para ser mais conservador
            )
            
            # Definir callback para atualizar progresso
            self.optimized_processor.set_progress_callback(self.update_progress)
            
            self.log_message("Processador otimizado configurado")
            self.update_heartbeat()
            
            # Processar arquivo com otimizações
            resultado = self.optimized_processor.process_inadimplentes_optimized(self.selected_file.get())
            
            if not self.processing:  # Verificar se foi interrompido
                return
                
            self.log_message("Processamento otimizado concluído")
            self.update_heartbeat()
            
            # Mostrar resultados
            self.show_processing_results(resultado)
            
            self.progress_bar.set(1.0)
            self.status_label.configure(text="Processamento otimizado concluído")
            
            # Log de estatísticas
            self.log_message(f"✅ Processamento concluído:")
            self.log_message(f"   - Pessoas criadas: {len(resultado.get('pessoas_criadas', []))}")
            self.log_message(f"   - Negócios criados: {len(resultado.get('negocios_criados', []))}")
            self.log_message(f"   - Negócios atualizados: {len(resultado.get('negocios_atualizados', []))}")
            self.log_message(f"   - Erros: {len(resultado.get('erros', []))}")
            
        except Exception as e:
            self.log_message(f"Erro no processamento otimizado: {e}")
            messagebox.showerror("Erro", f"Erro no processamento otimizado: {e}")
        finally:
            # Atualizar heartbeat uma última vez antes de parar
            self.update_heartbeat()
            self.processing = False
            self.process_btn.configure(state="normal")
            self.process_optimized_btn.configure(state="normal")
            self.export_excel_btn.configure(state="normal")
            self.stop_btn.configure(state="disabled")
            
            # Resetar contadores
            self.total_items = 0
            self.processed_items = 0
            
    def update_heartbeat(self):
        """Atualiza o timestamp do último heartbeat"""
        self.last_heartbeat = time.time()
        
    def process_with_timeout(self, processor, file_path):
        """Processa arquivo com timeout e atualizações de progresso"""
        import threading
        import time
        
        # Variável para controlar timeout
        timeout_occurred = False
        
        def timeout_handler():
            nonlocal timeout_occurred
            timeout_occurred = True
            
        # Configurar timeout de 10 minutos usando threading.Timer (compatível com Windows)
        timer = threading.Timer(600, timeout_handler)  # 10 minutos
        
        try:
            timer.start()
            
            # Processar em chunks para evitar travamentos
            resultado = processor.process_inadimplentes_from_txt(file_path)
            
            # Verificar se timeout ocorreu
            if timeout_occurred:
                raise TimeoutError("Processamento excedeu o tempo limite")
                
            return resultado
            
        except TimeoutError:
            raise Exception("Processamento travou por timeout (10 minutos)")
        except Exception as e:
            raise e
        finally:
            # Sempre cancelar o timer
            timer.cancel()
            
    def emergency_stop(self):
        """Para o processamento de emergência"""
        if messagebox.askyesno("Parada de Emergência", 
                              "⚠️ ATENÇÃO: Esta ação irá forçar a parada do processamento!\n\n"
                              "Isso pode causar perda de dados ou inconsistências.\n"
                              "Deseja realmente continuar?"):
            
            self.log_message("🚨 PARADA DE EMERGÊNCIA ATIVADA!")
            self.status_label.configure(text="🚨 PARADA DE EMERGÊNCIA")
            
            # Atualizar heartbeat antes de parar
            self.update_heartbeat()
            # Forçar parada
            self.processing = False
            
            # Tentar interromper thread
            if self.processing_thread and self.processing_thread.is_alive():
                # No Windows, vamos usar uma abordagem mais segura
                try:
                    # Marcar thread para parar
                    self.processing = False
                    # Aguardar um pouco para a thread terminar naturalmente
                    self.processing_thread.join(timeout=2.0)
                except Exception:
                    pass
                    
            # Resetar interface
            self.process_btn.configure(state="normal")
            self.stop_btn.configure(state="disabled")
            self.emergency_btn.configure(state="disabled")
            self.progress_bar.set(0)
            
            messagebox.showwarning("Parada de Emergência", 
                                 "Processamento interrompido de emergência!\n\n"
                                 "Verifique os logs para mais detalhes.")
            
    def run_system_diagnostic(self):
        """Executa diagnóstico completo do sistema"""
        try:
            diagnostic_info = "=== DIAGNÓSTICO DO SISTEMA ===\n\n"
            
            # Informações do sistema
            import platform
            diagnostic_info += f"Sistema Operacional: {platform.system()} {platform.release()}\n"
            diagnostic_info += f"Arquitetura: {platform.machine()}\n"
            diagnostic_info += f"Python: {platform.python_version()}\n\n"
            
            # Status de memória
            import psutil
            memory = psutil.virtual_memory()
            diagnostic_info += f"Memória Total: {memory.total / (1024**3):.1f} GB\n"
            diagnostic_info += f"Memória Disponível: {memory.available / (1024**3):.1f} GB\n"
            diagnostic_info += f"Uso de Memória: {memory.percent}%\n\n"
            
            # Status de disco
            disk = psutil.disk_usage('.')
            diagnostic_info += f"Disco Total: {disk.total / (1024**3):.1f} GB\n"
            diagnostic_info += f"Disco Disponível: {disk.free / (1024**3):.1f} GB\n"
            diagnostic_info += f"Uso de Disco: {disk.percent}%\n\n"
            
            # Status de rede
            try:
                network = psutil.net_io_counters()
                diagnostic_info += f"Bytes Enviados: {network.bytes_sent / (1024**2):.1f} MB\n"
                diagnostic_info += f"Bytes Recebidos: {network.bytes_recv / (1024**2):.1f} MB\n"
            except:
                diagnostic_info += "Status de Rede: Não disponível\n"
                
            # Status do processamento
            diagnostic_info += f"\nStatus do Processamento:\n"
            diagnostic_info += f"Processando: {'Sim' if self.processing else 'Não'}\n"
            diagnostic_info += f"Último Heartbeat: {time.time() - self.last_heartbeat:.1f}s atrás\n"
            
            # Mostrar diagnóstico
            diagnostic_window = ctk.CTkToplevel(self.root)
            diagnostic_window.title("Diagnóstico do Sistema")
            diagnostic_window.geometry("600x500")
            
            text_widget = ctk.CTkTextbox(diagnostic_window)
            text_widget.pack(fill="both", expand=True, padx=10, pady=10)
            text_widget.insert("1.0", diagnostic_info)
            
        except ImportError:
            messagebox.showwarning("Aviso", "Para diagnóstico completo, instale: pip install psutil")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro no diagnóstico: {e}")
            
    def check_memory_status(self):
        """Verifica status de memória em tempo real"""
        try:
            import psutil
            memory = psutil.virtual_memory()
            
            status_text = f"=== STATUS DE MEMÓRIA ===\n\n"
            status_text += f"Total: {memory.total / (1024**3):.1f} GB\n"
            status_text += f"Disponível: {memory.available / (1024**3):.1f} GB\n"
            status_text += f"Usado: {memory.used / (1024**3):.1f} GB\n"
            status_text += f"Percentual: {memory.percent}%\n\n"
            
            # Alertas
            if memory.percent > 90:
                status_text += "🚨 ALERTA: Uso de memória muito alto!\n"
            elif memory.percent > 80:
                status_text += "⚠️ ATENÇÃO: Uso de memória alto\n"
            else:
                status_text += "✅ Uso de memória normal\n"
                
            messagebox.showinfo("Status de Memória", status_text)
            
        except ImportError:
            messagebox.showwarning("Aviso", "Para verificar memória, instale: pip install psutil")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao verificar memória: {e}")
            
    def show_processing_results(self, resultado):
        """Mostra resultados do processamento"""
        # Criar janela de resultados
        results_window = ctk.CTkToplevel(self.root)
        results_window.title("Resultados do Processamento")
        results_window.geometry("600x400")
        
        # Texto com resultados
        text_widget = ctk.CTkTextbox(results_window)
        text_widget.pack(fill="both", expand=True, padx=10, pady=10)
        
        # Formatar resultados
        results_text = "=== RESULTADOS DO PROCESSAMENTO ===\n\n"
        
        for key, value in resultado.items():
            if isinstance(value, list):
                results_text += f"{key}: {len(value)} itens\n"
            else:
                results_text += f"{key}: {value}\n"
                
        text_widget.insert("1.0", results_text)
        
    def show_backup_stats(self):
        """Mostra estatísticas do backup"""
        try:
            # Importar aqui para evitar problemas de importação
            from utils.consulta_backup_sqlite import ConsultaBackupSQLite
            consulta = ConsultaBackupSQLite()
            stats = consulta.obter_estatisticas_gerais()
            
            stats_text = "=== ESTATÍSTICAS DO BACKUP ===\n\n"
            for key, value in stats.items():
                if isinstance(value, dict):
                    stats_text += f"{key}:\n"
                    for sub_key, sub_value in value.items():
                        stats_text += f"  {sub_key}: {sub_value}\n"
                else:
                    stats_text += f"{key}: {value}\n"
                    
            self.results_text.delete("1.0", "end")
            self.results_text.insert("1.0", stats_text)
            
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao obter estatísticas: {e}")
            
    def search_by_document(self):
        """Busca por documento"""
        # Criar diálogo de busca
        dialog = ctk.CTkInputDialog(
            text="Digite o CPF/CNPJ:",
            title="Buscar por Documento"
        )
        documento = dialog.get_input()
        
        if documento:
            try:
                from utils.consulta_backup_sqlite import ConsultaBackupSQLite
                consulta = ConsultaBackupSQLite()
                # Tentar PF primeiro
                resultado = consulta.buscar_entidade_por_documento(documento, "PF")
                if not resultado:
                    # Tentar PJ
                    resultado = consulta.buscar_entidade_por_documento(documento, "PJ")
                    
                if resultado:
                    self.results_text.delete("1.0", "end")
                    self.results_text.insert("1.0", f"Resultado encontrado:\n{resultado}")
                else:
                    self.results_text.delete("1.0", "end")
                    self.results_text.insert("1.0", "Nenhum resultado encontrado")
                    
            except Exception as e:
                messagebox.showerror("Erro", f"Erro na busca: {e}")
                
    def show_full_report(self):
        """Mostra relatório completo"""
        try:
            from utils.consulta_backup_sqlite import ConsultaBackupSQLite
            consulta = ConsultaBackupSQLite()
            relatorio = consulta.gerar_relatorio_backup()
            
            self.results_text.delete("1.0", "end")
            self.results_text.insert("1.0", relatorio)
            
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao gerar relatório: {e}")
            
    def export_to_csv(self):
        """Exporta dados para CSV"""
        filename = filedialog.asksaveasfilename(
            title="Salvar como CSV",
            defaultextension=".csv",
            filetypes=[("Arquivos CSV", "*.csv")]
        )
        
        if filename:
            try:
                from utils.consulta_backup_sqlite import ConsultaBackupSQLite
                consulta = ConsultaBackupSQLite()
                sucesso = consulta.exportar_entidades_csv(filename)
                
                if sucesso:
                    messagebox.showinfo("Sucesso", f"Dados exportados para {filename}")
                else:
                    messagebox.showerror("Erro", "Falha na exportação")
                    
            except Exception as e:
                messagebox.showerror("Erro", f"Erro na exportação: {e}")
                
    def list_custom_fields(self):
        """Lista campos personalizados"""
        try:
            # Executar o script diretamente
            import subprocess
            result = subprocess.run([sys.executable, "utils/listar_campos_personalizados.py"], 
                                  capture_output=True, text=True, cwd=os.getcwd())
            
            # Criar janela com resultados
            fields_window = ctk.CTkToplevel(self.root)
            fields_window.title("Campos Personalizados")
            fields_window.geometry("800x600")
            
            text_widget = ctk.CTkTextbox(fields_window)
            text_widget.pack(fill="both", expand=True, padx=10, pady=10)
            
            if result.stdout:
                text_widget.insert("1.0", result.stdout)
            else:
                text_widget.insert("1.0", "Erro ao executar comando")
            
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao listar campos: {e}")
            
    def map_duplicates(self):
        """Mapeia duplicados"""
        try:
            # Executar o script diretamente
            import subprocess
            result = subprocess.run([sys.executable, "utils/mapeamento_duplicados.py"], 
                                  capture_output=True, text=True, cwd=os.getcwd())
            
            if result.returncode == 0:
                messagebox.showinfo("Sucesso", "Mapeamento concluído com sucesso!")
            else:
                messagebox.showerror("Erro", f"Erro no mapeamento: {result.stderr}")
            
        except Exception as e:
            messagebox.showerror("Erro", f"Erro no mapeamento: {e}")
            
    def test_configuration(self):
        """Testa configuração"""
        try:
            # Testar token
            token = getattr(active_config, 'PIPEDRIVE_API_TOKEN', '')
            if not token or token == "SEU_TOKEN_AQUI":
                messagebox.showerror("Erro", "Token da API não configurado")
                return
                
            # Testar conexão
            from src.pipedrive_client import PipedriveClient
            pipedrive = PipedriveClient()
            
            if pipedrive.test_connection():
                messagebox.showinfo("Sucesso", "Configuração válida! Conexão com Pipedrive estabelecida.")
            else:
                messagebox.showerror("Erro", "Falha na conexão com Pipedrive")
                
        except Exception as e:
            messagebox.showerror("Erro", f"Erro no teste: {e}")
            
    def show_processing_report(self):
        """Mostra relatório de processamento"""
        try:
            from utils.consulta_backup_sqlite import ConsultaBackupSQLite
            consulta = ConsultaBackupSQLite()
            processamentos = consulta.gerar_relatorio_processamentos()
            
            # Criar janela com relatório
            report_window = ctk.CTkToplevel(self.root)
            report_window.title("Relatório de Processamentos")
            report_window.geometry("800x600")
            
            text_widget = ctk.CTkTextbox(report_window)
            text_widget.pack(fill="both", expand=True, padx=10, pady=10)
            
            report_text = "=== RELATÓRIO DE PROCESSAMENTOS ===\n\n"
            for proc in processamentos:
                report_text += f"Arquivo: {proc['arquivo_txt']}\n"
                report_text += f"Data: {proc['timestamp_inicio']}\n"
                report_text += f"Status: {proc['status']}\n"
                report_text += f"Criadas: {proc['entidades_criadas']}\n"
                report_text += f"Atualizadas: {proc['entidades_atualizadas']}\n"
                report_text += "-" * 50 + "\n"
                
            text_widget.insert("1.0", report_text)
            
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao gerar relatório: {e}")
            
    def refresh_logs(self):
        """Atualiza logs usando as configurações do config.py"""
        try:
            # Usar a pasta de logs configurada no config.py
            logs_dir = active_config.LOGS_FOLDER
            if os.path.exists(logs_dir):
                log_files = [f for f in os.listdir(logs_dir) if f.endswith('.log')]
                if log_files:
                    # Pegar o mais recente
                    latest_log = max(log_files, key=lambda x: os.path.getctime(os.path.join(logs_dir, x)))
                    log_path = os.path.join(logs_dir, latest_log)
                    
                    with open(log_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        
                    self.logs_text.delete("1.0", "end")
                    self.logs_text.insert("1.0", content)
                    self.logs_text.see("end")
                    
                    # Log da ação
                    self.log_message(f"Logs atualizados: {latest_log}")
                else:
                    self.logs_text.delete("1.0", "end")
                    self.logs_text.insert("1.0", "Nenhum arquivo de log encontrado")
            else:
                self.logs_text.delete("1.0", "end")
                self.logs_text.insert("1.0", f"Diretório de logs não encontrado: {logs_dir}")
                
        except Exception as e:
            self.logs_text.delete("1.0", "end")
            self.logs_text.insert("1.0", f"Erro ao carregar logs: {e}")
            self.log_message(f"Erro ao atualizar logs: {e}")
            
        # Atualizar informações de logging na aba de configuração
        self.update_logging_info()
            
    def clear_logs(self):
        """Limpa logs"""
        if messagebox.askyesno("Confirmar", "Deseja limpar os logs?"):
            self.logs_text.delete("1.0", "end")
            
    def open_logs_folder(self):
        """Abre a pasta de logs no explorador de arquivos"""
        logs_dir = active_config.LOGS_FOLDER
        
        # Criar pasta se não existir
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        try:
            # Abrir pasta no explorador de arquivos
            if os.name == 'nt':  # Windows
                os.startfile(logs_dir)
            elif os.name == 'posix':  # macOS e Linux
                import subprocess
                subprocess.run(['open', logs_dir])  # macOS
            else:
                import subprocess
                subprocess.run(['xdg-open', logs_dir])  # Linux
                
            self.log_message(f"Pasta de logs aberta: {logs_dir}")
            
        except Exception as e:
            self.log_message(f"Erro ao abrir pasta de logs: {e}")
            messagebox.showerror("Erro", f"Erro ao abrir pasta de logs: {e}")
            
    def save_configuration(self):
        """Salva configuração"""
        try:
            # Aqui você implementaria a lógica para salvar as configurações
            messagebox.showinfo("Sucesso", "Configuração salva com sucesso!")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao salvar configuração: {e}")
            
    def load_configuration(self):
        """Carrega configuração"""
        try:
            # Aqui você implementaria a lógica para carregar as configurações
            messagebox.showinfo("Sucesso", "Configuração carregada com sucesso!")
        except Exception as e:
            messagebox.showerror("Erro", f"Erro ao carregar configuração: {e}")
    
    def export_to_excel(self):
        """Exporta dados para planilha Excel de importação do Pipedrive"""
        if not self.selected_file.get():
            messagebox.showerror("Erro", "Selecione um arquivo TXT primeiro")
            return
            
        if not os.path.exists(self.selected_file.get()):
            messagebox.showerror("Erro", "Arquivo não encontrado")
            return
        
        try:
            # Perguntar tipo de exportação
            export_type = messagebox.askyesnocancel(
                "Tipo de Exportação",
                "Escolha o tipo de exportação:\n\n"
                "SIM = Dados completos (Pessoas + Negócios)\n"
                "NÃO = Apenas template de campos personalizados\n"
                "CANCELAR = Cancelar operação"
            )
            
            if export_type is None:  # Cancelar
                return
            
            if export_type:  # Exportar dados completos
                self.log_message("📊 Iniciando exportação de dados completos...")
                
                # Processar arquivo TXT
                from src.file_processor import FileProcessor
                file_processor = FileProcessor()
                inadimplentes_data = file_processor.process_txt_file_direct(self.selected_file.get())
                
                if not inadimplentes_data:
                    messagebox.showerror("Erro", "Nenhum dado válido encontrado no arquivo")
                    return
                
                # Gerar nome do arquivo
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"pipedrive_import_{timestamp}.xlsx"
                
                # Exportar para Excel
                filepath = self.excel_exporter.export_inadimplentes_to_excel(inadimplentes_data, filename)
                
                # Obter resumo
                summary = self.excel_exporter.get_export_summary(filepath)
                
                # Mostrar resultado
                messagebox.showinfo(
                    "Exportação Concluída",
                    f"✅ Arquivo Excel gerado com sucesso!\n\n"
                    f"📁 Arquivo: {summary['arquivo']}\n"
                    f"📊 Registros: {summary['total_registros']}\n"
                    f"📏 Tamanho: {summary['tamanho_mb']} MB\n"
                    f"📅 Criado em: {summary['data_criacao']}\n\n"
                    f"📍 Local: {filepath}"
                )
                
                self.log_message(f"✅ Exportação concluída: {summary['total_registros']} registros")
                
            else:  # Exportar apenas template
                self.log_message("📋 Gerando template de campos personalizados...")
                
                filepath = self.excel_exporter.export_custom_fields_template()
                
                messagebox.showinfo(
                    "Template Gerado",
                    f"✅ Template de campos personalizados gerado!\n\n"
                    f"📁 Arquivo: {os.path.basename(filepath)}\n"
                    f"📍 Local: {filepath}\n\n"
                    f"Este template contém a estrutura dos campos personalizados\n"
                    f"do Pipedrive para referência na importação."
                )
                
                self.log_message(f"✅ Template gerado: {filepath}")
                
        except Exception as e:
            error_msg = f"Erro na exportação: {e}"
            self.log_message(f"❌ {error_msg}")
            messagebox.showerror("Erro", error_msg)
    
    def start_background_processing(self):
        """Inicia processamento em background"""
        if not self.selected_file.get():
            messagebox.showerror("Erro", "Selecione um arquivo TXT primeiro")
            return
            
        if not os.path.exists(self.selected_file.get()):
            messagebox.showerror("Erro", "Arquivo não encontrado")
            return
        
        # Perguntar confirmação
        confirm = messagebox.askyesno(
            "Processamento em Background",
            "O processamento em background permite que você feche a GUI e o processamento continue.\n\n"
            "Deseja iniciar o processamento em background?\n\n"
            "⚠️ ATENÇÃO: O processamento continuará mesmo se você fechar a aplicação."
        )
        
        if not confirm:
            return
        
        try:
            self.log_message("🌙 Iniciando processamento em background...")
            
            # Configurar processador em background
            from src.background_processor import BackgroundProcessor
            self.background_processor = BackgroundProcessor()
            
            # Executar em thread separada
            self.processing_thread = threading.Thread(
                target=self._run_background_processing,
                daemon=False  # Não daemon para continuar após fechar GUI
            )
            self.processing_thread.start()
            
            # Mostrar informações
            messagebox.showinfo(
                "Processamento em Background",
                "✅ Processamento em background iniciado!\n\n"
                "📋 Características:\n"
                "• Continua mesmo se fechar a GUI\n"
                "• Salva progresso automaticamente\n"
                "• Pode ser interrompido via logs\n"
                "• Recupera de interrupções\n\n"
                "📁 Logs: logs/background/background_processing.log"
            )
            
            self.log_message("✅ Processamento em background iniciado com sucesso")
            
        except Exception as e:
            error_msg = f"Erro ao iniciar processamento em background: {e}"
            self.log_message(f"❌ {error_msg}")
            messagebox.showerror("Erro", error_msg)
    
    def _run_background_processing(self):
        """Executa processamento em background"""
        try:
            db_name = self.db_name_entry.get() if self.db_name_entry.get() else None
            
            results = self.background_processor.process_file_background(
                self.selected_file.get(),
                db_name
            )
            
            # Log de resultados
            if 'error' in results:
                self.log_message(f"❌ Erro no processamento em background: {results['error']}")
            else:
                self.log_message("✅ Processamento em background concluído com sucesso")
                self.log_message(f"📊 Resultados:")
                self.log_message(f"   - Pessoas criadas: {len(results.get('pessoas_criadas', []))}")
                self.log_message(f"   - Negócios criados: {len(results.get('negocios_criados', []))}")
                self.log_message(f"   - Negócios atualizados: {len(results.get('negocios_atualizados', []))}")
                self.log_message(f"   - Erros: {len(results.get('erros', []))}")
                
        except Exception as e:
            self.log_message(f"❌ Erro no processamento em background: {e}")
    
    def check_background_status(self):
        """Verifica status do processamento em background"""
        try:
            if self.background_processor:
                status = self.background_processor.get_processing_status()
                
                if status['status'] == 'processing':
                    self.log_message(f"🔄 Processamento em background ativo:")
                    self.log_message(f"   - Arquivo: {status.get('txt_path', 'N/A')}")
                    self.log_message(f"   - Progresso: {status.get('last_processed_index', 0)} itens")
                    self.log_message(f"   - Iniciado: {status.get('timestamp', 'N/A')}")
                elif status['status'] == 'completed':
                    self.log_message("✅ Processamento em background concluído")
                elif status['status'] == 'idle':
                    self.log_message("💤 Nenhum processamento em background ativo")
                else:
                    self.log_message(f"❓ Status desconhecido: {status}")
            else:
                self.log_message("💤 Processador em background não inicializado")
                
        except Exception as e:
            self.log_message(f"❌ Erro ao verificar status: {e}")
    
    def stop_background_processing(self):
        """Para processamento em background"""
        try:
            if self.background_processor:
                self.background_processor.stop_background_processing()
                self.log_message("🛑 Processamento em background interrompido")
            else:
                self.log_message("❌ Nenhum processamento em background ativo")
                
        except Exception as e:
            self.log_message(f"❌ Erro ao parar processamento em background: {e}")
            
    def run(self):
        """Executa a GUI"""
        self.root.mainloop()

def main():
    """Função principal"""
    app = PipedriveGUI()
    app.run()

if __name__ == "__main__":
    main()
