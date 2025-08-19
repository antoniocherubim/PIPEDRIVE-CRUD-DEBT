# 🖥️ GUI do Sistema Pipedrive - Inadimplentes

Interface gráfica moderna e intuitiva para o sistema de processamento de inadimplentes no Pipedrive.

## 🚀 Como Usar

### Instalação

1. **Instalar dependências:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Executar a GUI:**
   ```bash
   python gui_main.py
   ```

## 📋 Funcionalidades

### 📊 Aba de Processamento
- **Seleção de arquivo TXT**: Procurar ou auto-detect
- **Configuração de banco SQLite**: Nome customizado
- **Processamento completo**: Com barra de progresso
- **Controle de execução**: Iniciar/parar processamento
- **Status em tempo real**: Monitoramento do progresso

### 💾 Aba de Backup SQLite
- **Estatísticas gerais**: Visão geral dos dados
- **Busca por documento**: CPF/CNPJ específico
- **Relatório completo**: Dados detalhados
- **Exportação CSV**: Dados para análise externa
- **Área de resultados**: Visualização dos dados

### 🔧 Aba de Utilitários
- **Listar campos personalizados**: Configuração do Pipedrive
- **Mapear duplicados**: Identificação de registros duplicados
- **Testar configuração**: Validação da API
- **Relatório de processamento**: Histórico de execuções

### 📝 Aba de Logs
- **Visualização de logs**: Em tempo real
- **Atualização automática**: Logs mais recentes
- **Controles de log**: Atualizar/limpar
- **Histórico completo**: Todas as operações

### ⚙️ Aba de Configuração
- **Token da API**: Configuração do Pipedrive
- **Domínio**: URL da instância
- **Salvar/Carregar**: Persistência de configurações

## 🎨 Interface Moderna

### Características Visuais
- **Tema escuro**: Interface moderna e confortável
- **Material Design**: Componentes visuais atuais
- **Responsiva**: Adaptável a diferentes tamanhos
- **Ícones intuitivos**: Navegação fácil
- **Cores consistentes**: Padrão visual unificado

### Experiência do Usuário
- **Navegação por abas**: Organização clara
- **Feedback visual**: Status e progresso
- **Diálogos informativos**: Mensagens claras
- **Validação de entrada**: Prevenção de erros
- **Threading**: Interface não trava durante processamento

## 🔧 Configuração

### Pré-requisitos
- Python 3.8+
- CustomTkinter 5.2.0+
- Todas as dependências do sistema principal

### Configuração Inicial
1. **Token da API**: Configure no arquivo `src/config.py`
2. **Domínio Pipedrive**: URL da sua instância
3. **Arquivos de entrada**: Coloque na pasta `input/`

## 📱 Compatibilidade

### Sistemas Operacionais
- ✅ **Windows**: Totalmente compatível
- ✅ **macOS**: Compatível
- ✅ **Linux**: Compatível

### Resoluções
- **Mínima**: 1000x600 pixels
- **Recomendada**: 1200x800 pixels ou superior
- **Responsiva**: Adapta-se a diferentes tamanhos

## 🚨 Tratamento de Erros

### Validações
- **Arquivo TXT**: Verificação de existência e formato
- **Configuração**: Validação de token e domínio
- **Conexão**: Teste de conectividade com Pipedrive
- **Processamento**: Tratamento de erros em tempo real

### Feedback ao Usuário
- **Mensagens de erro**: Explicações claras
- **Diálogos informativos**: Confirmações importantes
- **Logs detalhados**: Histórico completo de operações
- **Status visual**: Indicadores de progresso

## 🔄 Integração

### Sistema Principal
- **BusinessRulesProcessor**: Processamento completo
- **Backup SQLite**: Sistema de backup automático
- **Utilitários**: Todas as ferramentas disponíveis
- **Configuração**: Acesso às configurações do sistema

### Fluxo de Trabalho
1. **Selecionar arquivo**: TXT de inadimplentes
2. **Configurar banco**: SQLite personalizado (opcional)
3. **Iniciar processamento**: Com monitoramento
4. **Acompanhar progresso**: Logs em tempo real
5. **Visualizar resultados**: Estatísticas e relatórios
6. **Consultar backup**: Dados históricos

## 📊 Monitoramento

### Indicadores Visuais
- **Barra de progresso**: Progresso do processamento
- **Status label**: Estado atual da operação
- **Logs em tempo real**: Atualizações automáticas
- **Botões de controle**: Iniciar/parar processamento

### Métricas Disponíveis
- **Entidades processadas**: Contadores em tempo real
- **Tempo de execução**: Duração das operações
- **Taxa de sucesso**: Percentual de operações bem-sucedidas
- **Erros encontrados**: Detalhamento de problemas

## 🎯 Benefícios

### Para o Usuário Final
- **Interface intuitiva**: Fácil de usar
- **Feedback imediato**: Status em tempo real
- **Controle total**: Iniciar/parar quando necessário
- **Visualização clara**: Dados organizados
- **Automação**: Processamento sem intervenção manual

### Para o Sistema
- **Integração completa**: Todas as funcionalidades
- **Robustez**: Tratamento de erros
- **Performance**: Threading para não travar
- **Manutenibilidade**: Código organizado
- **Escalabilidade**: Fácil de expandir

## 🚀 Próximos Passos

### Melhorias Futuras
- **Temas personalizáveis**: Claro/escuro/sistema
- **Atalhos de teclado**: Navegação rápida
- **Exportação avançada**: Múltiplos formatos
- **Dashboard**: Métricas visuais
- **Notificações**: Alertas de conclusão

### Funcionalidades Adicionais
- **Agendamento**: Processamento automático
- **Relatórios gráficos**: Gráficos e estatísticas
- **Configuração avançada**: Mais opções
- **Backup remoto**: Sincronização automática
- **API REST**: Interface web

---

**Desenvolvido com CustomTkinter** - Interface moderna e funcional para o Sistema Pipedrive
