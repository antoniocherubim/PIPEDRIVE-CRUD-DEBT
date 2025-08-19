# 🚀 Sistema Pipedrive - Inadimplentes

Sistema automatizado para processamento e integração de inadimplentes no Pipedrive, desenvolvido para cooperativas de crédito.

## 📋 Índice

- [Sobre o Projeto](#sobre-o-projeto)
- [Funcionalidades](#funcionalidades)
- [Pré-requisitos](#pré-requisitos)
- [Instalação](#instalação)
- [Configuração](#configuração)
- [Uso](#uso)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [API Reference](#api-reference)
- [Contribuindo](#contribuindo)
- [Licença](#licença)

## 🎯 Sobre o Projeto

O **Sistema Pipedrive - Inadimplentes** é uma solução completa para automatizar o processamento de dados de inadimplentes e sua integração com o Pipedrive CRM. Desenvolvido especificamente para cooperativas de crédito, o sistema oferece:

- **Processamento automatizado** de arquivos TXT de inadimplentes
- **Integração completa** com API Pipedrive v1/v2
- **Interface gráfica moderna** com CustomTkinter
- **Backup automático** em SQLite
- **Validação de dados** e tratamento de erros
- **Relatórios detalhados** de processamento

## ✨ Funcionalidades

### 🔄 Processamento de Dados
- Leitura e parsing de arquivos TXT de inadimplentes
- Validação de CPF/CNPJ
- Normalização de dados
- Consolidação de informações de devedores e avalistas

### 🔗 Integração Pipedrive
- Criação automática de pessoas (devedores)
- Criação de negócios (deals) com valores e status
- Mapeamento inteligente de campos personalizados
- Suporte às APIs v1 e v2 do Pipedrive

### 🖥️ Interface Gráfica
- Interface moderna e intuitiva
- Abas organizadas por funcionalidade
- Configuração visual de parâmetros
- Monitoramento em tempo real

### 💾 Backup e Auditoria
- Backup automático em SQLite
- Histórico completo de processamentos
- Consultas e relatórios de auditoria
- Exportação de dados

### 🛡️ Segurança
- Configuração via variáveis de ambiente
- Validação de tokens e credenciais
- Logs detalhados de operações
- Tratamento seguro de dados sensíveis

## 📋 Pré-requisitos

- **Python 3.8+**
- **Token da API Pipedrive**
- **Acesso à internet** para integração com Pipedrive
- **Arquivos TXT** de inadimplentes no formato especificado

## 🛠️ Instalação

### Opção 1: Instalação Manual

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/pipedrive-inadimplentes.git
cd pipedrive-inadimplentes

# 2. Crie um ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/macOS
# ou
venv\Scripts\activate     # Windows

# 3. Instale as dependências
pip install -r requirements.txt

# 4. Configure o ambiente
cp env_example.txt .env
# Edite o arquivo .env com suas configurações
```

### Opção 2: Instalação via Pip

```bash
# Instalar dependências
pip install requests pandas openpyxl python-dotenv customtkinter

# Instalar o sistema
pip install .

# Executar
pipedrive-gui
```

## ⚙️ Configuração

### 1. Configurar Token da API

Edite o arquivo `.env`:

```env
# Token da API Pipedrive (OBRIGATÓRIO)
PIPEDRIVE_API_TOKEN=seu_token_aqui

# Domínio do Pipedrive
PIPEDRIVE_DOMAIN=sua-empresa

# Configurações do sistema
LOG_LEVEL=INFO
BACKUP_ENABLED=true
PROCESSING_MODE=full
```

### 2. Obter Token da API Pipedrive

1. Acesse sua conta Pipedrive
2. Vá em **Settings** → **Personal Preferences** → **API**
3. Copie o token da API
4. Configure no arquivo `.env`

### 3. Configurar Campos Personalizados

O sistema mapeia automaticamente os campos personalizados. Para configuração manual, edite `src/custom_fields_config.py`.

## 🚀 Uso

### Interface Gráfica (Recomendado)

```bash
python gui_main.py
```

### Linha de Comando

```bash
# Ver opções disponíveis
python main.py --help

# Processamento completo
python main.py

# Apenas TXT → Pipedrive
python main.py --modo txt-pipedrive

# Testar configuração
python main.py --config-test
```

### Estrutura de Arquivos

```
pipedrive-inadimplentes/
├── input/
│   ├── escritorio_cobranca/  # Arquivos TXT
│   └── garantinorte/         # Planilhas de referência
├── src/                      # Código fonte
├── utils/                    # Utilitários
├── logs/                     # Logs do sistema
├── backup/                   # Backups SQLite
├── output/                   # Arquivos de saída
└── relatorios/               # Relatórios gerados
```

## 📁 Estrutura do Projeto

```
pipedrive-inadimplentes/
├── src/
│   ├── config.py                 # Configurações do sistema
│   ├── pipedrive_client.py       # Cliente da API Pipedrive
│   ├── file_processor.py         # Processamento de arquivos
│   ├── business_rules.py         # Regras de negócio
│   └── custom_fields_config.py   # Configuração de campos
├── utils/
│   ├── backup_sqlite.py          # Sistema de backup
│   ├── consulta_backup_sqlite.py # Consultas ao backup
│   └── mapeamento_duplicados.py  # Detecção de duplicados
├── gui_main.py                   # Interface gráfica
├── main.py                       # Interface de linha de comando
├── requirements.txt              # Dependências Python
├── .env.example                  # Exemplo de configuração
└── README.md                     # Documentação
```

## 🔧 API Reference

### Configuração

```python
from src.config import active_config

# Acessar configurações
token = active_config.PIPEDRIVE_API_TOKEN
domain = active_config.PIPEDRIVE_DOMAIN
```

### Cliente Pipedrive

```python
from src.pipedrive_client import PipedriveClient

client = PipedriveClient()

# Criar pessoa
person_data = {
    'name': 'João Silva',
    'email': 'joao@email.com',
    'phone': '(11) 99999-9999'
}
person = client.create_person(person_data)

# Criar negócio
deal_data = {
    'title': 'Cobrança - João Silva',
    'value': 15000.50,
    'person_id': person['id']
}
deal = client.create_deal(deal_data)
```

### Processamento de Arquivos

```python
from src.file_processor import FileProcessor

processor = FileProcessor()
data = processor.process_txt_file('input/arquivo.txt')
```

## 🤝 Contribuindo

1. **Fork** o projeto
2. Crie uma **branch** para sua feature (`git checkout -b feature/AmazingFeature`)
3. **Commit** suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. **Push** para a branch (`git push origin feature/AmazingFeature`)
5. Abra um **Pull Request**

### Padrões de Código

- Use **type hints** em todas as funções
- Documente funções com **docstrings**
- Siga o padrão **PEP 8**
- Escreva **testes** para novas funcionalidades

## 📝 Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para detalhes.

## 📞 Suporte

- **Documentação**: [Wiki do Projeto](https://github.com/seu-usuario/pipedrive-inadimplentes/wiki)
- **Issues**: [GitHub Issues](https://github.com/seu-usuario/pipedrive-inadimplentes/issues)
- **Email**: suporte@sistema.com

## 🙏 Agradecimentos

- **Pipedrive** pela excelente API
- **CustomTkinter** pela interface moderna
- **Comunidade Python** pelo suporte

---

**Sistema Pipedrive - Inadimplentes**  
*Automatizando a gestão de inadimplentes com eficiência e segurança* 🚀 