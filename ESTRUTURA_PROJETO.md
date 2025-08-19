# Estrutura do Projeto Pipedrive

Este documento descreve a estrutura do projeto Pipedrive para processamento de inadimplentes.

## Visão Geral

O projeto está organizado em módulos específicos para facilitar manutenção e extensão:

```
Projeto Gustavo/
├── src/                          # Código principal
│   ├── config.py                 # Configurações do sistema
│   ├── pipedrive_client.py       # Cliente da API Pipedrive
│   ├── file_processor.py         # Processamento de arquivos TXT
│   ├── business_rules.py         # Regras de negócio
│   └── custom_fields_config.py   # Configuração de campos personalizados
├── utils/                        # Utilitários
│   ├── backup_sqlite.py          # Sistema de backup SQLite
│   ├── consulta_backup_sqlite.py # Consulta de dados do backup
│   ├── listar_campos_personalizados.py # Listagem de campos
│   ├── mapeamento_duplicados.py  # Mapeamento de duplicados
│   └── menu_principal.py         # Menu principal de utilitários
├── input/                        # Arquivos de entrada
│   ├── escritorio_cobranca/      # Arquivos TXT de inadimplentes
│   └── garantinorte/             # Planilhas de referência
├── logs/                         # Logs do sistema
├── backup/                       # Backups SQLite
└── main.py                       # Script principal
```

## Módulos Principais

### src/ - Código Principal

#### config.py
- Configurações do sistema
- Tokens da API Pipedrive
- IDs dos funis e etapas
- Configurações de logging

#### pipedrive_client.py
- Cliente da API Pipedrive
- Métodos para criar/atualizar pessoas, organizações e negócios
- Tratamento de erros da API
- Formatação de dados para API v2

#### file_processor.py
- Processamento de arquivos TXT
- Extração e normalização de dados
- Consolidação de informações
- Preparação para Pipedrive

#### business_rules.py
- Regras de negócio do sistema com backup SQLite integrado
- Lógica de processamento de inadimplentes
- Aplicação de regras específicas
- Integração com Pipedrive e backup automático

#### custom_fields_config.py
- Configuração de campos personalizados
- Mapeamento de nomes para IDs
- Validação de tipos de campos
- Configuração dinâmica

### utils/ - Utilitários

#### backup_sqlite.py
- Sistema de backup usando SQLite
- Armazenamento de dados processados
- Consulta de histórico
- Validação de integridade

#### consulta_backup_sqlite.py
- Interface para consultar backup
- Relatórios de processamento
- Estatísticas do sistema
- Busca por documento/nome

#### listar_campos_personalizados.py
- Listagem de campos personalizados
- Verificação de configuração
- Validação de IDs
- Documentação de campos

#### mapeamento_duplicados.py
- Identificação de duplicados
- Relatórios Excel
- Sugestões de consolidação
- Categorização de casos

#### menu_principal.py
- Menu interativo
- Acesso a utilitários
- Validação de configurações
- Interface amigável

## Arquivos de Entrada

### input/escritorio_cobranca/
- Arquivos TXT de inadimplentes
- Formato estruturado
- Dados de devedores
- Informações de avalistas

### input/garantinorte/
- Planilhas de referência
- Dados de contratos
- Informações complementares
- Cruzamento de dados

## Logs e Backup

### logs/
- Logs de execução
- Timestamps únicos
- Níveis de log configuráveis
- Histórico de operações

### backup/
- Backups SQLite
- Dados processados
- Histórico completo
- Auditoria de mudanças

## Scripts Principais

### main.py
- Script principal do sistema
- Processamento de inadimplentes
- Integração com Pipedrive
- Relatórios de execução

## Configuração

### Variáveis de Ambiente
```bash
PIPEDRIVE_API_TOKEN=seu_token_aqui
PIPEDRIVE_DOMAIN=seu_dominio.pipedrive.com
```

### Arquivo de Configuração
- `src/config.py`
- Configurações centralizadas
- IDs dos funis e etapas
- Configurações de campos

## Como Usar

1. **Configurar**: Editar `src/config.py`
2. **Executar**: `python main.py`
3. **Utilitários**: `python utils/menu_principal.py`
4. **Consultas**: `python utils/consulta_backup_sqlite.py`

## Manutenção

### Logs
- Verificar logs em `logs/`
- Limpeza periódica
- Monitoramento de erros

### Backup
- Backups automáticos
- Validação de integridade
- Limpeza de backups antigos

### Configuração
- Atualizar IDs quando necessário
- Verificar tokens da API
- Validar campos personalizados 