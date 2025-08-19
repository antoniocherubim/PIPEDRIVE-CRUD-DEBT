# 🔧 Guia de Configuração - IDs do Pipedrive

Este guia ajudará você a identificar e configurar os IDs corretos dos funis e etapas no seu Pipedrive.

## 📋 Pré-requisitos

- Acesso administrativo ao Pipedrive
- Token da API configurado
- Python instalado com as dependências

## 🔍 Identificando os IDs dos Funis (Pipelines)

### Método 1: Script de Identificação

Execute o script auxiliar para listar todos os funis:

```python
# Criar arquivo: get_pipelines.py
from pipedrive_client import PipedriveClient

client = PipedriveClient()
pipelines = client.get_pipelines()

print("=== FUNIS DISPONÍVEIS ===")
for pipeline in pipelines:
    print(f"ID: {pipeline['id']} - Nome: {pipeline['name']}")
```

Execute:
```bash
python get_pipelines.py
```

### Método 2: Interface do Pipedrive

1. Acesse seu Pipedrive
2. Vá em **Configurações** → **Funis e etapas**
3. Clique em cada funil para ver o ID na URL
4. URL exemplo: `https://empresa.pipedrive.com/settings/pipelines/edit/123`
   - O ID é `123`

## 🎯 Identificando os IDs das Etapas (Stages)

### Método 1: Script de Identificação

```python
# Criar arquivo: get_stages.py
from pipedrive_client import PipedriveClient

client = PipedriveClient()

# Substitua pelos IDs dos funis encontrados acima
pipeline_ids = [1, 2, 3]  # IDs dos funis BASE NOVA

for pipeline_id in pipeline_ids:
    print(f"\n=== ETAPAS DO FUNIL {pipeline_id} ===")
    stages = client.get_stages(pipeline_id)
    
    for stage in stages:
        print(f"ID: {stage['id']} - Nome: {stage['name']}")
```

Execute:
```bash
python get_stages.py
```

### Método 2: Interface do Pipedrive

1. Acesse **Configurações** → **Funis e etapas**
2. Clique no funil desejado
3. Cada etapa mostra o ID quando você clica em "Editar"

## ⚙️ Configuração dos IDs

### 1. Funis BASE NOVA

Encontre os IDs dos funis com os nomes:
- `BASE NOVA - SDR`
- `BASE NOVA - NEGOCIAÇÃO`
- `BASE NOVA - FORMALIZAÇÃO/PAGAMENTO`

### 2. Etapas Específicas

Encontre os IDs das etapas:
- `NOVAS COBRANÇAS` (no funil SDR)
- `Enviar minuta e boleto`
- `Aguardando pagamento`
- `Acompanhamento acordo parcelado`
- `Boleto pago`

### 3. Atualizar config.py

Substitua os valores no arquivo `config.py`:

```python
# ========== CONFIGURAÇÕES DOS FUNIS BASE NOVA ==========
PIPELINE_BASE_NOVA_SDR_ID = 45  # ✅ ID real encontrado
PIPELINE_BASE_NOVA_NEGOCIAÇÃO_ID = 46  # ✅ ID real encontrado
PIPELINE_BASE_NOVA_FORMALIZAÇÃO_ID = 47  # ✅ ID real encontrado

# ========== CONFIGURAÇÕES DAS ETAPAS ==========
STAGE_NOVAS_COBRANÇAS_ID = 201  # ✅ ID real encontrado
STAGE_ENVIAR_MINUTA_BOLETO_ID = 202  # ✅ ID real encontrado
STAGE_AGUARDANDO_PAGAMENTO_ID = 203  # ✅ ID real encontrado
STAGE_ACOMPANHAMENTO_ACORDO_ID = 204  # ✅ ID real encontrado
STAGE_BOLETO_PAGO_ID = 205  # ✅ ID real encontrado
```

## 🏷️ Identificando Campos Personalizados

### Método 1: Script de Identificação

```python
# Criar arquivo: get_custom_fields.py
from pipedrive_client import PipedriveClient

client = PipedriveClient()

# Buscar campos personalizados de pessoas
response = client._make_request('GET', 'personFields')

print("=== CAMPOS PERSONALIZADOS DE PESSOAS ===")
for field in response.get('data', []):
    print(f"Key: {field['key']} - Nome: {field['name']} - Tipo: {field['field_type']}")
```

### Método 2: Interface do Pipedrive

1. Acesse **Configurações** → **Campos personalizados**
2. Clique em **Pessoas**
3. Identifique os campos para CPF/CNPJ

### 3. Atualizar pipedrive_client.py

```python
# Método create_person
person_data = {
    'name': name,
    'a1b2c3d4e5f6g7h8': documento,  # ✅ Key real do campo CPF/CNPJ
    'h8g7f6e5d4c3b2a1': tipo_pessoa  # ✅ Key real do campo tipo pessoa
}

# Métodos de verificação
def _check_cpf_in_person(self, person_data: Dict, cpf: str) -> bool:
    custom_fields = person_data.get('custom_fields', [])
    for field in custom_fields:
        if field.get('key') == 'a1b2c3d4e5f6g7h8':  # ✅ Key real
            return field.get('value') == cpf
    return False
```

## 🧪 Testando a Configuração

### 1. Teste de Conexão

```bash
python main.py --config-test
```

### 2. Teste com Dados Pequenos

Crie um arquivo Excel pequeno para testar:

```bash
python main.py --modo excel-pipedrive --excel-file teste_pequeno.xlsx
```

### 3. Verificar Logs

```bash
tail -f ./logs/pipedrive_crud.log
```

## 📋 Checklist de Configuração

- [ ] ✅ Token da API configurado
- [ ] ✅ IDs dos 3 funis BASE NOVA identificados
- [ ] ✅ ID da etapa NOVAS COBRANÇAS identificado
- [ ] ✅ IDs das 4 etapas de exceção identificados
- [ ] ✅ Campos personalizados de CPF/CNPJ identificados
- [ ] ✅ Campos personalizados de tipo pessoa identificados
- [ ] ✅ Arquivo config.py atualizado
- [ ] ✅ Arquivo pipedrive_client.py atualizado
- [ ] ✅ Teste de configuração executado
- [ ] ✅ Teste com dados pequenos executado

## 🚨 Problemas Comuns

### "Pipeline não encontrado"
- Verifique se o ID do funil está correto
- Confirme se o funil não foi deletado
- Verifique permissões do token

### "Stage não encontrado"
- Confirme se a etapa pertence ao funil correto
- Verifique se a etapa não foi deletada
- Confirme se o ID da etapa está correto

### "Campo personalizado não encontrado"
- Verifique se o campo existe em "Pessoas"
- Confirme se a key do campo está correta
- Verifique se o campo não foi deletado

## 🔄 Scripts Auxiliares Completos

### get_all_info.py
```python
from pipedrive_client import PipedriveClient

client = PipedriveClient()

print("=== INFORMAÇÕES COMPLETAS DO PIPEDRIVE ===\n")

# Listar Funis
print("🔥 FUNIS:")
pipelines = client.get_pipelines()
for pipeline in pipelines:
    print(f"  ID: {pipeline['id']} - {pipeline['name']}")

# Listar Etapas por Funil
print("\n🎯 ETAPAS POR FUNIL:")
for pipeline in pipelines:
    stages = client.get_stages(pipeline['id'])
    if stages:
        print(f"\n  Funil: {pipeline['name']} (ID: {pipeline['id']})")
        for stage in stages:
            print(f"    ID: {stage['id']} - {stage['name']}")

# Listar Campos Personalizados
print("\n🏷️ CAMPOS PERSONALIZADOS DE PESSOAS:")
try:
    response = client._make_request('GET', 'personFields')
    for field in response.get('data', []):
        print(f"  Key: {field['key']} - Nome: {field['name']}")
except Exception as e:
    print(f"  Erro ao buscar campos: {e}")
```

Execute este script para obter todas as informações necessárias de uma vez:

```bash
python get_all_info.py
``` 