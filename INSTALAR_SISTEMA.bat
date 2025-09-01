@echo off
chcp 65001 >nul
title Sistema Pipedrive - Inadimplentes - Instalador

echo ========================================
echo  Sistema Pipedrive - Inadimplentes
echo  Instalador Automático v2.0
echo ========================================
echo.

echo 🔍 Verificando Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python não encontrado!
    echo Por favor, instale o Python 3.8+ primeiro.
    echo Download: https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)
echo ✅ Python encontrado

echo.
echo 📥 Baixando código fonte do GitHub...
echo    Repositório: SEU-USUARIO/pipedrive-inadimplentes

REM Baixar arquivo ZIP
curl -L -o pipedrive-inadimplentes.zip "https://github.com/antoniocherubim/PIPEDRIVE-CRUD-DEBT/archive/refs/heads/main.zip"
if errorlevel 1 (
    echo ❌ Erro ao baixar do GitHub
    echo 💡 Verifique sua conexão com a internet
    pause
    exit /b 1
)
echo ✅ Download concluído

echo.
echo 📁 Extraindo arquivos...
powershell -command "Expand-Archive -Path 'pipedrive-inadimplentes.zip' -DestinationPath '.' -Force"
if errorlevel 1 (
    echo ❌ Erro ao extrair arquivos
    pause
    exit /b 1
)
echo ✅ Arquivos extraídos

REM Encontrar pasta extraída
for /d %%i in (PIPEDRIVE-CRUD-DEBT-main) do set EXTRACTED_FOLDER=%%i

REM Verificar se a pasta foi encontrada
if not defined EXTRACTED_FOLDER (
    echo ❌ Pasta extraída não encontrada
    echo Procurando por pastas extraídas...
    dir /b /ad
    pause
    exit /b 1
)

echo.
echo 📋 Organizando arquivos...
echo Pasta extraída: %EXTRACTED_FOLDER%

REM Copiar arquivos específicos da pasta extraída
echo Copiando arquivos principais...

REM Copiar arquivos Python principais
if exist "%EXTRACTED_FOLDER%\main.py" copy "%EXTRACTED_FOLDER%\main.py" "." >nul 2>&1
if exist "%EXTRACTED_FOLDER%\gui_main.py" copy "%EXTRACTED_FOLDER%\gui_main.py" "." >nul 2>&1
if exist "%EXTRACTED_FOLDER%\requirements.txt" copy "%EXTRACTED_FOLDER%\requirements.txt" "." >nul 2>&1

REM Copiar pasta src se não existir
if not exist "src" (
    if exist "%EXTRACTED_FOLDER%\src" (
        xcopy /E /I /Y "%EXTRACTED_FOLDER%\src" "src" >nul 2>&1
    )
)

REM Copiar pasta utils se não existir
if not exist "utils" (
    if exist "%EXTRACTED_FOLDER%\utils" (
        xcopy /E /I /Y "%EXTRACTED_FOLDER%\utils" "utils" >nul 2>&1
    )
)

REM Copiar arquivos de documentação
if exist "%EXTRACTED_FOLDER%\README.md" copy "%EXTRACTED_FOLDER%\README.md" "." >nul 2>&1
if exist "%EXTRACTED_FOLDER%\README_GUI.md" copy "%EXTRACTED_FOLDER%\README_GUI.md" "." >nul 2>&1
if exist "%EXTRACTED_FOLDER%\ESTRUTURA_PROJETO.md" copy "%EXTRACTED_FOLDER%\ESTRUTURA_PROJETO.md" "." >nul 2>&1
if exist "%EXTRACTED_FOLDER%\CONFIGURAÇÃO_IDS.md" copy "%EXTRACTED_FOLDER%\CONFIGURAÇÃO_IDS.md" "." >nul 2>&1

echo ✅ Arquivos organizados

REM Limpar arquivos temporários
rmdir /S /Q "%EXTRACTED_FOLDER%"
del pipedrive-inadimplentes.zip
echo ✅ Arquivos temporários removidos

echo.
echo 📦 Instalando dependências...
python -m pip install --upgrade pip >nul 2>&1
python -m pip install -r requirements.txt
if errorlevel 1 (
    echo ❌ Erro na instalação das dependências
    pause
    exit /b 1
)
echo ✅ Dependências instaladas

echo.
echo 📁 Criando estrutura de diretórios...
if not exist "input" mkdir input
if not exist "input\escritorio_cobranca" mkdir input\escritorio_cobranca
if not exist "input\garantinorte" mkdir input\garantinorte
if not exist "logs" mkdir logs
if not exist "backup" mkdir backup
if not exist "output" mkdir output
if not exist "relatorios" mkdir relatorios
echo ✅ Estrutura criada

echo.
echo ⚙️ Criando arquivo de configuração...
if not exist ".env" (
    echo # Configurações do Sistema Pipedrive - Inadimplentes > .env
    echo # Token da API (OBRIGATÓRIO - Configure na GUI >> .env
    echo PIPEDRIVE_API_TOKEN=SEU_TOKEN_AQUI >> .env
    echo. >> .env
    echo # Domínio do Pipedrive >> .env
    echo PIPEDRIVE_DOMAIN=sua-empresa >> .env
    echo. >> .env
    echo # Configurações do sistema >> .env
    echo LOG_LEVEL=INFO >> .env
    echo BACKUP_ENABLED=true >> .env
    echo PROCESSING_MODE=full >> .env
    echo ✅ Arquivo .env criado
) else (
    echo ⚠️ Arquivo .env já existe
)

echo.
echo 🚀 Criando executáveis...
echo @echo off > iniciar_sistema.bat
echo chcp 65001 ^>nul >> iniciar_sistema.bat
echo title Sistema Pipedrive - Inadimplentes >> iniciar_sistema.bat
echo. >> iniciar_sistema.bat
echo echo ======================================== >> iniciar_sistema.bat
echo echo  Sistema Pipedrive - Inadimplentes >> iniciar_sistema.bat
echo echo  Iniciando Interface Gráfica... >> iniciar_sistema.bat
echo echo ======================================== >> iniciar_sistema.bat
echo echo. >> iniciar_sistema.bat
echo. >> iniciar_sistema.bat
echo python gui_main.py >> iniciar_sistema.bat
echo pause >> iniciar_sistema.bat
echo ✅ iniciar_sistema.bat criado

echo @echo off > configurar_token.bat
echo chcp 65001 ^>nul >> configurar_token.bat
echo title Configurar Token - Sistema Pipedrive - Inadimplentes >> configurar_token.bat
echo. >> configurar_token.bat
echo echo ======================================== >> configurar_token.bat
echo echo  Sistema Pipedrive - Inadimplentes >> configurar_token.bat
echo echo  Configurador de Token da API >> configurar_token.bat
echo echo ======================================== >> configurar_token.bat
echo echo. >> configurar_token.bat
echo echo Para configurar o token da API Pipedrive: >> configurar_token.bat
echo echo. >> configurar_token.bat
echo echo 1. Acesse sua conta Pipedrive >> configurar_token.bat
echo echo 2. Vá em Settings ^> Personal Preferences ^> API >> configurar_token.bat
echo echo 3. Copie o token da API >> configurar_token.bat
echo echo 4. Execute o sistema e configure na GUI >> configurar_token.bat
echo echo. >> configurar_token.bat
echo echo Ou edite manualmente o arquivo .env >> configurar_token.bat
echo echo. >> configurar_token.bat
echo echo PIPEDRIVE_API_TOKEN=seu_token_aqui >> configurar_token.bat
echo echo PIPEDRIVE_DOMAIN=sua-empresa >> configurar_token.bat
echo echo. >> configurar_token.bat
echo echo Deseja abrir o arquivo .env para edição? (s/n) >> configurar_token.bat
echo set /p choice="Digite s para sim, n para não: " >> configurar_token.bat
echo. >> configurar_token.bat
echo if /i "%%choice%%"=="s" ( >> configurar_token.bat
echo     if exist ".env" ( >> configurar_token.bat
echo         notepad .env >> configurar_token.bat
echo     ) else ( >> configurar_token.bat
echo         echo Arquivo .env não encontrado. Execute o sistema primeiro. >> configurar_token.bat
echo     ) >> configurar_token.bat
echo ) >> configurar_token.bat
echo echo. >> configurar_token.bat
echo echo Configuração concluída! >> configurar_token.bat
echo pause >> configurar_token.bat
echo ✅ configurar_token.bat criado

echo.
echo 🧪 Testando instalação...
python -c "import customtkinter; print('✅ CustomTkinter OK')" >nul 2>&1
if errorlevel 1 (
    echo ❌ CustomTkinter não encontrado
    echo Tentando instalação manual...
    python -m pip install customtkinter
)

if exist "gui_main.py" (
    echo ✅ gui_main.py encontrado
) else (
    echo ❌ gui_main.py não encontrado
    pause
    exit /b 1
)

echo.
echo ========================================
echo 🎉 INSTALAÇÃO CONCLUÍDA COM SUCESSO!
echo ========================================
echo.
echo 📋 Próximos passos:
echo 1. Execute: configurar_token.bat
echo 2. Configure seu token da API Pipedrive
echo 3. Execute: iniciar_sistema.bat
echo.
echo 📄 Leia: COMO_USAR.txt para instruções detalhadas
echo.
echo 🚀 Deseja iniciar o sistema agora? (s/n)
set /p start_choice="Digite s para sim, n para não: "

if /i "%start_choice%"=="s" (
    echo.
    echo 🚀 Iniciando sistema...
    python gui_main.py
)

echo.
echo Sistema finalizado.
pause
