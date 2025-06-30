@echo off
rem 競輪データ更新GUIの起動スクリプト
setlocal enabledelayedexpansion

rem 変数定義
set SCRIPT_DIR=%~dp0
set ROOT_DIR=%SCRIPT_DIR%\..
cd %ROOT_DIR%

rem Pythonパスチェック
where python >nul 2>&1
if %errorLevel% neq 0 (
    echo Pythonが見つかりません。インストールしてください。
    pause
    exit /b 1
)

rem 必要なディレクトリ作成
if not exist "logs" mkdir logs

echo 競輪データ更新ツールを起動しています...
python main.py

exit /b 0 