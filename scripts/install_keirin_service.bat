@echo off
rem 競輪データ更新サービスインストールスクリプト
setlocal enabledelayedexpansion

rem 管理者権限チェック
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo 管理者権限が必要です。右クリックで「管理者として実行」してください。
    pause
    exit /b 1
)

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

rem pywin32チェック
python -c "import win32service" >nul 2>&1
if %errorLevel% neq 0 (
    echo pywin32モジュールが見つかりません。インストールします...
    pip install pywin32
    if %errorLevel% neq 0 (
        echo pywin32のインストールに失敗しました。
        pause
        exit /b 1
    )
)

rem 必要なディレクトリ作成
if not exist "logs" mkdir logs

rem 設定ファイルが存在するか確認
if not exist "config.ini" (
    echo 設定ファイルが見つかりません。デフォルト設定を使用します。
    copy "%SCRIPT_DIR%\config.ini.template" "config.ini" >nul 2>&1
    if %errorLevel% neq 0 (
        echo 設定ファイルのコピーに失敗しました。
        pause
        exit /b 1
    )
)

echo 競輪データ更新サービスをインストールします...

rem サービスインストール
python -c "from scripts.keirin_data_daemon import create_windows_service; create_windows_service()" > logs\service_install.log 2>&1
if %errorLevel% neq 0 (
    echo サービスのインストールに失敗しました。logs\service_install.logを確認してください。
    type logs\service_install.log
    pause
    exit /b 1
)

echo サービスが正常にインストールされました。
echo サービスは「サービス」アプリケーションから「競輪データ更新サービス」の名前で確認できます。
echo サービスを開始するには「サービス」アプリケーションから「競輪データ更新サービス」を右クリックして「開始」を選択してください。

pause
exit /b 0 