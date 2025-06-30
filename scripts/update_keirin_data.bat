@echo off
rem 競輪データ更新スクリプト
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

rem 引数の解析
set DATE=%1
set OPTIONS=
if "%DATE%"=="" (
    rem 日付が指定されていない場合は昨日の日付を使用
    for /f "tokens=2-4 delims=/ " %%a in ('date /t') do (
        set DAY=%%a
        set MONTH=%%b
        set YEAR=%%c
    )
    
    rem 月と日が1桁の場合は0埋め
    if %MONTH% LSS 10 set MONTH=0%MONTH%
    if %DAY% LSS 10 set DAY=0%DAY%
    
    set DATE=%YEAR%%MONTH%%DAY%
)

rem オプション設定
set UPDATE_WINTICKET=--winticket
set UPDATE_YENJOY=--yenjoy

rem 更新実行
echo %DATE%の競輪データを更新します...
python -c "from scripts.update_winticket_data import update_winticket_data; from scripts.update_yenjoy_data import update_yenjoy_data; update_winticket_data('%DATE%'); update_yenjoy_data('%DATE%')"

if %errorLevel% neq 0 (
    echo データ更新中にエラーが発生しました。
    pause
    exit /b 1
)

echo データ更新が完了しました。

if not "%2"=="nopause" pause
exit /b 0 