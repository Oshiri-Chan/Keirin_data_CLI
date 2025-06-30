@echo off
chcp 65001
echo.
echo ==========================================
echo 2021年1月1日データ取得テスト (ステップ毎実行)
echo ==========================================
echo.

set TARGET_DATE=2021-01-01

echo 📅 対象日付: %TARGET_DATE%
echo 🔧 実行方法: 各ステップを個別実行して結果を確認
echo.

echo ===============================
echo 🔹 Step1: 月間開催情報取得
echo ===============================
echo ⏳ Step1を実行中...
python main.py --mode period --start-date %TARGET_DATE% --end-date %TARGET_DATE% --step1 1 --step2 0 --step3 0 --step4 0 --step5 0 --force-update 1 --debug 1

if %ERRORLEVEL% equ 0 (
    echo ✅ Step1: 成功
) else (
    echo ❌ Step1: 失敗 (エラーレベル: %ERRORLEVEL%)
    echo Step1で問題が発生しました。ログを確認してください。
    pause
    exit /b 1
)
echo.

echo ===============================
echo 🔹 Step2: 開催詳細情報取得
echo ===============================
echo ⏳ Step2を実行中...
python main.py --mode period --start-date %TARGET_DATE% --end-date %TARGET_DATE% --step1 0 --step2 1 --step3 0 --step4 0 --step5 0 --force-update 1 --debug 1

if %ERRORLEVEL% equ 0 (
    echo ✅ Step2: 成功
) else (
    echo ❌ Step2: 失敗 (エラーレベル: %ERRORLEVEL%)
    echo Step2で問題が発生しました。ログを確認してください。
    pause
    exit /b 2
)
echo.

echo ===============================
echo 🔹 Step3: レース詳細情報取得
echo ===============================
echo ⏳ Step3を実行中...
python main.py --mode period --start-date %TARGET_DATE% --end-date %TARGET_DATE% --step1 0 --step2 0 --step3 1 --step4 0 --step5 0 --force-update 1 --debug 1

if %ERRORLEVEL% equ 0 (
    echo ✅ Step3: 成功
) else (
    echo ❌ Step3: 失敗 (エラーレベル: %ERRORLEVEL%)
    echo Step3で問題が発生しました。ログを確認してください。
    pause
    exit /b 3
)
echo.

echo ===============================
echo 🔹 Step4: オッズ情報取得
echo ===============================
echo ⏳ Step4を実行中...
python main.py --mode period --start-date %TARGET_DATE% --end-date %TARGET_DATE% --step1 0 --step2 0 --step3 0 --step4 1 --step5 0 --force-update 1 --debug 1

if %ERRORLEVEL% equ 0 (
    echo ✅ Step4: 成功
) else (
    echo ❌ Step4: 失敗 (エラーレベル: %ERRORLEVEL%)
    echo Step4で問題が発生しました。ログを確認してください。
    pause
    exit /b 4
)
echo.

echo ===============================
echo 🔹 Step5: レース結果情報取得
echo ===============================
echo ⏳ Step5を実行中...
python main.py --mode period --start-date %TARGET_DATE% --end-date %TARGET_DATE% --step1 0 --step2 0 --step3 0 --step4 0 --step5 1 --force-update 1 --debug 1

if %ERRORLEVEL% equ 0 (
    echo ✅ Step5: 成功
) else (
    echo ❌ Step5: 失敗 (エラーレベル: %ERRORLEVEL%)
    echo Step5で問題が発生しました。ログを確認してください。
    pause
    exit /b 5
)
echo.

echo ==========================================
echo 🎉 全ステップが正常に完了しました！
echo ==========================================
echo.
echo 📊 実行結果サマリー:
echo   ✅ Step1: 月間開催情報取得 - 成功
echo   ✅ Step2: 開催詳細情報取得 - 成功  
echo   ✅ Step3: レース詳細情報取得 - 成功
echo   ✅ Step4: オッズ情報取得 - 成功
echo   ✅ Step5: レース結果情報取得 - 成功
echo.
echo 🔧 使用されたサービス構成:
echo   - UpdateService (services/update_service.py)
echo   - Step1Updater + Step1Saver (MySQL対応)
echo   - Step2Updater + Step2Saver (MySQL対応)
echo   - Step3Updater + Step3Saver (MySQL対応)  
echo   - Step4Updater + Step4Saver (MySQL対応)
echo   - Step5Updater + Step5Saver (MySQL対応)
echo   - KeirinDataAccessor (database/db_accessor.py)
echo.

pause 