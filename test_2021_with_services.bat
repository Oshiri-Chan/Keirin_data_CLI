@echo off
chcp 65001
echo.
echo ================================
echo 2021年1月1日データ取得テスト
echo (既存サービス使用)
echo ================================
echo.

echo 📅 対象日付: 2021-01-01
echo 🔧 使用方法: 既存のmain.py CLIモード + UpdateService
echo 📋 実行ステップ: Step1-5 全て
echo 🔄 強制更新: 有効
echo.

echo ⏳ データ取得を開始します...
echo.

python main.py ^
  --mode period ^
  --start-date 2021-01-01 ^
  --end-date 2021-01-01 ^
  --step1 1 ^
  --step2 1 ^
  --step3 1 ^
  --step4 1 ^
  --step5 1 ^
  --force-update 1 ^
  --debug 1

echo.
if %ERRORLEVEL% equ 0 (
    echo ✅ テスト完了: すべてのステップが正常に実行されました
) else (
    echo ❌ テスト失敗: エラーレベル %ERRORLEVEL%
    echo 詳細はログファイルを確認してください
)

echo.
echo 📊 実行されたサービス:
echo   - Step1: WinticketAPI + Step1Updater + Step1Saver
echo   - Step2: WinticketAPI + Step2Updater + Step2Saver  
echo   - Step3: WinticketAPI + Step3Updater + Step3Saver
echo   - Step4: WinticketAPI + Step4Updater + Step4Saver
echo   - Step5: YenjoyAPI + Step5Updater + Step5Saver
echo.

pause 