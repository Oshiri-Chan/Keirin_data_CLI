@echo off
chcp 65001
echo.
echo ================================
echo Step1のみテスト (2021-01-01)
echo ================================
echo.

echo 🔧 修正されたKeirinDataAccessorでStep1テスト
echo 📅 対象日付: 2021-01-01
echo 📋 実行ステップ: Step1のみ
echo.

echo ⏳ Step1を実行中...
python main.py --mode period --start-date 2021-01-01 --end-date 2021-01-01 --step1 1 --step2 0 --step3 0 --step4 0 --step5 0 --force-update 1

echo.
if %ERRORLEVEL% equ 0 (
    echo ✅ Step1テスト成功: 地域情報保存完了
) else (
    echo ❌ Step1テスト失敗: エラーレベル %ERRORLEVEL%
)

echo.
pause 