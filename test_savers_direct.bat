@echo off
chcp 65001 > nul
echo 🔍 services/savers直接テスト実行（KeirinDataAccessor問題回避）

python test_savers_direct.py
set ERROR_LEVEL=%ERRORLEVEL%

echo.
echo ===============================================
if %ERROR_LEVEL% EQU 0 (
    echo ✅ services/savers直接テスト成功！
) else (
    echo ❌ services/savers直接テスト失敗 ^(エラーレベル: %ERROR_LEVEL%^)
)
echo ===============================================

pause 