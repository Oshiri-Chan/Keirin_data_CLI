@echo off
echo Git設定を調整しています...

REM HTTPバッファサイズを増やす
git config --global http.postBuffer 524288000

REM タイムアウトを延長
git config --global http.lowSpeedLimit 0
git config --global http.lowSpeedTime 999999

echo 設定が完了しました。
echo.
echo 次のコマンドを実行してください:
echo git push --set-upstream origin main
