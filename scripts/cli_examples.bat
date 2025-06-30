@echo off
chcp 65001 > nul
echo 競輪データ更新ツール - コマンドライン使用例

echo.
echo ========================================
echo 基本的な使用例
echo ========================================

echo.
echo 1. 通常の更新（更新日から前後2日分をチェック・更新）
echo   python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1
echo.

echo 2. 期間指定更新（2024年1月のデータを更新）
echo   python main.py --mode period --start-date 2024-01-01 --end-date 2024-01-31 --step1 1 --step2 1 --step3 1 --step4 1 --step5 1
echo.

echo 3. セットアップ（2018年から現在までの全データ保存）
echo   python main.py --mode setup --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --force-update 1
echo.

echo 4. 特定のステップのみ実行（ステップ1とステップ3のみ）
echo   python main.py --mode check_update --step1 1 --step3 1
echo.

echo 5. 特定の会場のみ更新
echo   python main.py --mode period --start-date 2024-01-01 --end-date 2024-01-31 --step1 1 --step2 1 --venue-codes 01 02 03
echo.

echo ========================================
echo 高度な使用例
echo ========================================

echo.
echo 6. ドライラン（処理内容のみ表示、実際の更新は行わない）
echo   python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --dry-run 1
echo.

echo 7. デバッグモード（詳細ログ出力）
echo   python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --debug 1
echo.

echo 8. 並列処理数を指定
echo   python main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --max-workers 10
echo.

echo 9. 強制更新モード
echo   python main.py --mode period --start-date 2024-01-01 --end-date 2024-01-31 --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --force-update 1
echo.

echo ========================================
echo 引数の説明
echo ========================================
echo.
echo --mode: 更新モード
echo   check_update : 更新日から前後2日分をチェック・更新
echo   period       : 期間指定更新
echo   setup        : 2018年から現在までの全データ保存
echo.
echo --start-date / --end-date: 更新期間（YYYY-MM-DD形式）
echo   期間指定モード（--mode period）でのみ使用
echo.
echo --step1 〜 --step5: 各ステップの実行指定
echo   0 = 実行しない, 1 = 実行する
echo.
echo --force-update: 強制更新モード
echo   0 = 通常更新, 1 = 強制更新
echo.
echo --venue-codes: 処理対象の会場コード（複数指定可能）
echo   例: --venue-codes 01 02 03
echo.
echo --debug: デバッグモード
echo   0 = 通常ログ, 1 = 詳細ログ
echo.
echo --max-workers: 最大並列処理数
echo   デフォルト: 設定ファイルの値
echo.
echo --dry-run: ドライランモード
echo   0 = 実際に更新, 1 = 更新せずに処理内容のみ表示
echo.

echo ========================================
echo GUIモード
echo ========================================
echo.
echo 引数なしで実行するとGUIモードで起動します：
echo   python main.py
echo.

pause 