#!/bin/bash

echo "競輪データ更新ツール - クイック実行"
echo
echo "以下のオプションから選択してください："
echo
echo "1. 通常更新（前後2日分、全ステップ）"
echo "2. 通常更新（前後2日分、ステップ1-3のみ）"
echo "3. 期間指定更新（日付を指定）"
echo "4. セットアップ（全データ）"
echo "5. ドライラン（確認のみ）"
echo "6. GUIモード"
echo "7. 終了"
echo

read -p "選択してください (1-7): " choice

case $choice in
    1)
        echo "通常更新（前後2日分、全ステップ）を実行します..."
        python3 main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1
        ;;
    2)
        echo "通常更新（前後2日分、ステップ1-3のみ）を実行します..."
        python3 main.py --mode check_update --step1 1 --step2 1 --step3 1
        ;;
    3)
        echo "期間指定更新モードです。"
        read -p "開始日を入力してください (YYYY-MM-DD): " start_date
        read -p "終了日を入力してください (YYYY-MM-DD): " end_date
        echo "期間: $start_date から $end_date で更新を実行します..."
        python3 main.py --mode period --start-date $start_date --end-date $end_date --step1 1 --step2 1 --step3 1 --step4 1 --step5 1
        ;;
    4)
        echo "セットアップ（全データ）を実行します..."
        echo "注意: この処理は非常に長時間かかる可能性があります。"
        read -p "続行しますか？ (y/n): " confirm
        if [[ $confirm == [yY] ]]; then
            python3 main.py --mode setup --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --force-update 1
        else
            echo "キャンセルしました。"
        fi
        ;;
    5)
        echo "ドライラン（確認のみ）を実行します..."
        python3 main.py --mode check_update --step1 1 --step2 1 --step3 1 --step4 1 --step5 1 --dry-run 1
        ;;
    6)
        echo "GUIモードで起動します..."
        python3 main.py
        ;;
    7)
        echo "終了します。"
        exit 0
        ;;
    *)
        echo "無効な選択です。"
        ;;
esac

echo
echo "処理が完了しました。" 