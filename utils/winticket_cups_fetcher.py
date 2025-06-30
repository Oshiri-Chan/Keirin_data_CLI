#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Winticketの月間開催情報取得ユーティリティ
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

import requests

# 自作モジュールを使えるようにパスを追加
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

from database.keirin_database import KeirinDatabase
from services.winticket_data_saver import WinticketDataSaver
from utils.logger import setup_application_logger


def fetch_cups_data(date_str, logger=None):
    """
    Winticketの月間開催情報を取得

    Args:
        date_str (str): 対象月の初日を表す日付文字列（YYYYMMDD形式）
        logger (logging.Logger, optional): ロガーオブジェクト

    Returns:
        dict: APIレスポンスデータ
    """
    logger = logger or logging.getLogger(__name__)

    # APIエンドポイントとパラメータ
    url = "https://api.winticket.jp/v1/keirin/cups"
    params = {"date": date_str, "fields": "month,venues,regions", "pfm": "web"}

    logger.info(f"Winticket開催情報の取得を開始 日付: {date_str}")

    try:
        # API呼び出し
        response = requests.get(url, params=params, timeout=30)

        # ステータスコードのチェック
        if response.status_code == 200:
            data = response.json()

            # データの妥当性チェック
            if "month" in data:
                cups_count = len(data.get("month", {}).get("cups", []))
                venues_count = len(data.get("month", {}).get("venues", []))
                regions_count = len(data.get("month", {}).get("regions", []))

                logger.info(
                    f"データ取得成功: {cups_count}件の開催、{venues_count}件の会場、"
                    f"{regions_count}件の地域情報"
                )

                return data
            else:
                logger.error("レスポンスにmonthフィールドがありません")
                return None
        else:
            logger.error(f"API呼び出しエラー: ステータスコード {response.status_code}")
            logger.error(f"レスポンス: {response.text}")
            return None

    except Exception as e:
        logger.error(f"開催情報取得中にエラーが発生しました: {str(e)}", exc_info=True)
        return None


def save_cups_data(data, logger=None):
    """
    取得した開催情報をデータベースに保存

    Args:
        data (dict): 開催情報データ
        logger (logging.Logger, optional): ロガーオブジェクト

    Returns:
        bool: 保存成功の場合はTrue
    """
    logger = logger or logging.getLogger(__name__)

    try:
        # データベース接続
        db = KeirinDatabase()

        # データセーバーの初期化
        saver = WinticketDataSaver(db, logger)

        # データの保存
        success = saver.save_cups_data(data)

        if success:
            logger.info("開催情報の保存が完了しました")
        else:
            logger.error("開催情報の保存に失敗しました")

        # データベース接続のクローズ
        db.close()

        return success

    except Exception as e:
        logger.error(f"データ保存中にエラーが発生しました: {str(e)}", exc_info=True)
        return False


def parse_args():
    """コマンドライン引数をパース"""
    parser = argparse.ArgumentParser(
        description="Winticketの月間開催情報を取得して保存"
    )

    parser.add_argument(
        "--date",
        "-d",
        help="取得する月の初日（YYYYMMDD形式）。指定がない場合は現在の月の初日",
    )

    parser.add_argument(
        "--save-json",
        "-j",
        action="store_true",
        help="取得したデータをJSONファイルとして保存",
    )

    parser.add_argument(
        "--output-dir",
        "-o",
        default="data",
        help="JSONファイルの保存先ディレクトリ。デフォルトは'data'",
    )

    return parser.parse_args()


def main():
    """メイン関数"""
    # 引数のパース
    args = parse_args()

    # ロガーの設定
    logger = setup_application_logger()

    # 日付の設定
    if args.date:
        date_str = args.date
    else:
        # 現在の日付を取得し、月初に設定
        now = datetime.now()
        first_day = datetime(now.year, now.month, 1)
        date_str = first_day.strftime("%Y%m%d")

    # 開催情報の取得
    data = fetch_cups_data(date_str, logger)

    if not data:
        logger.error("開催情報の取得に失敗しました")
        return 1

    # JSONファイルとして保存
    if args.save_json:
        output_dir = args.output_dir

        # 出力ディレクトリの作成
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        # ファイル名の設定
        filename = f"cups_{date_str}.json"
        output_path = os.path.join(output_dir, filename)

        # JSONファイルに保存
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        logger.info(f"データをJSONファイルに保存しました: {output_path}")

    # データベースに保存
    success = save_cups_data(data, logger)

    # 終了コード
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
