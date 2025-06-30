#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
MySQL接続確認スクリプト
"""

import sys
import logging
from database.db_accessor import KeirinDataAccessor

# ロガー設定
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("mysql_test")


def main():
    print("🔍 データベース接続テストを実行中...")

    try:
        # KeirinDataAccessor初期化
        db_accessor = KeirinDataAccessor("config/config.ini", logger)

        # 基本接続テスト
        result = db_accessor.execute_query(
            "SELECT VERSION() as version", fetch_one=True
        )
        if result:
            print(f'✅ MySQL接続成功: {result["version"]}')
        else:
            print("❌ バージョン取得失敗")
            return 1

        # テーブル存在確認
        tables_result = db_accessor.execute_query(
            "SELECT COUNT(*) as table_count FROM information_schema.tables WHERE table_schema = DATABASE()",
            fetch_one=True,
        )

        if tables_result:
            table_count = tables_result["table_count"]
            print(f"📊 データベース内テーブル数: {table_count}")

            if table_count >= 20:
                print("✅ 充分な数のテーブルが存在します")
            else:
                print("⚠️  テーブル数が少ない可能性があります")

        # 主要テーブル確認
        key_tables = ["regions", "venues", "cups", "races", "players"]
        print("\n🔍 主要テーブル確認:")

        for table in key_tables:
            check_result = db_accessor.execute_query(
                "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s",
                (table,),
                fetch_one=True,
            )

            if check_result and check_result["count"] > 0:
                print(f"  ✅ {table}")
            else:
                print(f"  ❌ {table} (見つかりません)")

        print("\n🎉 データベース準備完了!")
        return 0

    except Exception as e:
        print(f"❌ データベース接続エラー: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
