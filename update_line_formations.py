#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
line_formationが空のレコードを修正するスクリプト
"""

import mysql.connector
from mysql.connector import Error


def update_line_formations():
    """line_formationが空のレコードを修正"""

    try:
        # データベース接続設定
        connection = mysql.connector.connect(
            host="localhost",
            database="keirin_db",
            user="keirin_user",
            password="keirin_password",
        )

        if connection.is_connected():
            cursor = connection.cursor(dictionary=True)
            print("データベースに接続しました")

            # line_formationが空のレコード数を確認
            cursor.execute(
                "SELECT COUNT(*) as count FROM line_predictions WHERE line_formation = '' OR line_formation IS NULL"
            )
            result = cursor.fetchone()
            empty_count = result["count"]
            print(f"line_formationが空のレコード数: {empty_count}件")

            if empty_count == 0:
                print("修正対象のレコードがありません")
                return

            # 少数のレコードでテスト
            cursor.execute(
                """
                SELECT race_id, line_type, line_formation 
                FROM line_predictions 
                WHERE (line_formation = '' OR line_formation IS NULL) 
                AND line_type != ''
                LIMIT 5
            """
            )

            test_records = cursor.fetchall()
            print(f"\nテスト対象レコード: {len(test_records)}件")

            for record in test_records:
                race_id = record["race_id"]
                line_type = record["line_type"]
                current_formation = record["line_formation"]

                print(
                    f"Race ID: {race_id}, Line Type: {line_type}, Current Formation: '{current_formation}'"
                )

                # ここでAPIからデータを取得してline_formationを更新する処理を実装
                # 今回は例として固定値を設定
                if line_type == "二分戦":
                    new_formation = "1・2・3―4・5・6・7"  # 例
                elif line_type == "三分戦":
                    new_formation = "1・2―3・4・5―6・7"  # 例
                else:
                    new_formation = "1―2―3―4―5―6―7"  # 例

                # UPDATE実行（テストのため実際には実行しない）
                print(
                    f"  → 新しいformation: '{new_formation}' (テストモードのため更新しません)"
                )

                # 実際に更新する場合は以下のコメントを外す
                # update_query = "UPDATE line_predictions SET line_formation = %s WHERE race_id = %s"
                # cursor.execute(update_query, (new_formation, race_id))

            # connection.commit()  # 実際に更新する場合
            print("\nテスト完了")

    except Error as e:
        print(f"データベースエラー: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("データベース接続を閉じました")


if __name__ == "__main__":
    update_line_formations()
