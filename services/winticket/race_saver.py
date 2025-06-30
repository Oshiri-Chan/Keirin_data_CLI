"""
Winticketレース情報保存サービス
"""

import pandas as pd

from .base_saver import WinticketBaseSaver


class WinticketRaceSaver(WinticketBaseSaver):
    """
    Winticketのレース基本情報を保存するサービス
    """

    def save_race_info(self, race_info, date_str):
        """
        レース基本情報を保存

        Args:
            race_info (dict): レース基本情報
            date_str (str): 日付文字列（YYYYMMDD）

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            if not race_info:
                self.logger.warning("有効なレース情報がありません")
                return False

            self.logger.debug(
                f"レース {race_info.get('race_id', '不明')} の情報: {race_info}"
            )

            # venue情報の取得を改善
            venue = "不明"
            if "venue_name" in race_info and race_info["venue_name"]:
                venue = race_info["venue_name"]
            elif "venue" in race_info and race_info["venue"]:
                venue = race_info["venue"]
            elif "venue_id" in race_info and race_info["venue_id"]:
                venue = self.map_venue_id_to_name(race_info["venue_id"])

            # レース情報をDataFrameに変換
            race_df = pd.DataFrame(
                [
                    {
                        "race_id": race_info.get("race_id", ""),
                        "date": date_str,
                        "venue": venue,
                        "race_number": race_info.get("race_number", 0),
                        "title": race_info.get("race_name", ""),
                        "distance": race_info.get("distance", 0),
                        "race_class": race_info.get("race_type", ""),
                        "weather": "",  # 天候情報は別途取得が必要
                        "temperature": 0.0,  # 気温情報は別途取得が必要
                        "is_finished": race_info.get("status")
                        == 3,  # ステータス3はレース終了
                        "entry_count": race_info.get("entry_count", 0),
                        "created_at": self.get_current_timestamp(),
                        "updated_at": self.get_current_timestamp(),
                    }
                ]
            )

            self.logger.info(
                f"レース {race_info.get('race_id', '不明')} の基本情報を保存します"
            )

            # データベースに保存
            success = self.save_to_database(race_df, "races", ["race_id"])

            if not success:
                self.logger.error(
                    f"レース {race_info.get('race_id', '不明')} の基本情報の保存に失敗しました"
                )

            return success

        except Exception as e:
            self.logger.error(f"レース情報の保存中にエラー: {str(e)}", exc_info=True)
            return False

    def update_race_status(self, race_id, is_finished):
        """
        レースのステータスを更新

        Args:
            race_id (str): レースID
            is_finished (bool): レース終了フラグ

        Returns:
            bool: 更新成功の場合はTrue
        """
        try:
            # レースのステータスを更新するSQLクエリ
            query = f"""
            UPDATE races 
            SET is_finished = {1 if is_finished else 0}, updated_at = '{self.get_current_timestamp()}'
            WHERE race_id = '{race_id}'
            """

            result = self.db.execute_query(query, is_insert=True)

            if result:
                status_str = "終了" if is_finished else "未完了"
                self.logger.info(
                    f"レース {race_id} のステータスを「{status_str}」に更新しました"
                )
                return True
            else:
                self.logger.error(f"レース {race_id} のステータス更新に失敗しました")
                return False

        except Exception as e:
            self.logger.error(
                f"レースステータスの更新中にエラー: {str(e)}", exc_info=True
            )
            return False
