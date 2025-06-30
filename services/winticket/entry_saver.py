"""
Winticket出走表情報保存サービス
"""

import pandas as pd

from .base_saver import WinticketBaseSaver


class WinticketEntrySaver(WinticketBaseSaver):
    """
    Winticketの出走表情報を保存するサービス
    """

    def save_entry_data(self, race_id, entry_data):
        """
        出走表情報を保存

        Args:
            race_id (str): レースID
            entry_data (list): 出走表情報のリスト

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            if not entry_data:
                self.logger.warning(f"レース {race_id} の有効な出走表情報がありません")
                return False

            # 各エントリに一意のIDを付与
            entries = []
            for i, entry in enumerate(entry_data):
                entry_id = f"{race_id}_{entry.get('frame_num', i+1)}"

                entry_dict = {
                    "entry_id": entry_id,
                    "race_id": race_id,
                    "rider_id": entry.get("racer_id", ""),  # 選手IDがない場合は空文字
                    "rider_name": entry.get("racer_name", ""),
                    "frame_num": int(entry.get("frame_num", 0)),
                    "racer_num": entry.get("racer_num", ""),
                    "points": entry.get("points", ""),
                    "win_rate": entry.get("win_rate", ""),
                    "track_score": entry.get("track_score", ""),
                    "created_at": self.get_current_timestamp(),
                    "updated_at": self.get_current_timestamp(),
                }
                entries.append(entry_dict)

            # DataFrameに変換
            entries_df = pd.DataFrame(entries)

            self.logger.info(
                f"レース {race_id} の出走表情報 {len(entries_df)}件を保存します"
            )

            # データベースに保存
            success = self.save_to_database(entries_df, "entries", ["entry_id"])

            if not success:
                self.logger.error(f"レース {race_id} の出走表情報の保存に失敗しました")

            return success

        except Exception as e:
            self.logger.error(f"出走表情報の保存中にエラー: {str(e)}", exc_info=True)
            return False

    def entry_exists(self, race_id):
        """
        レースの出走表情報が既に存在するかチェック

        Args:
            race_id (str): レースID

        Returns:
            bool: 存在する場合はTrue
        """
        try:
            query = f"SELECT COUNT(*) FROM entries WHERE race_id = '{race_id}'"
            result = self.db.execute_query(query)

            if result and result[0][0] > 0:
                self.logger.info(
                    f"レース {race_id} の出走表情報は既に存在します（{result[0][0]}件）"
                )
                return True
            else:
                self.logger.info(f"レース {race_id} の出走表情報はまだ存在しません")
                return False

        except Exception as e:
            self.logger.error(f"出走表情報の確認中にエラー: {str(e)}", exc_info=True)
            return False
