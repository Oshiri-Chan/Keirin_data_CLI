"""
Winticketオッズ情報保存サービス
"""

import pandas as pd

from .base_saver import WinticketBaseSaver


class WinticketOddsSaver(WinticketBaseSaver):
    """
    Winticketのオッズ情報を保存するサービス
    """

    def save_odds_data(self, race_id, odds_data):
        """
        オッズ情報を保存

        Args:
            race_id (str): レースID
            odds_data (dict): オッズ情報の辞書

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            if not odds_data:
                self.logger.warning(f"レース {race_id} の有効なオッズ情報がありません")
                return False

            # オッズタイプごとにデータを整形
            payouts_list = []

            # 3連単オッズ
            if "odds_3t" in odds_data and odds_data["odds_3t"]:
                for bet_number, odds_value in odds_data["odds_3t"].items():
                    payouts_list.append(
                        {
                            "race_id": race_id,
                            "odds_type": "3t",
                            "bet_number": bet_number,
                            "odds_value": float(odds_value),
                            "updated_at": self.get_current_timestamp(),
                        }
                    )

            # 3連複オッズ
            if "odds_3f" in odds_data and odds_data["odds_3f"]:
                for bet_number, odds_value in odds_data["odds_3f"].items():
                    payouts_list.append(
                        {
                            "race_id": race_id,
                            "odds_type": "3f",
                            "bet_number": bet_number,
                            "odds_value": float(odds_value),
                            "updated_at": self.get_current_timestamp(),
                        }
                    )

            # 2連単オッズ
            if "odds_2t" in odds_data and odds_data["odds_2t"]:
                for bet_number, odds_value in odds_data["odds_2t"].items():
                    payouts_list.append(
                        {
                            "race_id": race_id,
                            "odds_type": "2t",
                            "bet_number": bet_number,
                            "odds_value": float(odds_value),
                            "updated_at": self.get_current_timestamp(),
                        }
                    )

            # 2連複オッズ
            if "odds_2f" in odds_data and odds_data["odds_2f"]:
                for bet_number, odds_value in odds_data["odds_2f"].items():
                    payouts_list.append(
                        {
                            "race_id": race_id,
                            "odds_type": "2f",
                            "bet_number": bet_number,
                            "odds_value": float(odds_value),
                            "updated_at": self.get_current_timestamp(),
                        }
                    )

            # 単勝オッズ
            if "odds_win" in odds_data and odds_data["odds_win"]:
                for bet_number, odds_value in odds_data["odds_win"].items():
                    payouts_list.append(
                        {
                            "race_id": race_id,
                            "odds_type": "win",
                            "bet_number": bet_number,
                            "odds_value": float(odds_value),
                            "updated_at": self.get_current_timestamp(),
                        }
                    )

            # DataFrameに変換
            if not payouts_list:
                self.logger.warning(
                    f"レース {race_id} の有効なオッズデータがありません"
                )
                return False

            payouts_df = pd.DataFrame(payouts_list)

            self.logger.info(
                f"レース {race_id} のオッズ情報 {len(payouts_df)}件を保存します"
            )

            # データベースに保存
            success = self.save_to_database(
                payouts_df, "odds", ["race_id", "odds_type", "bet_number"]
            )

            if not success:
                self.logger.error(f"レース {race_id} のオッズ情報の保存に失敗しました")

            return success

        except Exception as e:
            self.logger.error(f"オッズ情報の保存中にエラー: {str(e)}", exc_info=True)
            return False

    def odds_exists(self, race_id, odds_type=None):
        """
        レースのオッズ情報が既に存在するかチェック

        Args:
            race_id (str): レースID
            odds_type (str): オッズタイプ（省略時は全タイプ）

        Returns:
            bool: 存在する場合はTrue
        """
        try:
            query = f"SELECT COUNT(*) FROM odds WHERE race_id = '{race_id}'"

            if odds_type:
                query += f" AND odds_type = '{odds_type}'"

            result = self.db.execute_query(query)

            if result and result[0][0] > 0:
                type_str = f"（{odds_type}）" if odds_type else ""
                self.logger.info(
                    f"レース {race_id} のオッズ情報{type_str}は既に存在します（{result[0][0]}件）"
                )
                return True
            else:
                type_str = f"（{odds_type}）" if odds_type else ""
                self.logger.info(
                    f"レース {race_id} のオッズ情報{type_str}はまだ存在しません"
                )
                return False

        except Exception as e:
            self.logger.error(f"オッズ情報の確認中にエラー: {str(e)}", exc_info=True)
            return False
