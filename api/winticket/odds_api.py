"""
Winticket オッズ情報取得API
"""

from .base_api import WinticketBaseAPI


class WinticketOddsAPI(WinticketBaseAPI):
    """
    Winticketのオッズ情報を取得するAPI
    """

    def get_odds_data(self, race_id, target_date=None):
        """
        オッズ情報を取得

        Args:
            race_id (str): レースID
            target_date (str): 特定の日付（YYYYMMDD形式、省略時は最新）

        Returns:
            dict: オッズ情報
        """
        try:
            # APIエンドポイントを使用してレース情報を取得
            cup_url = f"{self.API_URL}/keirin/cups/{race_id}?fields=cup,schedules,races&pfm=web"
            self.logger.debug(f"レース情報取得URL: {cup_url}")

            cup_data = self._make_api_request(cup_url)

            if not cup_data:
                self.logger.error("APIからのレスポンスがありません")
                return None

            # 日程と対応するインデックスを取得
            schedule_id, schedule_index = self._get_schedule_id_for_date(
                cup_data, target_date
            )

            if not schedule_id:
                self.logger.warning("有効なスケジュールIDが取得できませんでした")
                return None

            self.logger.debug(
                f"取得したスケジュールID: {schedule_id}, インデックス: {schedule_index}"
            )

            # レース番号を取得
            race_number = None
            if "races" in cup_data and cup_data["races"]:
                race_number = cup_data["races"][0]["number"]

            if not race_number:
                self.logger.warning(f"レース番号が取得できません: {race_id}")
                return None

            # オッズ情報を取得（インデックスを使用）
            url = (
                f"{self.API_URL}/keirin/cups/{race_id}/schedules/{schedule_index}/"
                f"races/{race_number}/odds?fields=odds&pfm=web"
            )
            self.logger.debug(f"オッズ情報取得URL: {url}")

            try:
                response = self.session.get(url, timeout=30)
                if response.status_code == 404:
                    self.logger.info(f"オッズ情報はまだ公開されていません (404): {url}")
                    return None

                # エラーチェック（404以外の場合）
                response.raise_for_status()
            except Exception as e:
                self.logger.error(f"オッズ情報の取得に失敗: {str(e)}")
                return None

            # JSONデータを解析
            data = response.json()

            # オッズデータを構造化
            odds_data = {
                "odds_3t": {},
                "odds_3f": {},
                "odds_2t": {},
                "odds_2f": {},
                "odds_win": {},
            }

            # 3連単オッズ
            if "odds" in data and "tripleExacta" in data["odds"]:
                for odds in data["odds"]["tripleExacta"]:
                    key = (
                        f"{odds.get('first', '')}"
                        f"-{odds.get('second', '')}"
                        f"-{odds.get('third', '')}"
                    )
                    odds_data["odds_3t"][key] = odds.get("ratio", 0)

            # 3連複オッズ
            if "odds" in data and "tripleQuinella" in data["odds"]:
                for odds in data["odds"]["tripleQuinella"]:
                    key = (
                        f"{odds.get('first', '')}"
                        f"-{odds.get('second', '')}"
                        f"-{odds.get('third', '')}"
                    )
                    odds_data["odds_3f"][key] = odds.get("ratio", 0)

            # 2連単オッズ
            if "odds" in data and "exacta" in data["odds"]:
                for odds in data["odds"]["exacta"]:
                    key = f"{odds.get('first', '')}-{odds.get('second', '')}"
                    odds_data["odds_2t"][key] = odds.get("ratio", 0)

            # 2連複オッズ
            if "odds" in data and "quinella" in data["odds"]:
                for odds in data["odds"]["quinella"]:
                    key = f"{odds.get('first', '')}-{odds.get('second', '')}"
                    odds_data["odds_2f"][key] = odds.get("ratio", 0)

            # 単勝オッズ
            if "odds" in data and "win" in data["odds"]:
                for odds in data["odds"]["win"]:
                    key = str(odds.get("number", ""))
                    odds_data["odds_win"][key] = odds.get("ratio", 0)

            self.logger.info(f"レース {race_id} のオッズ情報取得が完了しました")
            return odds_data

        except Exception as e:
            import traceback

            self.logger.error(f"オッズ情報の解析中に予期せぬエラー: {str(e)}")
            self.logger.debug(f"エラーの詳細: {traceback.format_exc()}")
            return None
