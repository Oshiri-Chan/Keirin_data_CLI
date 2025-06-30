"""
Winticket 出走表情報取得API
"""

from .base_api import WinticketBaseAPI


class WinticketEntryAPI(WinticketBaseAPI):
    """
    Winticketの出走表情報を取得するAPI
    """

    def get_entry_data(self, race_id, target_date=None):
        """
        出走表情報を取得

        Args:
            race_id (str): レースID
            target_date (str): 特定の日付（YYYYMMDD形式、省略時は最新）

        Returns:
            list: 選手情報のリスト
        """
        try:
            # APIエンドポイントを使用して races 情報を取得
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

            # レース番号を取得（該当スケジュールの最初のレース）
            race_number = None
            if "races" in cup_data and cup_data["races"]:
                race_number = cup_data["races"][0]["number"]

            if not race_number:
                self.logger.warning(f"レース番号が取得できません: {race_id}")
                return None

            # 詳細なレース情報を取得（インデックスを使用）
            url = (
                f"{self.API_URL}/keirin/cups/{race_id}/schedules/{schedule_index}/"
                f"races/{race_number}?fields=race,players&pfm=web"
            )
            self.logger.debug(f"出走表情報取得URL: {url}")

            response = self.session.get(url, timeout=30)
            if response.status_code == 404:
                self.logger.info(f"出走表情報はまだ公開されていません (404): {url}")
                return None

            # エラーチェック（404以外の場合）
            response.raise_for_status()

            # JSONデータを解析
            data = response.json()
            entries = []

            if "players" in data:
                for player in data["players"]:
                    entry = {
                        "race_id": race_id,
                        "frame_num": str(player.get("frame", "")),
                        "racer_num": str(player.get("number", "")),
                        "racer_name": player.get("name", ""),
                        "points": str(player.get("point", "")),
                        "win_rate": (
                            f"{player.get('placeRate1', 0)}"
                            f"-{player.get('placeRate2', 0)}"
                            f"-{player.get('placeRate3', 0)}"
                        ),
                        "track_score": str(player.get("coursePoint", "")),
                    }
                    entries.append(entry)

            self.logger.info(
                f"レース {race_id} の出走表情報取得が完了しました（選手数: {len(entries)}）"
            )
            return entries

        except Exception as e:
            import traceback

            self.logger.error(f"出走表情報の解析中に予期せぬエラー: {str(e)}")
            self.logger.debug(f"エラーの詳細: {traceback.format_exc()}")
            return None
