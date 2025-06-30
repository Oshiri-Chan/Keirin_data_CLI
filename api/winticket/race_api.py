"""
Winticket レース情報取得API
"""

from .base_api import WinticketBaseAPI


class WinticketRaceAPI(WinticketBaseAPI):
    """
    Winticketのレース情報を取得するAPI
    """

    def get_race_info(self, race_id):
        """
        レース基本情報を取得

        Args:
            race_id (str): レースID (cup_id)

        Returns:
            dict: レース情報
        """
        self.logger.info(f"=== レース {race_id} の基本情報取得を開始 ===")

        try:
            # APIエンドポイントを呼び出し
            url = f"{self.API_URL}/keirin/cups/{race_id}?fields=cup,schedules,races&pfm=web"

            self.logger.debug(f"レース情報取得URL: {url}")
            self.logger.info("Winticket APIへリクエスト送信中...")

            data = self._make_api_request(url)

            if not data:
                self.logger.error("APIからのレスポンスがありません")
                return None

            # デバッグ用にレスポンス全体の概要をログに出力
            if "cup" in data:
                self.logger.info(
                    f"カップデータ取得成功: {data['cup'].get('name', '不明')}"
                )
            else:
                self.logger.warning(
                    f"期待されるデータ構造ではありません。キー: {list(data.keys())}"
                )

            # 開催基本情報
            cup_info = data.get("cup", {})
            venue_id = cup_info.get("venueId", "")
            venue_name = cup_info.get("venueName", "不明")

            # 開催期間と競輪場IDから基本情報を生成
            race_info = {
                "race_id": race_id,
                "venue_id": venue_id,
                "venue_name": venue_name,
                "race_number": cup_info.get("raceNumber", 0),
                "race_name": cup_info.get("name", ""),
                "race_type": cup_info.get("type", ""),
                "distance": cup_info.get("distance", 0),
                "start_time": cup_info.get("startTime", ""),
                "end_time": cup_info.get("endTime", ""),
                "status": cup_info.get("status", 0),
                "entry_count": cup_info.get("entryCount", 0),
            }

            # レースステータスのログ出力
            status = race_info.get("status")
            if status == 3:
                self.logger.info(
                    f"レース {race_id} は終了しています（ステータス: {status}）"
                )
            else:
                self.logger.info(
                    f"レース {race_id} は未完了です（ステータス: {status}）"
                )

            # 詳細なレース情報があれば追加
            races = data.get("races", [])
            if races:
                races_count = len(races)
                self.logger.info(f"レース情報あり: {races_count}件")

                # レース番号などの詳細は、最初のレース情報から取得
                first_race = races[0]
                race_info["race_number"] = first_race.get("number", "1")

                # レースのステータスを取得・確認
                race_status = first_race.get("status", "不明")

                # cup_infoかfirst_raceのどちらかで終了ステータス(3)が設定されていれば終了とする
                if race_status == 3 or status == 3:
                    race_info["is_finished"] = True
                    race_info["status"] = 3  # ステータスを終了に統一
                    self.logger.info(f"レースは終了済み (status: {race_status})")
                else:
                    race_info["is_finished"] = False
                    self.logger.info(f"レースは未終了 (status: {race_status})")
            else:
                self.logger.warning("レース詳細情報がありません")

            self.logger.info(f"レース {race_id} の基本情報取得が完了しました")
            return race_info

        except Exception as e:
            import traceback

            self.logger.error(f"レース情報の解析中に予期せぬエラー: {str(e)}")
            self.logger.debug(f"エラーの詳細: {traceback.format_exc()}")
            return None
