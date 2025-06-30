"""
Winticket 月間開催情報取得API
"""

from .base_api import WinticketBaseAPI


class WinticketCupsAPI(WinticketBaseAPI):
    """
    Winticketの月間開催情報を取得するAPI
    """

    def get_monthly_races(self, year_month):
        """
        月の開催レース一覧をAPI経由で取得

        Args:
            year_month (str): 年月文字列（YYYYMM）

        Returns:
            dict: 月の開催レース一覧情報
        """
        try:
            # 月初日の日付を作成（YYYYMM01形式）
            date_str = f"{year_month}01"

            # APIエンドポイントを呼び出し
            url = f"{self.API_URL}/keirin/cups?date={date_str}&fields=month,venues,regions&pfm=web"

            data = self._make_api_request(url)

            self.logger.info(f"{year_month}の月間レース情報を取得しました")
            return data

        except Exception as e:
            self.logger.error(f"月間レース一覧の取得に失敗: {str(e)}")
            return None

    def get_race_ids_for_date(self, date_str):
        """
        指定日の開催レースIDを取得

        Args:
            date_str (str): 日付文字列（YYYYMMDD）

        Returns:
            list: レースIDのリスト
        """
        self.logger.info(f"=== {date_str} の開催レースID取得を開始 ===")
        self.logger.info(f"日付: {date_str[:4]}年{date_str[4:6]}月{date_str[6:8]}日")

        try:
            # 月初日を計算 (YYYYMM01形式)
            first_day_of_month = f"{date_str[:6]}01"

            # APIエンドポイントを呼び出し（月初を使用）
            url = f"{self.API_URL}/keirin/cups?date={first_day_of_month}&fields=month&pfm=web"

            self.logger.debug(f"月間レース一覧取得URL: {url}")
            self.logger.info("Winticket APIへリクエスト送信中...")

            data = self._make_api_request(url)

            if not data:
                self.logger.error("APIからのレスポンスがありません")
                return []

            # 月間データから、指定日付に該当するレースを抽出
            race_ids = []
            if "month" in data and "cups" in data["month"]:
                # 月間のカップ数をログに出力
                self.logger.debug(f"月間カップ数: {len(data['month']['cups'])}")

                for cup in data["month"]["cups"]:
                    if "id" in cup and "startDate" in cup and "endDate" in cup:
                        # カップの開催期間をログに出力
                        cup_name = cup.get("name", "なし")
                        cup_id = cup.get("id", "なし")
                        start_date = cup.get("startDate", "なし")
                        end_date = cup.get("endDate", "なし")

                        self.logger.debug(
                            f"カップ: {cup_name}, ID: {cup_id}, 期間: {start_date}～{end_date}"
                        )

                        # 開催期間内かをチェック (開始日 <= 指定日 <= 終了日)
                        # 日付は YYYY-MM-DD 形式なので比較用に変換
                        start_date_formatted = start_date.replace("-", "")
                        end_date_formatted = end_date.replace("-", "")

                        if start_date_formatted <= date_str <= end_date_formatted:
                            self.logger.info(
                                f"一致するカップ: [{cup_name}] ID: {cup_id}, 期間: {start_date}～{end_date}"
                            )
                            race_ids.append(cup_id)
            else:
                self.logger.warning(
                    f"APIレスポンスに 'month' または 'cups' がありません: {data.keys() if data else 'None'}"
                )

                # 詳細なデバッグ情報
                if data:
                    self.logger.debug(
                        f"レスポンスの詳細キー: {self._get_nested_keys(data)}"
                    )

            self.logger.info(f"{date_str} の開催レース数: {len(race_ids)}")
            if race_ids:
                self.logger.info(f"取得したレースID: {race_ids}")
            else:
                self.logger.info(f"{date_str} の開催レースはありません")

            return race_ids

        except Exception as e:
            import traceback

            self.logger.error(f"レース一覧の解析中に予期せぬエラー: {str(e)}")
            self.logger.debug(f"エラーの詳細: {traceback.format_exc()}")
            return []
