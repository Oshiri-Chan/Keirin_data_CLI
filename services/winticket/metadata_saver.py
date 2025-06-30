"""
Winticketメタデータ保存サービス
"""

import pandas as pd

from .base_saver import WinticketBaseSaver


class WinticketMetadataSaver(WinticketBaseSaver):
    """
    Winticketのメタデータ（地域、会場など）を保存するサービス
    """

    def save_regions(self, regions_data):
        """
        地域情報を保存

        Args:
            regions_data (list): 地域情報のリスト

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            if not regions_data:
                self.logger.warning("有効な地域情報がありません")
                return False

            # 地域情報をDataFrameに変換
            regions_df = pd.DataFrame(
                [
                    {
                        "region_id": region.get("id", ""),
                        "region_name": region.get("name", ""),
                        "updated_at": self.get_current_timestamp(),
                    }
                    for region in regions_data
                ]
            )

            self.logger.info(f"{len(regions_df)} 件の地域情報を保存します")

            # データベースに保存
            return self.save_to_database(regions_df, "regions", ["region_id"])

        except Exception as e:
            self.logger.error(f"地域情報の保存中にエラー: {str(e)}", exc_info=True)
            return False

    def save_venues(self, venues_data):
        """
        会場情報を保存

        Args:
            venues_data (list): 会場情報のリスト

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            if not venues_data:
                self.logger.warning("有効な会場情報がありません")
                return False

            # 会場情報をDataFrameに変換
            venues_list = []

            for venue in venues_data:
                venue_dict = {
                    "venue_id": venue.get("id", ""),
                    "venue_name": venue.get("name", ""),
                    "venue_short_name": venue.get("name1", ""),
                    "venue_slug": venue.get("slug", ""),
                    "address": venue.get("address", ""),
                    "phone_number": venue.get("phoneNumber", ""),
                    "region_id": venue.get("regionId", ""),
                    "website_url": venue.get("websiteUrl", ""),
                    "twitter_account": venue.get("twitterAccountId", ""),
                    "track_distance": venue.get("trackDistance", 0),
                    "bank_feature": venue.get("bankFeature", ""),
                    "updated_at": self.get_current_timestamp(),
                }

                # 最高記録情報を追加
                if "bestRecord" in venue and venue["bestRecord"]:
                    venue_dict.update(
                        {
                            "best_record_player_id": venue["bestRecord"].get(
                                "playerId", ""
                            ),
                            "best_record_seconds": venue["bestRecord"].get("second", 0),
                            "best_record_date": venue["bestRecord"].get("date", ""),
                        }
                    )

                venues_list.append(venue_dict)

            venues_df = pd.DataFrame(venues_list)

            self.logger.info(f"{len(venues_df)} 件の会場情報を保存します")

            # データベースに保存
            return self.save_to_database(venues_df, "venues", ["venue_id"])

        except Exception as e:
            self.logger.error(f"会場情報の保存中にエラー: {str(e)}", exc_info=True)
            return False

    def save_cups(self, cups_data):
        """
        開催情報を保存

        Args:
            cups_data (list): 開催情報のリスト

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            if not cups_data:
                self.logger.warning("有効な開催情報がありません")
                return False

            # 開催情報をDataFrameに変換
            cups_df = pd.DataFrame(
                [
                    {
                        "cup_id": cup.get("id", ""),
                        "cup_name": cup.get("name", ""),
                        "start_date": cup.get("startDate", ""),
                        "end_date": cup.get("endDate", ""),
                        "duration": cup.get("duration", 0),
                        "grade": cup.get("grade", 0),
                        "venue_id": cup.get("venueId", ""),
                        "labels": ",".join(cup.get("labels", [])),
                        "players_unfixed": 1 if cup.get("playersUnfixed", False) else 0,
                        "updated_at": self.get_current_timestamp(),
                    }
                    for cup in cups_data
                ]
            )

            self.logger.info(f"{len(cups_df)} 件の開催情報を保存します")

            # データベースに保存
            return self.save_to_database(cups_df, "cups", ["cup_id"])

        except Exception as e:
            self.logger.error(f"開催情報の保存中にエラー: {str(e)}", exc_info=True)
            return False

    def save_monthly_data(self, data):
        """
        月間開催情報をまとめて保存

        Args:
            data (dict): APIから取得した月間開催情報

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info("月間開催情報の保存を開始します")

            if not data or not isinstance(data, dict) or "month" not in data:
                self.logger.error("有効な開催情報がありません")
                return False

            month_data = data["month"]
            success = True

            # 地域情報の保存
            if "regions" in month_data and month_data["regions"]:
                regions_success = self.save_regions(month_data["regions"])
                if not regions_success:
                    self.logger.error("地域情報の保存に失敗しました")
                    success = False

            # 会場情報の保存
            if "venues" in month_data and month_data["venues"]:
                venues_success = self.save_venues(month_data["venues"])
                if not venues_success:
                    self.logger.error("会場情報の保存に失敗しました")
                    success = False

            # 開催情報の保存
            if "cups" in month_data and month_data["cups"]:
                cups_success = self.save_cups(month_data["cups"])
                if not cups_success:
                    self.logger.error("開催情報の保存に失敗しました")
                    success = False

            self.logger.info("月間開催情報の保存が完了しました")
            return success

        except Exception as e:
            self.logger.error(
                f"月間開催情報の保存中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return False
