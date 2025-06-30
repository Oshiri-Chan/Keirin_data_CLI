"""
ステップ1: 月間開催情報の取得・更新クラス (MySQL対応)
"""

import calendar
import logging
import time
from datetime import date, datetime
from typing import Any, Optional

# Step1Saver と APIクライアント(仮に BaseKeirinAPI) をインポート
from services.savers.step1_saver import Step1Saver  # パスは環境に合わせてください

# import json # json モジュールは save_monthly_cups の中では直接使われていなかったので一旦コメントアウト


# from services.clients.base_keirin_api import BaseKeirinApi # APIクライアントの具体的なクラス名に置き換えてください


class Step1Updater:
    """
    ステップ1: 月間開催情報を取得・更新するクラス (MySQL対応)
    """

    # def __init__(self, winticket_api, db_instance, saver, logger=None): # 旧コンストラクタ
    def __init__(
        self, api_client: Any, saver: Step1Saver, logger: logging.Logger = None
    ):
        """
        初期化

        Args:
            api_client: APIクライアントインスタンス (型は実際のクライアントクラスに置き換えてください)
            saver (Step1Saver): Step1Saver のインスタンス
            logger (logging.Logger, optional): ロガーオブジェクト。 Defaults to None.
        """
        self.api = api_client  # winticket_api を api_client に変更
        # self.db = db_instance # db_instance は不要なので削除
        self.saver = saver
        self.logger = logger or logging.getLogger(__name__)

    def update_monthly_cups(self, year: int, month: int) -> tuple[bool, list]:
        """
        指定された年月の月間開催情報を更新

        Args:
            year (int): 年
            month (int): 月

        Returns:
            tuple: (成功したかどうか, 更新/追加された cup_id のリスト)
        """
        try:
            self.logger.info(f"{year}年{month}月の月間開催情報を取得・保存します")
            date_str = f"{year}{month:02d}01"
            monthly_data_from_api = self.api.get_monthly_cups(
                date_str
            )  # APIから取得するデータ

            if not monthly_data_from_api:
                self.logger.error(
                    f"{year}年{month}月の月間開催情報の取得に失敗しました (APIレスポンスが空)。"
                )
                return False, []

            # Step1Saver の save_monthly_cups を呼び出す
            # このメソッドは内部で地域、会場、カップ、スケジュールの保存を行う想定
            success, saved_cup_ids = self.saver.save_monthly_cups(monthly_data_from_api)

            if success:
                self.logger.info(
                    f"{year}年{month}月の月間開催情報を更新しました（処理された開催数: {len(saved_cup_ids)}件）"
                )
            else:
                self.logger.warning(
                    f"{year}年{month}月の月間開催情報の更新に一部失敗しました"
                )

            return success, saved_cup_ids

        except Exception as e:
            self.logger.error(
                f"{year}年{month}月の月間開催情報の更新中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False, []

    def update_period(
        self, start_date_str: Optional[str] = None, end_date_str: Optional[str] = None
    ) -> tuple[bool, dict]:
        """
        指定された期間の開催情報を更新する (月単位でAPIから取得し、DBに保存)

        Args:
            start_date_str (str, optional): 開始日 (YYYY-MM-DD or YYYYMMDD)。指定がない場合は現在の年月の初日。
            end_date_str (str, optional): 終了日 (YYYY-MM-DD or YYYYMMDD)。指定がない場合は現在の年月の末日。

        Returns:
            tuple: (全体として成功したか, {'regions': [...], 'venues': [...], 'cups': [...]} の形で保存されたIDリスト)
        """
        self.logger.info(
            f"Step1 Updater (update_period) 起動: 期間 {start_date_str} - {end_date_str}"
        )

        try:
            if start_date_str is None or end_date_str is None:
                now = datetime.now()
                target_first_date = date(now.year, now.month, 1)
                last_day_of_month = calendar.monthrange(now.year, now.month)[1]
                target_last_date = date(now.year, now.month, last_day_of_month)
            else:
                target_first_date = datetime.strptime(
                    start_date_str.replace("-", ""), "%Y%m%d"
                ).date()
                target_last_date = datetime.strptime(
                    end_date_str.replace("-", ""), "%Y%m%d"
                ).date()

            self.logger.info(
                f"処理対象期間: {target_first_date} から {target_last_date}"
            )

            all_regions_to_save = []
            all_venues_to_save = []
            all_cups_to_save = []
            # all_schedules_to_save = [] # schedule は Step1Saver.save_monthly_cups で処理される想定

            # 既存ID管理はSaver側で行うためUpdater側では不要

            current_month_start = date(
                target_first_date.year, target_first_date.month, 1
            )

            while current_month_start <= target_last_date:
                year_to_fetch = current_month_start.year
                month_to_fetch = current_month_start.month
                self.logger.info(
                    f"{year_to_fetch}年{month_to_fetch}月のデータをAPIから取得・処理します"
                )

                api_date_str = f"{year_to_fetch}{month_to_fetch:02d}01"
                monthly_api_response = self.api.get_monthly_cups(
                    api_date_str
                )  # APIから月間データを取得

                if not monthly_api_response or "month" not in monthly_api_response:
                    self.logger.warning(
                        f"{year_to_fetch}年{month_to_fetch}月のAPIデータ取得に失敗、またはmonthキーが存在しません。スキップします。"
                    )
                    # 次の月へ
                    if current_month_start.month == 12:
                        current_month_start = date(current_month_start.year + 1, 1, 1)
                    else:
                        current_month_start = date(
                            current_month_start.year, current_month_start.month + 1, 1
                        )
                    time.sleep(0.1)  # 念のため
                    continue

                month_content = monthly_api_response["month"]

                # Regions データ整形
                if "regions" in month_content and month_content["regions"]:
                    for region_api in month_content["regions"]:
                        if region_api.get("id"):  # IDがないものはスキップ
                            all_regions_to_save.append(
                                {
                                    "region_id": str(region_api["id"]),
                                    "region_name": region_api.get("name"),
                                }
                            )

                # Venues データ整形
                if "venues" in month_content and month_content["venues"]:
                    for venue_api in month_content["venues"]:
                        if venue_api.get("id"):  # IDがないものはスキップ
                            venue_data = {
                                "venue_id": str(venue_api["id"]),
                                "venue_name": venue_api.get("name"),
                                "name1": venue_api.get("name1"),
                                "address": venue_api.get("address"),
                                "phoneNumber": venue_api.get("phoneNumber"),
                                "websiteUrl": venue_api.get("websiteUrl"),
                                "bankFeature": venue_api.get("bankFeature"),
                                "trackStraightDistance": self._safe_float_convert(
                                    venue_api.get("trackStraightDistance")
                                ),
                                "trackAngleCenter": venue_api.get("trackAngleCenter"),
                                "trackAngleStraight": venue_api.get(
                                    "trackAngleStraight"
                                ),
                                "homeWidth": self._safe_int_convert(
                                    venue_api.get("homeWidth")
                                ),
                                "backWidth": self._safe_int_convert(
                                    venue_api.get("backWidth")
                                ),
                                "centerWidth": self._safe_float_convert(
                                    venue_api.get("centerWidth")
                                ),
                                # 'region_id' は venues APIレスポンスに通常含まれないため、
                                # venues テーブル定義で region_id が NOT NULL の場合、別途取得・設定ロジックが必要
                                # 今回は venues テーブルに region_id がないか、NULL許容と仮定
                            }
                            all_venues_to_save.append(
                                {k: v for k, v in venue_data.items() if v is not None}
                            )

                # Cups データ整形 (Schedules はCupの中にネストされている想定でSaver側で処理)
                if "cups" in month_content and month_content["cups"]:
                    for cup_api in month_content["cups"]:
                        cup_id = cup_api.get("id")
                        start_date_api = cup_api.get("startDate")
                        end_date_api = cup_api.get("endDate")

                        if not all(
                            [
                                cup_id,
                                start_date_api,
                                end_date_api,
                                cup_api.get("venueId"),
                            ]
                        ):
                            self.logger.warning(
                                f"必須情報が欠けたカップデータのためスキップ: {cup_api}"
                            )
                            continue

                        # APIの日付文字列をYYYY-MM-DD形式に正規化 (Saverは datetime.date を期待するかもしれない)
                        try:
                            cup_start_dt = datetime.strptime(
                                start_date_api.replace("-", ""), "%Y%m%d"
                            ).date()
                            cup_end_dt = datetime.strptime(
                                end_date_api.replace("-", ""), "%Y%m%d"
                            ).date()
                        except ValueError:
                            self.logger.warning(
                                f"日付形式が不正なカップデータのためスキップ: {cup_api}"
                            )
                            continue

                        # 期間フィルター: APIから取得したカップが指定期間内か確認
                        if (
                            cup_end_dt >= target_first_date
                            and cup_start_dt <= target_last_date
                        ):
                            cup_data_to_save = {
                                "cup_id": str(cup_id),
                                "cup_name": cup_api.get("name"),
                                "startDate": start_date_api,  # APIの元の形式
                                "endDate": end_date_api,  # APIの元の形式
                                "duration": self._safe_int_convert(
                                    cup_api.get("duration"), None
                                ),
                                "grade": self._safe_int_convert(
                                    cup_api.get("grade"), None
                                ),
                                "venueId": str(cup_api["venueId"]),  # APIのキー名を保持
                                "labels": cup_api.get("labels", []),  # APIの配列形式
                                "playersUnfixed": cup_api.get("playersUnfixed", False),
                            }
                            all_cups_to_save.append(
                                {
                                    k: v
                                    for k, v in cup_data_to_save.items()
                                    if v is not None
                                }
                            )

                # 次の月へ
                if current_month_start.month == 12:
                    current_month_start = date(current_month_start.year + 1, 1, 1)
                else:
                    current_month_start = date(
                        current_month_start.year, current_month_start.month + 1, 1
                    )
                time.sleep(0.2)  # API負荷軽減

            # --- データベースへの保存処理 ---
            saved_ids_map = {"regions": [], "venues": [], "cups": [], "schedules": []}
            overall_success = True

            if all_regions_to_save:
                self.logger.info(f"{len(all_regions_to_save)} 件の地域情報を保存します")
                try:
                    self.saver.save_regions_batch(
                        all_regions_to_save
                    )  # 戻り値をアンパックしない
                    # 成功した場合、処理対象となったIDを保存（例）
                    saved_ids_map["regions"] = [
                        r.get("region_id")
                        for r in all_regions_to_save
                        if r.get("region_id")
                    ]
                    self.logger.info("地域情報の保存処理が正常に完了しました。")
                except Exception as e:
                    self.logger.error(
                        f"地域情報の保存中にエラーが発生: {e}", exc_info=True
                    )
                    overall_success = False

            if all_venues_to_save:
                self.logger.info(f"{len(all_venues_to_save)} 件の会場情報を保存します")
                try:
                    self.saver.save_venues_batch(
                        all_venues_to_save
                    )  # 戻り値をアンパックしない
                    # 成功した場合、処理対象となったIDを保存（例）
                    saved_ids_map["venues"] = [
                        v.get("venue_id")
                        for v in all_venues_to_save
                        if v.get("venue_id")
                    ]
                    self.logger.info("会場情報の保存処理が正常に完了しました。")
                except Exception as e:
                    self.logger.error(
                        f"会場情報の保存中にエラーが発生: {e}", exc_info=True
                    )
                    overall_success = False

            if all_cups_to_save:
                self.logger.info(f"{len(all_cups_to_save)} 件のカップ情報を保存します")
                try:
                    self.saver.save_cups_batch(
                        all_cups_to_save
                    )  # 戻り値をアンパックしない
                    # 成功した場合、処理対象となったIDを保存（例）
                    saved_ids_map["cups"] = [
                        c.get("cup_id") for c in all_cups_to_save if c.get("cup_id")
                    ]
                    self.logger.info("カップ情報の保存処理が正常に完了しました。")
                except Exception as e:
                    self.logger.error(
                        f"カップ情報の保存中にエラーが発生: {e}", exc_info=True
                    )
                    overall_success = False

                # schedules_data の準備と保存 (APIレスポンスとDBスキーマに依存)
                # monthly_api_response から schedules に必要な情報を抽出し、all_schedules_to_save に追加するロジックが必要
                # 例: APIレスポンスの cup['days'] や cup['schedule_info'] などをパース
                # all_schedules_to_save = self._extract_schedules_from_monthly_response(monthly_api_response, saved_cup_ids)
                # if all_schedules_to_save:
                #    self.logger.info(f'{len(all_schedules_to_save)} 件のスケジュール情報を保存します')
                #    success, saved_schedule_ids = self.saver.save_schedules_batch(all_schedules_to_save)
                #    if success: saved_ids_map['schedules'] = saved_schedule_ids
                #    else: overall_success = False

            if overall_success:
                self.logger.info(
                    f"期間 {target_first_date} - {target_last_date} の開催情報更新処理が正常に完了しました。"
                )
            else:
                self.logger.warning(
                    f"期間 {target_first_date} - {target_last_date} の開催情報更新処理中に一部エラーが発生しました。"
                )

            return overall_success, saved_ids_map

        except Exception as e:
            self.logger.error(
                f"期間 {start_date_str} - {end_date_str} の更新処理中に予期せぬエラー: {e}",
                exc_info=True,
            )
            return False, {}

    def _safe_int_convert(
        self, value: Any, default: Optional[int] = 0
    ) -> Optional[int]:
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def _safe_float_convert(
        self, value: Any, default: Optional[float] = 0.0
    ) -> Optional[float]:
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default


# # 使用例 (コメントアウト)
# if __name__ == '__main__':
#     from database.db_accessor import KeirinDataAccessor # 実際のKeirinDataAccessorを使用
#     from services.clients.winticket_client import WinticketKeirinCrawler # APIクライアントの例

#     # ロガー設定
#     logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#     logger = logging.getLogger('Step1UpdaterTest')

#     # --- 依存関係のインスタンス化 ---
#     # 1. KeirinDataAccessor (MySQL接続情報を使って初期化)
#     # db_config = { 'host': 'localhost', 'user': 'your_user', 'password': 'your_password', 'database': 'keirin_db' }
#     # try:
#     #     accessor = KeirinDataAccessor(db_config, logger)
#     # except Exception as e:
#     #     logger.error(f"データベースアクセサーの初期化に失敗: {e}")
#     #     exit(1)

#     # 2. APIクライアント
#     # api_client = WinticketKeirinCrawler(logger=logger) # APIキーなどが必要な場合は適宜渡す

#     # 3. Step1Saver (KeirinDataAccessorを渡して初期化)
#     # try:
#     #     step1_saver = Step1Saver(accessor, logger)
#     # except Exception as e:
#     #    logger.error(f"Step1Saverの初期化に失敗: {e}")
#     #    exit(1)

#     # 4. Step1Updater (APIクライアントとStep1Saverを渡して初期化)
#     # try:
#     #     step1_updater = Step1Updater(api_client, step1_saver, logger)
#     # except Exception as e:
#     #     logger.error(f"Step1Updaterの初期化に失敗: {e}")
#     #     exit(1)

#     # --- テスト実行 ---
#     logger.info("--- update_monthly_cups のテスト (例: 2024年1月) ---")
#     # success_monthly, updated_ids_monthly = step1_updater.update_monthly_cups(2024, 1)
#     # logger.info(f"月間更新結果: Success={success_monthly}, Updated Cup IDs={updated_ids_monthly}")
#     # time.sleep(1) # API負荷軽減

#     logger.info("\n--- update_period のテスト (例: 2023年12月1日から2024年1月31日) ---")
#     # success_period, updated_ids_map_period = step1_updater.update_period(start_date_str='2023-12-01', end_date_str='2024-01-31')
#     # logger.info(f"期間更新結果: Success={success_period}, Updated IDs Map={updated_ids_map_period}")

#     # --- 接続クローズ (KeirinDataAccessorに実装されていれば) ---
#     # if hasattr(accessor, 'close_connection') and callable(getattr(accessor, 'close_connection')):
#     #     accessor.close_connection()
#     #     logger.info("データベース接続をクローズしました。")
#     pass
