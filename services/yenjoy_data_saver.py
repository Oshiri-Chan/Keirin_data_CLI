"""
Yenjoyデータ保存サービス
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple

import pandas as pd

# Step5Saver をインポート (ファイルは後で作成)
from services.savers.step5_saver import Step5Saver


class YenjoyDataSaver:
    """
    Yenjoyのデータ保存を担当するクラス
    """

    def __init__(self, db_instance, data_saver=None, logger=None):
        """
        初期化

        Args:
            db_instance: データベースインスタンス
            data_saver: 共通データ保存インスタンス（省略時はNone）
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
        """
        self.db = db_instance
        self.data_saver = data_saver
        self.logger = logger or logging.getLogger(__name__)

    def save_race_position_data(self, race_id, date_str, position_data):
        """
        レース位置情報データを保存

        Args:
            race_id (str): レースID
            date_str (str): 日付文字列（YYYYMMDD）
            position_data (dict): 位置情報データ

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(f"レース {race_id} の位置情報データ保存を開始します")

            if not position_data:
                self.logger.error(f"レース {race_id} の位置情報データがありません")
                return False

            # レースデータがDBに存在するか確認
            race_data = self.db.get_race_data_for_url_construction(race_id=race_id)
            if race_data.empty:
                self.logger.warning(
                    f"レース {race_id} のデータがデータベースに存在しません"
                )

            # 位置情報データから以下の情報を抽出
            # 1. ラップタイム情報（周回ごとの時間）
            # 2. 選手ごとの位置情報

            # ラップタイム情報の保存
            lap_times_saved = False
            if "lap_times" in position_data:
                lap_times_df = pd.DataFrame(
                    [
                        {
                            "race_id": race_id,
                            "lap_number": lap.get("lap_number", 0),
                            "lap_time": lap.get("lap_time", ""),
                            "total_time": lap.get("total_time", ""),
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for lap in position_data["lap_times"]
                    ]
                )

                if not lap_times_df.empty:
                    # データベースに保存
                    lap_times_saved = self.db.process_with_temp_file(
                        lap_times_df,
                        "race_lap_times",
                        ["race_id", "lap_number"],
                        format="csv",
                    )

                    if not lap_times_saved:
                        self.logger.error(
                            f"レース {race_id} のラップタイム情報の保存に失敗しました"
                        )

            # 選手ごとの位置情報の保存
            racer_positions_saved = False
            if "racer_positions" in position_data:
                positions_list = []

                for racer in position_data["racer_positions"]:
                    rider_id = racer.get("rider_id", "")

                    for position in racer.get("positions", []):
                        position_dict = {
                            "race_id": race_id,
                            "rider_id": rider_id,
                            "time_point": position.get("time_point", 0),
                            "lap": position.get("lap", 0),
                            "x_position": position.get("x", 0),
                            "y_position": position.get("y", 0),
                            "speed": position.get("speed", 0),
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        positions_list.append(position_dict)

                if positions_list:
                    positions_df = pd.DataFrame(positions_list)

                    # データベースに保存
                    racer_positions_saved = self.db.process_with_temp_file(
                        positions_df,
                        "racer_positions",
                        ["race_id", "rider_id", "time_point"],
                        format="csv",
                    )

                    if not racer_positions_saved:
                        self.logger.error(
                            f"レース {race_id} の選手位置情報の保存に失敗しました"
                        )

            # 選手ごとのラップタイム情報の保存
            racer_lap_times_saved = False
            if "racer_lap_times" in position_data:
                racer_lap_times_list = []

                for racer in position_data["racer_lap_times"]:
                    rider_id = racer.get("rider_id", "")

                    for lap in racer.get("laps", []):
                        lap_dict = {
                            "race_id": race_id,
                            "rider_id": rider_id,
                            "lap_number": lap.get("lap_number", 0),
                            "lap_time": lap.get("lap_time", ""),
                            "total_time": lap.get("total_time", ""),
                            "speed": lap.get("speed", 0),
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        racer_lap_times_list.append(lap_dict)

                if racer_lap_times_list:
                    racer_lap_times_df = pd.DataFrame(racer_lap_times_list)

                    # データベースに保存
                    racer_lap_times_saved = self.db.process_with_temp_file(
                        racer_lap_times_df,
                        "racer_lap_times",
                        ["race_id", "rider_id", "lap_number"],
                        format="csv",
                    )

                    if not racer_lap_times_saved:
                        self.logger.error(
                            f"レース {race_id} の選手ラップタイム情報の保存に失敗しました"
                        )

            success = lap_times_saved or racer_positions_saved or racer_lap_times_saved

            if success:
                self.logger.info(
                    f"レース {race_id} の位置情報データの保存が完了しました"
                )
            else:
                self.logger.warning(
                    f"レース {race_id} の位置情報データの保存に一部失敗しました"
                )

            return success

        except Exception as e:
            self.logger.error(
                f"レース {race_id} の位置情報データ保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def save_lap_data(self, race_id, date_str, race_laps, racer_laps):
        """
        周回データを保存

        Args:
            race_id (str): レースID
            date_str (str): 日付文字列（YYYYMMDD）
            race_laps (dict): レース周回データ
            racer_laps (dict): 選手周回データ

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(f"レース {race_id} の周回データ保存を開始します")

            # レース周回データの保存
            if race_laps and "laps" in race_laps:
                laps_df = pd.DataFrame(
                    [
                        {
                            "race_id": race_id,
                            "lap_number": lap.get("lapNumber", 0),
                            "lap_time": lap.get("lapTime", ""),
                            "total_time": lap.get("totalTime", ""),
                            "speed": lap.get("speed", 0),
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for lap in race_laps["laps"]
                    ]
                )

                # 一時ファイル経由でレース周回データを保存
                laps_success = self.db.process_with_temp_file(
                    laps_df, "race_lap_times", ["race_id", "lap_number"], format="csv"
                )

                if not laps_success:
                    self.logger.error(
                        f"レース {race_id} の周回データの保存に失敗しました"
                    )
                    return False

            # 選手周回データの保存
            if racer_laps and "racer_laps" in racer_laps:
                racer_laps_df = pd.DataFrame(
                    [
                        {
                            "race_id": race_id,
                            "rider_id": racer.get("playerId", ""),
                            "lap_number": lap.get("lapNumber", 0),
                            "lap_time": lap.get("lapTime", ""),
                            "total_time": lap.get("totalTime", ""),
                            "speed": lap.get("speed", 0),
                            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for racer in racer_laps["racer_laps"]
                        for lap in racer.get("laps", [])
                    ]
                )

                # 一時ファイル経由で選手周回データを保存
                racer_laps_success = self.db.process_with_temp_file(
                    racer_laps_df,
                    "racer_lap_times",
                    ["race_id", "rider_id", "lap_number"],
                    format="csv",
                )

                if not racer_laps_success:
                    self.logger.error(
                        f"レース {race_id} の選手周回データの保存に失敗しました"
                    )
                    return False

            self.logger.info(f"レース {race_id} の周回データ保存が完了しました")
            return True

        except Exception as e:
            self.logger.error(
                f"レース {race_id} の周回データ保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def save_step5_lap_data(self, race_id, date_str, race_laps, racer_laps):
        """
        ステップ5: Yenjoyの周回データを保存

        Args:
            race_id (str): レースID
            date_str (str): 日付文字列（YYYYMMDD）
            race_laps (dict): レース周回データ
            racer_laps (dict): 選手周回データ

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(
                f"ステップ5: レース {race_id} の周回データ保存を開始します"
            )

            # データセーバーが設定されている場合はそちらを使用
            if self.data_saver:
                success = self.data_saver.save_yenjoy_lap_data(
                    race_id, date_str, race_laps, racer_laps
                )
            else:
                # レース周回データの保存
                if race_laps and "laps" in race_laps:
                    laps_df = pd.DataFrame(
                        [
                            {
                                "race_id": race_id,
                                "lap_number": lap.get("lapNumber", 0),
                                "lap_time": lap.get("lapTime", ""),
                                "total_time": lap.get("totalTime", ""),
                                "speed": lap.get("speed", 0),
                                "updated_at": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                            }
                            for lap in race_laps["laps"]
                        ]
                    )

                    # 一時ファイル経由でレース周回データを保存
                    laps_success = self.db.process_with_temp_file(
                        laps_df,
                        "race_lap_times",
                        ["race_id", "lap_number"],
                        format="csv",
                    )
                    if not laps_success:
                        self.logger.error(
                            f"レース {race_id} の周回データの保存に失敗しました"
                        )
                        return False

                # 選手周回データの保存
                success = True
                if racer_laps and "racer_laps" in racer_laps:
                    racer_laps_df = pd.DataFrame(
                        [
                            {
                                "race_id": race_id,
                                "rider_id": racer.get("playerId", ""),
                                "lap_number": lap.get("lapNumber", 0),
                                "lap_time": lap.get("lapTime", ""),
                                "total_time": lap.get("totalTime", ""),
                                "speed": lap.get("speed", 0),
                                "updated_at": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                            }
                            for racer in racer_laps["racer_laps"]
                            for lap in racer.get("laps", [])
                        ]
                    )

                    # 一時ファイル経由で選手周回データを保存
                    racer_laps_success = self.db.process_with_temp_file(
                        racer_laps_df,
                        "racer_lap_times",
                        ["race_id", "rider_id", "lap_number"],
                        format="csv",
                    )
                    if not racer_laps_success:
                        self.logger.error(
                            f"レース {race_id} の選手周回データの保存に失敗しました"
                        )
                        success = False

            if success:
                lap_count = (
                    len(race_laps.get("laps", []))
                    if race_laps and "laps" in race_laps
                    else 0
                )
                racer_count = (
                    len(racer_laps.get("racer_laps", []))
                    if racer_laps and "racer_laps" in racer_laps
                    else 0
                )
                self.logger.info(
                    f"ステップ5: レース {race_id} の周回データを保存しました。周回数: {lap_count}件、選手: {racer_count}人"
                )
            else:
                self.logger.error(
                    f"ステップ5: レース {race_id} の周回データの保存に失敗しました"
                )

            return success
        except Exception as e:
            self.logger.error(
                f"ステップ5: レース {race_id} の周回データ保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def save_step5_position_data(self, race_id, date_str, position_data):
        """
        ステップ5: レース位置情報データを保存

        Args:
            race_id (str): レースID
            date_str (str): 日付文字列（YYYYMMDD）
            position_data (dict): 位置情報データ

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(
                f"ステップ5: レース {race_id} の位置情報データ保存を開始します"
            )

            # データセーバーが設定されている場合はそちらを使用
            if self.data_saver:
                success = self.data_saver.save_position_data(
                    race_id, date_str, position_data
                )
            else:
                # 位置情報データから周回データを構築
                success = True
                if "positions" in position_data:
                    # 周回データの構築と保存
                    lap_times = position_data.get("lap_times", [])
                    if lap_times:
                        lap_times_df = pd.DataFrame(
                            [
                                {
                                    "race_id": race_id,
                                    "lap_number": lap.get("lap_number", 0),
                                    "lap_time": lap.get("lap_time", ""),
                                    "total_time": lap.get("total_time", ""),
                                    "updated_at": datetime.now().strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    ),
                                }
                                for lap in lap_times
                            ]
                        )

                        # 一時ファイル経由でレース周回データを保存
                        lap_times_success = self.db.process_with_temp_file(
                            lap_times_df,
                            "race_lap_times",
                            ["race_id", "lap_number"],
                            format="csv",
                        )

                        if not lap_times_success:
                            self.logger.error(
                                f"レース {race_id} の周回データの保存に失敗しました"
                            )
                            success = False

                    # 選手ごとの位置情報を保存
                    positions = position_data.get("positions", {})
                    if positions:
                        positions_list = []

                        for rider_id, rider_positions in positions.items():
                            for pos in rider_positions:
                                position_dict = {
                                    "race_id": race_id,
                                    "rider_id": rider_id,
                                    "timestamp": pos.get("time", 0),
                                    "x": pos.get("x", 0),
                                    "y": pos.get("y", 0),
                                    "lap": pos.get("lap", 0),
                                    "speed": pos.get("speed", 0),
                                    "updated_at": datetime.now().strftime(
                                        "%Y-%m-%d %H:%M:%S"
                                    ),
                                }
                                positions_list.append(position_dict)

                        if positions_list:
                            positions_df = pd.DataFrame(positions_list)

                            # 一時ファイル経由で位置情報を保存
                            positions_success = self.db.process_with_temp_file(
                                positions_df,
                                "rider_positions",
                                ["race_id", "rider_id", "timestamp"],
                                format="csv",
                            )

                            if not positions_success:
                                self.logger.error(
                                    f"レース {race_id} の位置情報の保存に失敗しました"
                                )
                                success = False

            if success:
                rider_count = len(position_data.get("positions", {}))
                position_count = sum(
                    len(positions)
                    for positions in position_data.get("positions", {}).values()
                )
                self.logger.info(
                    f"ステップ5: レース {race_id} の位置情報データを保存しました。選手数: {rider_count}人、位置情報: {position_count}件"
                )
            else:
                self.logger.error(
                    f"ステップ5: レース {race_id} の位置情報データの保存に失敗しました"
                )

            return success
        except Exception as e:
            self.logger.error(
                f"ステップ5: レース {race_id} の位置情報データ保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def save_step5_race_results(self, race_id, date_str, result_data):
        """
        ステップ5: レース結果情報を保存（Yenjoyから取得したデータ）

        Args:
            race_id (str): レースID
            date_str (str): 日付文字列（YYYYMMDD）
            result_data (dict): 結果情報

        Returns:
            bool: 保存成功の場合はTrue
        """
        try:
            self.logger.info(
                f"ステップ5: レース {race_id} の結果情報（Yenjoy）保存を開始します"
            )

            if not result_data or "results" not in result_data:
                self.logger.error(f"レース {race_id} の有効な結果データがありません")
                return False

            # 結果情報の保存
            results = result_data.get("results", [])
            if results:
                results_df = pd.DataFrame(
                    [
                        {
                            "race_id": race_id,
                            "rider_id": result.get("rider_id", ""),
                            "frame_number": result.get("frame_number", 0),
                            "rank": result.get("rank", 0),
                            "finish_time": result.get("finish_time", ""),
                            "lap_times": (
                                ",".join(result.get("lap_times", []))
                                if "lap_times" in result
                                else ""
                            ),
                            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        for result in results
                    ]
                )

                # 一時ファイル経由で結果情報を保存
                results_success = self.db.process_with_temp_file(
                    results_df,
                    "race_results_yenjoy",
                    ["race_id", "rider_id"],
                    format="csv",
                )

                if not results_success:
                    self.logger.error(
                        f"レース {race_id} の結果情報（Yenjoy）の保存に失敗しました"
                    )
                    return False

                # 払戻情報の保存
                if "payouts" in result_data:
                    payouts = result_data.get("payouts", {})
                    payouts_list = []

                    for bet_type, bet_data in payouts.items():
                        for combination, payout_data in bet_data.items():
                            payout_dict = {
                                "race_id": race_id,
                                "bet_type": bet_type,
                                "combination": combination,
                                "amount": payout_data.get("amount", 0),
                                "favorite": payout_data.get("favorite", 0),
                                "updated_at": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                            }
                            payouts_list.append(payout_dict)

                    if payouts_list:
                        payouts_df = pd.DataFrame(payouts_list)

                        # 一時ファイル経由で払戻情報を保存
                        payouts_success = self.db.process_with_temp_file(
                            payouts_df,
                            "race_payouts_yenjoy",
                            ["race_id", "bet_type", "combination"],
                            format="csv",
                        )

                        if not payouts_success:
                            self.logger.error(
                                f"レース {race_id} の払戻情報の保存に失敗しました"
                            )

                self.logger.info(
                    f"ステップ5: レース {race_id} の結果情報（Yenjoy）を保存しました。結果数: {len(results)}件"
                )
                return True
            else:
                self.logger.warning(f"レース {race_id} の結果データがありません")
                return False

        except Exception as e:
            self.logger.error(
                f"ステップ5: レース {race_id} の結果情報（Yenjoy）保存中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False

    def get_finished_races_without_position_data(self, date_str=None, limit=50):
        """
        位置情報がまだ取得されていない完了済みレースを取得

        Args:
            date_str (str, optional): 日付文字列（YYYYMMDD）。指定がない場合は全期間
            limit (int, optional): 取得する最大件数。デフォルトは50件

        Returns:
            List[Dict]: レース情報のリスト
        """
        try:
            self.logger.info("位置情報未取得の完了済みレースを検索します")

            query = """
            SELECT r.race_id, r.date, r.venue
            FROM races r
            LEFT JOIN (
                SELECT DISTINCT race_id FROM rider_positions
            ) p ON r.race_id = p.race_id
            WHERE r.is_finished = 1
            AND p.race_id IS NULL
            """

            if date_str:
                query += f" AND r.date = '{date_str}'"

            query += f" ORDER BY r.date DESC LIMIT {limit}"

            results = self.db.execute_query(query)

            races = []
            if results:
                for row in results:
                    races.append({"race_id": row[0], "date": row[1], "venue": row[2]})

            self.logger.info(f"位置情報未取得の完了済みレース: {len(races)}件")
            return races

        except Exception as e:
            self.logger.error(
                f"位置情報未取得レースの検索中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return []

    # --- Step 5 用の一括保存メソッドを追加 ---
    def bulk_save_step5_data(
        self,
        race_id: str,
        race_results: List[Dict],
        payouts: List[Dict],
        race_comments: List[Dict],
        inspection_reports: List[Dict],
        lap_positions: List[Dict],
    ) -> Tuple[bool, Dict[str, int]]:
        """
        ステップ5: Yenjoy から取得した結果関連データ (結果, 払戻, コメント, レポート, ラップ位置) を一括保存

        Args:
            race_id (str): 保存対象のレースID
            race_results (List[Dict]): race_results テーブル用データのリスト
            payouts (List[Dict]): payouts テーブル用データのリスト
            race_comments (List[Dict]): race_comments テーブル用データのリスト
            inspection_reports (List[Dict]): inspection_reports テーブル用データのリスト
            lap_positions (List[Dict]): lap_positions テーブル用データのリスト

        Returns:
            Tuple[bool, Dict[str, int]]: (成功したかどうか, 各テーブルの保存件数辞書)
        """
        try:
            # ログ出力用に各リストの件数を取得
            result_count = len(race_results) if race_results else 0
            payout_count = len(payouts) if payouts else 0
            comment_count = len(race_comments) if race_comments else 0
            report_count = len(inspection_reports) if inspection_reports else 0
            lap_count = len(lap_positions) if lap_positions else 0

            total_items = (
                result_count + payout_count + comment_count + report_count + lap_count
            )
            if total_items == 0:
                self.logger.info(
                    f"Race ID {race_id}: Step 5 保存対象の Yenjoy データがありません。"
                )
                return True, {}  # 対象なしは成功扱い

            self.logger.info(
                f"Race ID {race_id}: Step 5 Yenjoy データの一括保存を開始します。"
            )
            self.logger.info(f"  - Race Results: {result_count} 件")
            self.logger.info(f"  - Payouts: {payout_count} 件")
            self.logger.info(f"  - Race Comments: {comment_count} 件")
            self.logger.info(f"  - Inspection Reports: {report_count} 件")
            self.logger.info(f"  - Lap Positions: {lap_count} 件")

            step5_saver = Step5Saver(self.db, self.logger)

            parsed_data_for_saver = {
                "race_results": race_results,
                "race_comments": race_comments,
                "inspection_reports": inspection_reports,
                "lap_positions": lap_positions,
            }

            all_saved_successfully = True
            saved_counts = {
                "race_results": 0,
                "race_comments": 0,
                "inspection_reports": 0,
                "lap_positions": 0,
                "lap_data_status": 0,
                "payouts": 0,
            }

            # Step5Saver で主要なデータを保存
            if any(parsed_data_for_saver.values()):
                success_step5_main = step5_saver.save_parsed_html_data(
                    race_id, parsed_data_for_saver
                )
                if success_step5_main:
                    saved_counts["race_results"] = result_count
                    saved_counts["race_comments"] = comment_count
                    saved_counts["inspection_reports"] = report_count
                    saved_counts["lap_positions"] = lap_count
                    saved_counts["lap_data_status"] = 1  # 更新されると仮定
                else:
                    all_saved_successfully = False
                    self.logger.error(
                        f"Race ID {race_id}: Step5Saverによる主要データ保存に失敗。"
                    )

            # Payouts は別途保存
            if payouts:
                payouts_df = pd.DataFrame(payouts)
                if not payouts_df.empty:
                    payouts_table_name = "race_payouts_yenjoy"  # テーブル名確認済
                    cols = list(payouts_df.columns)
                    # ON DUPLICATE KEY UPDATE の対象から主キーを除外
                    update_cols_payout = [
                        f"`{c}`=VALUES(`{c}`)"
                        for c in cols
                        if c not in ["race_id", "bet_type", "combination"]
                    ]
                    update_clause_payout = ", ".join(update_cols_payout)

                    query_payout = f"INSERT INTO {payouts_table_name} ({', '.join([f'`{c}`' for c in cols])}) VALUES ({', '.join(['%s'] * len(cols))})"
                    if (
                        update_clause_payout
                    ):  # 更新対象カラムがある場合のみ UPDATE を追加
                        query_payout += (
                            f" ON DUPLICATE KEY UPDATE {update_clause_payout}"
                        )
                    else:  # 主キーのみの場合は INSERT IGNORE のような挙動にするか、エラーにするか検討。ここではエラーとしない。
                        query_payout += " ON DUPLICATE KEY UPDATE `race_id`=`race_id`"  # 実質何もしないUPDATE

                    params_list_payout = [
                        tuple(row)
                        for row in payouts_df.itertuples(index=False, name=None)
                    ]
                    try:
                        num_inserted_payouts = self.db.execute_many(
                            query_payout, params_list_payout
                        )
                        # execute_many の戻り値は影響を受けた行数なので、必ずしも挿入件数と一致しない場合がある
                        # ここでは便宜上、試行件数として扱うか、より正確な件数取得方法を検討
                        saved_counts["payouts"] = len(params_list_payout)  # 試行件数
                        self.logger.info(
                            f"Race ID {race_id}: {len(params_list_payout)}件の払戻情報の保存/更新を試行しました。 (影響行数: {num_inserted_payouts})"
                        )
                    except Exception as e_payout:
                        self.logger.error(
                            f"Race ID {race_id}: 払戻情報の保存中にエラー: {e_payout}",
                            exc_info=True,
                        )
                        all_saved_successfully = False
                else:
                    self.logger.info(
                        f"Race ID {race_id}: 保存する払戻情報がありません。"
                    )

            if all_saved_successfully:
                self.logger.info(
                    f"Step 5 Yenjoy データ (Race ID: {race_id}) の保存に成功しました。保存件数(試行ベース): {saved_counts}"
                )
            else:
                self.logger.error(
                    f"Step 5 Yenjoy データ (Race ID: {race_id}) の保存に一部または全て失敗しました。保存件数(試行ベース): {saved_counts}"
                )

            return all_saved_successfully, saved_counts

        except ImportError:
            self.logger.error(
                "Step5Saver のインポートに失敗しました。ファイルが存在するか確認してください。",
                exc_info=True,
            )
            return False, {}
        except Exception as e:
            self.logger.error(
                f"Step 5 Yenjoy データ (Race ID: {race_id}) の一括保存中に予期せぬエラーが発生しました: {e}",
                exc_info=True,
            )
            return False, {}
