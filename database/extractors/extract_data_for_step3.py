import logging
import threading
from typing import Any, Dict, List, Optional  # noqa: F401


class Step3DataExtractor:
    def __init__(self, database, logger: Optional[logging.Logger] = None):
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    def extract(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        cup_id: Optional[str] = None,
        force_update_all: bool = False,
    ) -> Dict[str, List[Dict]]:
        """
        ステップ3の処理に必要なレース識別子を抽出する
        - 期間内の race_id
        - API呼び出しに必要な情報 (cup_id, schedule_id, number)

        Args:
            start_date (str, optional): 開始日（YYYY-MM-DD形式）
            end_date (str, optional): 終了日（YYYY-MM-DD形式）
            cup_id (str, optional): 特定の開催IDを指定する場合。
                                     指定された場合、start_date/end_dateは無視される。
            force_update_all (bool, optional): race_status の step3_status を無視して抽出するかどうか。
                                    Defaults to False.

        Returns:
            dict: 抽出されたデータ {'races_for_update': [{'race_id': str, 'cup_id': str, ...}, ...]}
        """
        thread_id = threading.current_thread().ident
        self.logger.info(
            f"スレッド {thread_id}: ステップ3のデータ抽出を開始します (期間: {start_date} - {end_date}, cup_id: {cup_id}, 強制: {force_update_all})"
        )

        extracted_data = {"races_for_update": []}
        target_schedule_ids = []
        schedule_info_map = {}

        try:
            # cup_id が指定されている場合、期間指定を無視して該当カップのスケジュールIDを取得
            if cup_id:
                self.logger.info(
                    f"スレッド {thread_id}: 指定された開催ID {cup_id} のスケジュール情報を取得します。"
                )
                query_cup_schedules = """
                SELECT schedule_id, date, schedule_index AS race_index
                FROM schedules
                WHERE cup_id = %s
                ORDER BY date, schedule_index
                """
                params_cup_schedules = (cup_id,)
                schedules_result = self.database.execute_query(
                    query_cup_schedules, params=params_cup_schedules
                )
                if not schedules_result:
                    self.logger.warning(
                        f"スレッド {thread_id}: 開催ID {cup_id} に該当するスケジュールが見つかりませんでした。"
                    )
                    return extracted_data
                schedule_info_map = {
                    row["schedule_id"]: {
                        "date": row["date"],
                        "race_index": row["race_index"],
                    }
                    for row in schedules_result
                    if row["race_index"] is not None
                }
                target_schedule_ids = list(schedule_info_map.keys())
                self.logger.info(
                    f"スレッド {thread_id}: 開催ID {cup_id} から {len(target_schedule_ids)} 件のスケジュールIDを抽出しました（race_index(schedule_index)がNULLのものを除く）。"
                )

            # cup_id が指定されていない場合、期間指定でスケジュールIDを取得
            elif start_date and end_date:
                start_date_ymd = start_date.replace("-", "")
                end_date_ymd = end_date.replace("-", "")
                query_schedules = """
                SELECT schedule_id, date, schedule_index AS race_index
                FROM schedules
                WHERE date BETWEEN %s AND %s
                ORDER BY date, schedule_index
                """
                params_schedules = (start_date_ymd, end_date_ymd)
                schedules_result = self.database.execute_query(
                    query_schedules, params=params_schedules
                )
                if not schedules_result:
                    self.logger.info(
                        f"スレッド {thread_id}: 期間 {start_date} - {end_date} に該当するスケジュールが見つかりませんでした。"
                    )
                    return extracted_data
                schedule_info_map = {
                    row["schedule_id"]: {
                        "date": row["date"],
                        "race_index": row["race_index"],
                    }
                    for row in schedules_result
                    if row["race_index"] is not None
                }
                target_schedule_ids = list(schedule_info_map.keys())
                self.logger.info(
                    f"スレッド {thread_id}: 期間 {start_date} - {end_date} から {len(target_schedule_ids)} 件のスケジュールIDを抽出しました（race_index(schedule_index)がNULLのものを除く）。"
                )

            # どちらも指定されていない場合はエラー
            else:
                self.logger.error(
                    f"スレッド {thread_id}: Step3 Extractor に期間またはcup_idの指定がありません。処理を中止します。"
                )
                return extracted_data

            # 抽出したスケジュールIDが0件の場合、後続のクエリを実行しない
            if not target_schedule_ids:
                self.logger.info(
                    f"スレッド {thread_id}: 抽出されたスケジュールIDが0件のため、レース情報の取得をスキップします。"
                )
                return extracted_data

            # 2. 抽出したスケジュールIDに基づいてレース情報を取得
            placeholders = ",".join(["%s"] * len(target_schedule_ids))

            # ★★★ force_update_all フラグに応じて WHERE 句を変更 ★★★
            where_clause_step3_status = ""  # デフォルトは条件なし
            if not force_update_all:
                # step3_status が 'completed' のレースはスキップ
                # それ以外（'pending', 'processing', 'error', NULL など）は処理対象
                # レース終了フラグは考慮しない（recordsデータは終了後も取得が必要）
                where_clause_step3_status = (
                    "AND (rs.step3_status IS NULL OR rs.step3_status != 'completed')"
                )
                pass

            # races.status に関する条件は廃止 (force_update_all=False でも条件を付与しない)
            where_clause_race_general_status = ""
            # if not force_update_all: # 強制更新でない場合のみ、レースが終了していないことを考慮 # このブロックをコメントアウトまたは削除
            #      where_clause_race_general_status = "AND (r.status IS NULL OR r.status NOT IN ('2', '3'))" # '2':中止, '3':終了

            query_races = f"""
            SELECT
                r.race_id,
                r.cup_id,
                r.schedule_id,
                r.number,
                c.venue_id,
                r.status AS race_table_status,
                rs.step3_status
            FROM races r
            JOIN cups c ON r.cup_id = c.cup_id
            JOIN race_status rs ON r.race_id = rs.race_id
            WHERE r.schedule_id IN ({placeholders})
            {where_clause_step3_status} -- Step3ステータス条件
            {where_clause_race_general_status} -- レース自体の全体ステータス条件
            ORDER BY r.schedule_id, r.number
            """
            params_races = tuple(target_schedule_ids)

            # ★★★ デバッグログに force_update_all 状態を追加 ★★★
            self.logger.debug(
                f"スレッド {thread_id}: Executing query for races (force={force_update_all}). Query: {repr(query_races)}"
            )
            # パラメータログは変更なし
            if len(params_races) > 20:
                self.logger.debug(
                    f"スレッド {thread_id}: Params (first 10): {repr(params_races[:10])}"
                )
                self.logger.debug(
                    f"スレッド {thread_id}: Params (last 10): {repr(params_races[-10:])}"
                )
            else:
                self.logger.debug(f"スレッド {thread_id}: Params: {repr(params_races)}")
            # ★★★ デバッグログ修正ここまで ★★★

            races_result = self.database.execute_query(query_races, params=params_races)

            if not races_result:
                status_msg = (
                    "(強制更新モード)" if force_update_all else "未処理 (Step3)"
                )
                self.logger.info(
                    f"スレッド {thread_id}: 抽出されたスケジュールIDに紐づく{status_msg}のレースが見つかりませんでした。"
                )
                return extracted_data

            # 3. スケジュール情報とレース情報をマージ
            final_race_data = []
            for race_row in races_result:
                schedule_id = race_row["schedule_id"]
                if schedule_id in schedule_info_map:
                    merged_data = dict(race_row)  # レース情報をコピー
                    merged_data["date"] = schedule_info_map[schedule_id]["date"]
                    merged_data["race_index"] = schedule_info_map[schedule_id][
                        "race_index"
                    ]
                    final_race_data.append(merged_data)
                else:
                    self.logger.warning(
                        f"スレッド {thread_id}: レース {race_row['race_id']} の schedule_id ({schedule_id}) がスケジュールマップに見つかりません。"
                    )

            extracted_data["races_for_update"] = final_race_data
            self.logger.info(
                f"スレッド {thread_id}: 更新対象のレース {len(extracted_data['races_for_update'])} 件を最終的に抽出しました"
            )

        except ValueError as ve:
            self.logger.error(f"スレッド {thread_id}: 日付形式エラー: {ve}")
        except Exception as e:
            self.logger.error(
                f"スレッド {thread_id}: データ抽出中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            extracted_data["races_for_update"] = []  # エラー時は空にする

        return extracted_data

    def _extract_existing_player_ids(self) -> List[str]:
        """既存のプレイヤーIDリストを抽出するヘルパーメソッド（例）"""
        try:
            query = "SELECT player_id FROM players"
            players = self.database.execute_query(query)
            return [p["player_id"] for p in players] if players else []
        except Exception as e:
            self.logger.warning(f"既存プレイヤーIDの抽出に失敗: {e}")
            return []
