"""
ステップ2: 開催詳細情報の取得・更新クラス (MySQL対応)
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# Step2Saver と APIクライアント(仮に BaseKeirinAPI) をインポート
from services.savers.step2_saver import Step2Saver  # パスは環境に合わせてください

# from services.clients.base_keirin_api import BaseKeirinApi # APIクライアントの具体的なクラス名に置き換えてください


class Step2Updater:
    """
    ステップ2: 開催詳細情報を取得・更新するクラス (MySQL対応)
    """

    def __init__(
        self,
        api_client: Any,
        saver: Step2Saver,
        logger: logging.Logger = None,
        max_workers: int = 3,
        rate_limit_wait: float = 1.0,
    ):
        """
        初期化

        Args:
            api_client: APIクライアントインスタンス (型は実際のクライアントクラスに置き換えてください)
            saver (Step2Saver): Step2Saver のインスタンス
            logger (logging.Logger, optional): ロガーオブジェクト。 Defaults to None.
            max_workers (int): 並列処理の最大ワーカー数
            rate_limit_wait (float): API呼び出し間の待機時間（秒）
        """
        self.api = api_client
        # self.db = db_instance # db_instance は不要なので削除
        self.saver = saver
        self.logger = logger or logging.getLogger(__name__)
        self.max_workers = max_workers
        self.rate_limit_wait = rate_limit_wait

    def _fetch_cup_detail_worker(
        self, cup_id: str
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        単一の開催IDの詳細情報をAPIから取得するワーカー関数 (エラーハンドリング含む)
        """
        thread_id = threading.current_thread().ident
        try:
            self.logger.debug(
                f"スレッド {thread_id}: 開催ID {cup_id} の詳細情報を取得開始"
            )
            # time.sleep(self.rate_limit_wait) # 呼び出し側でまとめて制御するため、ここでは不要かも
            cup_detail_data = self.api.get_cup_detail(cup_id)

            # ▼▼▼ APIレスポンスのより詳細なログ ▼▼▼
            if not cup_detail_data:
                self.logger.warning(
                    f"スレッド {thread_id}: 開催ID {cup_id} の APIレスポンスが空またはNoneです。"
                )
                return cup_id, None

            # APIレスポンスが期待する構造かチェック (例: 'cup' キーの存在)
            # WinticketAPIのget_cup_detailは、成功時 {'cup': {...}, 'schedules': [...], 'races': [...]} のような辞書を返す想定
            if not isinstance(cup_detail_data, dict) or "cup" not in cup_detail_data:
                # レスポンスが期待通りでない場合、型と内容をログに出力
                self.logger.warning(
                    f"スレッド {thread_id}: 開催ID {cup_id} の詳細情報取得失敗。APIレスポンスの形式が不正か、'cup' キーが存在しません。レスポンス型: {type(cup_detail_data)}, レスポンス内容（一部）: {str(cup_detail_data)[:200]}"
                )  # レスポンスが巨大な場合を考慮して一部のみ出力
                return cup_id, None
            # ▲▲▲ APIレスポンスのより詳細なログ ▲▲▲

            self.logger.debug(
                f"スレッド {thread_id}: 開催ID {cup_id} の詳細情報を取得完了。"
            )
            return cup_id, cup_detail_data
        except Exception as e:
            self.logger.error(
                f"スレッド {thread_id}: 開催ID {cup_id} の詳細情報取得中に予期せぬエラー: {e}",
                exc_info=True,
            )
            return cup_id, None

    def update_cups(
        self, cup_ids: List[str], with_parallel: bool = True
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        指定された開催IDリストの詳細情報をAPIから一括取得し、Saverにバッチ保存を依頼する

        Args:
            cup_ids (List[str]): 開催IDのリスト
            with_parallel (bool): 並列処理を使用するかどうか

        Returns:
            tuple: (全体の成功/失敗, 保存結果に関する情報を含む辞書)
                   例: {'succeeded_cups': int, 'failed_cups': int, 'saved_schedules': int, 'saved_races': int}
        """
        if not cup_ids:
            self.logger.warning("更新する開催IDが指定されていません")
            return False, {
                "message": "No cup_ids provided",
                "succeeded_cups": 0,
                "failed_cups": 0,
                "saved_schedules": 0,
                "saved_races": 0,
            }

        thread_id = threading.current_thread().ident
        self.logger.info(
            f"スレッド {thread_id}: Step2 Updater 起動 (対象 Cup ID 数: {len(cup_ids)}, 並列: {with_parallel})"
        )

        all_api_responses: List[Dict[str, Any]] = []
        succeeded_cup_ids = []
        failed_cup_ids = []

        # --- 1. データ一括取得フェーズ ---
        if with_parallel and len(cup_ids) > 1 and self.max_workers > 0:
            self.logger.info(
                f"スレッド {thread_id}: 開催詳細情報の一括取得を並列処理で開始 (最大ワーカー数: {self.max_workers})"
            )
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_cup = {
                    executor.submit(self._fetch_cup_detail_worker, cup_id): cup_id
                    for cup_id in cup_ids
                }
                processed_count = 0
                for future in as_completed(future_to_cup):
                    cup_id_result, detail_data = future.result()
                    processed_count += 1
                    if detail_data:
                        all_api_responses.append(detail_data)
                        succeeded_cup_ids.append(cup_id_result)
                    else:
                        failed_cup_ids.append(cup_id_result)
                    self.logger.debug(
                        f"スレッド {thread_id}: API取得進捗: {processed_count}/{len(cup_ids)} (Cup ID: {cup_id_result}, 結果: {'成功' if detail_data else '失敗'})"
                    )
                    if self.max_workers > 0:
                        time.sleep(self.rate_limit_wait / self.max_workers)
        else:
            self.logger.info(
                f"スレッド {thread_id}: 開催詳細情報の一括取得を順次処理で開始"
            )
            for i, cup_id in enumerate(cup_ids):
                cup_id_result, detail_data = self._fetch_cup_detail_worker(cup_id)
                if detail_data:
                    all_api_responses.append(detail_data)
                    succeeded_cup_ids.append(cup_id_result)
                else:
                    failed_cup_ids.append(cup_id_result)
                self.logger.debug(
                    f"スレッド {thread_id}: API取得進捗: {i+1}/{len(cup_ids)} (Cup ID: {cup_id_result}, 結果: {'成功' if detail_data else '失敗'})"
                )
                time.sleep(self.rate_limit_wait)

        if not all_api_responses:
            self.logger.warning(
                f"スレッド {thread_id}: 有効な開催詳細データが1件も取得できませんでした。"
            )
            return False, {
                "message": "Failed to fetch any cup details",
                "succeeded_cups": len(succeeded_cup_ids),
                "failed_cups": len(failed_cup_ids),
                "saved_schedules": 0,
                "saved_races": 0,
            }

        self.logger.info(
            f"スレッド {thread_id}: 合計 {len(all_api_responses)} 件の開催詳細APIレスポンスを取得完了。整形・保存フェーズへ移行します。"
        )

        # --- 2. データ抽出・整形フェーズ (cup_id ごとにまとめる) ---
        schedules_to_save_for_saver: Dict[str, List[Dict[str, Any]]] = {}
        races_to_save_for_saver: Dict[str, List[Dict[str, Any]]] = {}
        valid_schedule_ids_map: Dict[str, set[str]] = {}

        for api_response in all_api_responses:
            cup_info = api_response.get("cup", {})
            current_cup_id = str(cup_info.get("id"))
            if not current_cup_id:
                self.logger.warning(
                    f"APIレスポンスからcup_idが取得できませんでした。スキップ: {api_response.get('cup', {}).get('name', 'Unknown Cup')}"
                )
                continue

            if current_cup_id not in schedules_to_save_for_saver:
                schedules_to_save_for_saver[current_cup_id] = []
                valid_schedule_ids_map[current_cup_id] = set()
            if current_cup_id not in races_to_save_for_saver:
                races_to_save_for_saver[current_cup_id] = []

            api_schedules_raw = api_response.get("schedules", [])
            for schedule_api_data in api_schedules_raw:
                schedule_id = schedule_api_data.get("id")
                if not schedule_id:
                    self.logger.warning(
                        f"Cup ID {current_cup_id} のスケジュールにIDがありません。スキップ: {schedule_api_data}"
                    )
                    continue

                schedule_id_str = str(schedule_id)
                transformed_schedule_data = {
                    "id": schedule_id_str,
                    "cup_id": current_cup_id,
                    "date": schedule_api_data.get("date"),
                    "day": self._safe_int_convert(schedule_api_data.get("day")),
                    "entriesUnfixed": (
                        1 if schedule_api_data.get("entriesUnfixed") else 0
                    ),
                    "index": self._safe_int_convert(
                        schedule_api_data.get("schedule_index")
                        or schedule_api_data.get("index")
                    ),
                }
                schedules_to_save_for_saver[current_cup_id].append(
                    transformed_schedule_data
                )
                valid_schedule_ids_map[current_cup_id].add(schedule_id_str)

            api_races_raw = api_response.get("races", [])
            for race_api_data in api_races_raw:
                race_id = race_api_data.get("id")
                schedule_id_from_api = race_api_data.get(
                    "scheduleId"
                ) or race_api_data.get("schedule_id")

                if not race_id:
                    self.logger.warning(
                        f"Cup ID {current_cup_id} のレースにIDがありません。スキップ: {race_api_data}"
                    )
                    continue

                final_schedule_id_for_race = None
                if schedule_id_from_api:
                    schedule_id_str = str(schedule_id_from_api)
                    if schedule_id_str in valid_schedule_ids_map.get(
                        current_cup_id, set()
                    ):
                        final_schedule_id_for_race = schedule_id_str
                    else:
                        self.logger.warning(
                            f"Cup ID {current_cup_id}, Race ID {race_id}: schedule_id '{schedule_id_str}' はこのCupの有効なスケジュールリストに存在しません。NULLとして扱います。"
                        )
                else:
                    self.logger.warning(
                        f"Cup ID {current_cup_id}, Race ID {race_id}: APIレスポンスに scheduleId がありません。NULLとして扱います。"
                    )

                transformed_race_data = {
                    "race_id": str(race_id),
                    "schedule_id": final_schedule_id_for_race,
                    "cup_id": current_cup_id,
                    "number": self._safe_int_convert(
                        race_api_data.get("number") or race_api_data.get("raceNumber")
                    ),
                    "class": race_api_data.get("class_name")
                    or race_api_data.get("class"),
                    "race_type": race_api_data.get("race_type_name")
                    or race_api_data.get("raceType"),
                    "start_at": self._to_timestamp(
                        race_api_data.get("startAt")
                        or race_api_data.get("start_time_str")
                    ),
                    "close_at": self._to_timestamp(race_api_data.get("closeAt")),
                    "status": self._safe_int_convert(
                        race_api_data.get("race_status_code")
                        or race_api_data.get("status")
                    ),
                    "cancel": bool(
                        race_api_data.get("cancel") or race_api_data.get("is_canceled")
                    ),
                    "cancel_reason": race_api_data.get("cancelReason"),
                    "weather": race_api_data.get("weather"),
                    "wind_speed": (
                        str(race_api_data.get("windSpeed"))
                        if race_api_data.get("windSpeed") is not None
                        else None
                    ),
                    "race_type3": race_api_data.get("raceType3"),
                    "distance": self._safe_int_convert(race_api_data.get("distance")),
                    "lap": self._safe_int_convert(
                        race_api_data.get("lapCount") or race_api_data.get("lap")
                    ),
                    "entries_number": self._safe_int_convert(
                        race_api_data.get("entriesCount")
                        or race_api_data.get("entriesNumber")
                    ),
                    "is_grade_race": bool(race_api_data.get("isGradeRace")),
                    "has_digest_video": bool(race_api_data.get("hasDigestVideo")),
                    "digest_video": race_api_data.get("digestVideoUrl")
                    or race_api_data.get("digestVideo"),
                    "digest_video_provider": (
                        str(
                            race_api_data.get("digestVideoProviderName")
                            or race_api_data.get("digestVideoProvider")
                        )
                        if (
                            race_api_data.get("digestVideoProviderName")
                            or race_api_data.get("digestVideoProvider")
                        )
                        is not None
                        else None
                    ),
                    "decided_at": self._to_timestamp(race_api_data.get("decidedAt")),
                }
                races_to_save_for_saver[current_cup_id].append(transformed_race_data)

        total_schedules_to_save_count = sum(
            len(s_list) for s_list in schedules_to_save_for_saver.values()
        )
        total_races_to_save_count = sum(
            len(r_list) for r_list in races_to_save_for_saver.values()
        )
        self.logger.info(
            f"スレッド {thread_id}: 整形完了 - スケジュール総数: {total_schedules_to_save_count}件, レース総数: {total_races_to_save_count}件 (Cup ID数: {len(schedules_to_save_for_saver)})"
        )

        # --- 3. 一括保存フェーズ (cup_id ごとにループして保存) ---
        overall_save_success = True
        total_saved_schedules_count = 0  # 保存成功した総数をカウント
        total_saved_races_count = 0  # 保存成功した総数をカウント

        if not total_schedules_to_save_count and not total_races_to_save_count:
            self.logger.warning(
                f"スレッド {thread_id}: 保存対象のスケジュール・レースデータがありません。"
            )
            final_success_status = len(failed_cup_ids) == 0
            return final_success_status, {
                "message": "No data to save after transformation",
                "succeeded_cups": len(succeeded_cup_ids),
                "failed_cups": len(failed_cup_ids),
                "saved_schedules": 0,
                "saved_races": 0,
            }

        self.logger.info(
            f"スレッド {thread_id}: Saver ({type(self.saver).__name__}) を使用してバッチ保存を開始します (cup_id ごと)..."
        )

        for cup_id_to_process in succeeded_cup_ids:  # API取得成功したcup_idのみ処理
            current_schedules_for_saver = schedules_to_save_for_saver.get(
                cup_id_to_process, []
            )
            current_races_for_saver = races_to_save_for_saver.get(cup_id_to_process, [])

            cup_data_save_successful_this_iteration = True  # このcup_idの保存成否フラグ

            if current_schedules_for_saver:
                try:
                    self.logger.debug(
                        f"スレッド {thread_id}: Cup ID {cup_id_to_process} のスケジュール {len(current_schedules_for_saver)}件を保存開始..."
                    )
                    self.saver.save_schedules_batch(
                        current_schedules_for_saver, cup_id_to_process
                    )
                    total_saved_schedules_count += len(
                        current_schedules_for_saver
                    )  # エラーなければ全件成功とみなす
                    self.logger.info(
                        f"スレッド {thread_id}: Cup ID {cup_id_to_process} のスケジュール {len(current_schedules_for_saver)}件の保存呼び出し完了。"
                    )
                except Exception as e_sch:
                    self.logger.error(
                        f"スレッド {thread_id}: Cup ID {cup_id_to_process} のスケジュール保存中にエラー: {e_sch}",
                        exc_info=True,
                    )
                    overall_save_success = False
                    cup_data_save_successful_this_iteration = (
                        False  # このcupのスケジュール保存は失敗
                    )

            # スケジュール保存が成功した場合のみ、またはレースデータが存在する場合にレース保存を試みる
            if cup_data_save_successful_this_iteration and current_races_for_saver:
                try:
                    self.logger.debug(
                        f"スレッド {thread_id}: Cup ID {cup_id_to_process} のレース {len(current_races_for_saver)}件を保存開始..."
                    )
                    race_save_result = self.saver.save_races_batch(
                        current_races_for_saver, cup_id_to_process
                    )

                    if race_save_result and isinstance(race_save_result, dict):
                        saved_count_for_this_batch = race_save_result.get("count", 0)
                        total_saved_races_count += saved_count_for_this_batch
                        if race_save_result.get("error_details"):
                            self.logger.error(
                                f"スレッド {thread_id}: Cup ID {cup_id_to_process} のレース保存中にSaverからエラー返却: {race_save_result['error_details']}"
                            )
                            overall_save_success = False
                            # cup_data_save_successful_this_iteration は既にFalseの可能性もあるが、レースで失敗したら明確にFalse
                            # ただし、一部成功・一部失敗のケースはSaver側の返り値 count で判断
                            if saved_count_for_this_batch < len(
                                current_races_for_saver
                            ):
                                self.logger.warning(
                                    f"Cup ID {cup_id_to_process}: レース保存で一部失敗の可能性 (要求: {len(current_races_for_saver)}, 成功: {saved_count_for_this_batch})"
                                )
                        else:
                            self.logger.info(
                                f"スレッド {thread_id}: Cup ID {cup_id_to_process} のレース {saved_count_for_this_batch}件の保存呼び出し完了。"
                            )
                    else:
                        self.logger.error(
                            f"スレッド {thread_id}: Cup ID {cup_id_to_process} のレース保存の返り値が不正です: {race_save_result}"
                        )
                        overall_save_success = False
                        # この場合も cup_data_save_successful_this_iteration を False にすべきだが、
                        # ループの最後にまとめて警告を出すため、ここでは overall_save_success のみ更新

                except Exception as e_race_outer:
                    self.logger.error(
                        f"スレッド {thread_id}: Cup ID {cup_id_to_process} のレース保存呼び出し中に予期せぬエラー: {e_race_outer}",
                        exc_info=True,
                    )
                    overall_save_success = False

            # このcup_idの処理でエラーがあったかどうかを最終的に判断 (スケジュールorレース)
            # (現状の cup_data_save_successful_this_iteration はスケジュール保存失敗しか見ていないため、レース保存失敗も考慮が必要)
            # よりシンプルには、overall_save_success がこのループ内で一度でもFalseになったら、そのcup_idが問題あった可能性があると考える
            # ここでは詳細なエラー追跡より、全体の成否で判断

        if not overall_save_success:
            self.logger.error(
                f"スレッド {thread_id}: 1つ以上のデータ保存処理でエラーが発生しました。詳細は各Cup IDのログを確認してください。"
            )

        final_message = f"Step2 処理完了. API成功Cup数: {len(succeeded_cup_ids)}, API失敗Cup数: {len(failed_cup_ids)}, 保存スケジュール数: {total_saved_schedules_count}, 保存レース数: {total_saved_races_count}"
        self.logger.info(f"スレッド {thread_id}: {final_message}")

        final_overall_success = (len(failed_cup_ids) == 0) and overall_save_success

        return final_overall_success, {
            "message": final_message,
            "succeeded_cups": len(succeeded_cup_ids),
            "failed_cups": len(failed_cup_ids),
            "saved_schedules": total_saved_schedules_count,  # 保存成功した総数
            "saved_races": total_saved_races_count,  # 保存成功した総数
            "save_process_errors": not overall_save_success,
        }

    # --- ここからヘルパーメソッドを追加 ---
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

    def _to_timestamp(self, date_input: Optional[Any]) -> Optional[int]:
        """
        日付/日時文字列 (YYYY-MM-DD HH:MM:SS や ISOフォーマット) またはUnixタイムスタンプ数値をUnixタイムスタンプ (秒) に変換する。
        変換できない場合はNoneを返す。
        '0000-00-00 00:00:00' のような無効な日付もNoneとして扱う。
        """
        if date_input is None:
            return None

        # もし入力が既に数値 (int or float) なら、それをタイムスタンプとして扱う
        if isinstance(date_input, (int, float)):
            try:
                return int(date_input)
            except (ValueError, TypeError):
                self.logger.debug(
                    f"タイムスタンプ変換失敗 (数値だがintに変換できない): {date_input}"
                )
                return None

        # 文字列の場合の処理
        if not isinstance(date_input, str) or date_input == "0000-00-00 00:00:00":
            # self.logger.debug(f"タイムスタンプ変換入力が文字列でないか、無効な日付文字列です: {date_input}") # 必要ならログ出す
            return None

        date_str = str(date_input)  # ここで文字列であることを確定させる

        try:
            # 一般的なISO 8601形式 (例: '2023-10-26T10:00:00Z', '2023-10-26T19:00:00+09:00')
            if "T" in date_str:
                dt_obj = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return int(dt_obj.timestamp())

            # YYYY-MM-DD HH:MM:SS 形式
            dt_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            return int(dt_obj.timestamp())
        except ValueError:
            try:
                # YYYY-MM-DD 形式 (時刻なし)
                dt_obj = datetime.strptime(date_str, "%Y-%m-%d")
                return int(dt_obj.timestamp())
            except ValueError:
                self.logger.debug(
                    f"タイムスタンプ変換失敗 (サポート外フォーマットか無効な値): {date_str}"
                )
                return None
        except Exception as e:  # その他の予期せぬエラー
            self.logger.error(
                f"タイムスタンプ変換中に予期せぬエラー: {date_str} - {e}", exc_info=True
            )
            return None


# 旧メソッド update_cups_from_db は完全に削除
