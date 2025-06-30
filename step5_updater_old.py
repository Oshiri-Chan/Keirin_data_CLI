"""
ステップ5: 結果情報の取得・更新クラス
"""

import concurrent.futures
import json  # ★ json ライブラリをインポート
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Step5DataExtractor をインポート
from database.extractors.extract_data_for_step5 import Step5DataExtractor

# API Client (Yenjoy) と Saver (Yenjoy) をインポート (パスは環境に合わせて調整)
# YenjoyAPIClient は HTML を取得するメソッドを持つと仮定
# from api.yenjoy_api import YenjoyAPIClient # 仮のインポートパス
# from services.yenjoy_data_saver import YenjoyDataSaver # 仮のインポートパス -> 不要に
# Step5Saver をインポート
from services.savers.step5_saver import Step5Saver

# HTML解析ライブラリ (別途インストール・実装が必要)
# from bs4 import BeautifulSoup # 例

# --- 設定値 ---
YENJOY_BASE_URL = "https://www.yen-joy.net/"


class Step5Updater:
    """
    ステップ5: 結果情報を取得・更新するクラス
    """

    def __init__(
        self,
        yenjoy_api,
        db_instance,
        saver,
        logger=None,
        max_workers=3,
        rate_limit_wait=1.0,
    ):
        """
        初期化

        Args:
            yenjoy_api: Yenjoy APIクライアントインスタンス (HTML取得等に使う？現状未使用)
            db_instance: データベースインスタンス
            saver: YenjoyDataSaver インスタンス -> ★ Step5Saver インスタンスに変更
            logger: ロガーオブジェクト（省略時は標準ロガーを使用）
            max_workers (int): 並列処理の最大ワーカー数
            rate_limit_wait (float): API呼び出し間の待機時間（秒）
        """
        self.yenjoy_api = (
            yenjoy_api  # 現状 HTML 取得は requests.Session を使うので不要かも
        )
        # self.yenjoy_saver = saver # YenjoyDataSaver は使わない
        self.db = db_instance
        self.logger = logger or logging.getLogger(__name__)
        self.max_workers = max_workers
        self.rate_limit_wait = rate_limit_wait

        # Step5Saver を直接インスタンス化
        self.step5_saver = Step5Saver(self.db, self.logger)

        # 処理状態の保持用
        self._processing_races = set()
        self._lock = threading.RLock()

        self.step5_extractor = Step5DataExtractor(db_instance, self.logger)
        # HTTPセッションを初期化 (Updater内で管理)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
            }
        )

    def update_race_result(self, race_id, use_transaction=True):
        """
        指定されたレースIDの結果情報を更新 (Winticket API 呼び出し削除版)

        Args:
            race_id (str): レースID
            use_transaction (bool): トランザクションを使用するかどうか

        Returns:
            bool: 成功したかどうか
        """
        thread_id = threading.current_thread().ident

        try:
            # 既に処理中のレースをスキップ
            with self._lock:
                if race_id in self._processing_races:
                    self.logger.info(
                        f"スレッド {thread_id}: race_id {race_id} は別のスレッドで処理中のためスキップします"
                    )
                    return False
                self._processing_races.add(race_id)

            self.logger.info(
                f"スレッド {thread_id}: race_id {race_id} の結果情報を取得します (YenJoy データのみ)"
            )

            # トランザクション開始
            if use_transaction:
                try:
                    self.db.begin_transaction()
                except AttributeError:
                    self.logger.warning(
                        f"スレッド {thread_id}: トランザクション開始機能が利用できません。トランザクションなしで処理を続行します。"
                    )
                    use_transaction = False

            # えんじょいAPIからラップとポジションデータを取得 (ここから開始)
            self.logger.info(
                f"レースID {race_id} のラップとポジションデータをえんじょいから取得します"
            )

            # 車番-選手IDのマッピングを取得
            # TODO: self.db.read_from_json_export は非推奨かも。ExtractorかRepository経由で取得すべき
            entry_query = (
                f"SELECT bracket_number, player_id FROM entries WHERE race_id = ?"
            )
            entry_data = self.db.execute_query(
                entry_query, params=(race_id,), fetch_all=True
            )

            if not entry_data:
                self.logger.error(f"レースID {race_id} の出走情報が見つかりません")
                with self._lock:
                    self._processing_races.discard(race_id)
                # Winticketデータがないので、エラー時は常にロールバック
                if use_transaction:
                    try:
                        self.db.rollback()
                    except AttributeError:
                        self.logger.warning(
                            f"スレッド {thread_id}: ロールバック機能が利用できません。"
                        )
                return False

            # 車番と選手IDのマッピングを作成
            bracket_to_player = {
                entry["bracket_number"]: entry["player_id"] for entry in entry_data
            }

            yenjoy_success = False  # 初期化
            try:
                # えんじょいAPIからラップデータを取得 (YenjoySaver経由)
                lap_data = self.yenjoy_saver.get_race_lap_data(
                    race_id, bracket_to_player
                )

                # えんじょいAPIからポジションデータを取得 (YenjoySaver経由)
                position_data = self.yenjoy_saver.get_race_position_data(
                    race_id, bracket_to_player
                )

                # えんじょいの結果データを結合
                yenjoy_result_data = {
                    "lap_data": lap_data,
                    "position_data": position_data,
                }

                # 検車場レポートを取得 (YenjoySaver経由)
                inspection_report = self.yenjoy_saver.get_inspection_report(race_id)
                if inspection_report:
                    self.logger.info(
                        f"レースID {race_id} の検車場レポートを取得しました（選手数: {len(inspection_report)}）"
                    )
                    yenjoy_result_data["inspection_report"] = inspection_report
                else:
                    self.logger.warning(
                        f"レースID {race_id} の検車場レポートを取得できませんでした"
                    )

                # えんじょいの結果データを保存 (Step5Saverを使用)
                # Saver インスタンスをここで作成
                from services.savers.step5_saver import Step5Saver

                step5_saver = Step5Saver(self.db, self.logger)
                yenjoy_success = step5_saver.save_race_result(
                    race_id, yenjoy_result_data
                )  # Winticketデータなし版

                if yenjoy_success:
                    self.logger.info(
                        f"レースID {race_id} のえんじょいデータを保存しました"
                    )
                else:
                    self.logger.warning(
                        f"レースID {race_id} のえんじょいデータの保存に失敗しました"
                    )

            except Exception as e:
                self.logger.error(
                    f"レースID {race_id} のえんじょいデータ取得または保存中にエラーが発生しました: {str(e)}",
                    exc_info=True,
                )
                yenjoy_success = False  # エラー時は失敗とする

            # 処理中リストから削除
            with self._lock:
                self._processing_races.discard(race_id)

            # 最終的な成功判定 (YenJoyデータの保存成否のみ)
            success = yenjoy_success

            if success:
                self.logger.info(f"レースID {race_id} の結果情報(YenJoy)を更新しました")
                # コミット
                if use_transaction:
                    try:
                        self.db.commit()
                    except AttributeError:
                        self.logger.warning(
                            f"スレッド {thread_id}: コミット機能が利用できません。"
                        )
            else:
                self.logger.warning(
                    f"レースID {race_id} の結果情報(YenJoy)の更新に失敗しました"
                )
                # ロールバック
                if use_transaction:
                    try:
                        self.db.rollback()
                    except AttributeError:
                        self.logger.warning(
                            f"スレッド {thread_id}: ロールバック機能が利用できません。"
                        )

            return success

        except Exception as e:
            self.logger.error(
                f"レースID {race_id} の結果情報の更新中に予期せぬエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            # 処理中リストから削除
            with self._lock:
                self._processing_races.discard(race_id)
            # ロールバック
            if use_transaction:
                try:
                    self.db.rollback()
                except AttributeError:
                    self.logger.warning(
                        f"スレッド {thread_id}: ロールバック機能が利用できません。"
                    )
            return False

    def update_races_results(self, race_ids, with_parallel=True, use_transaction=True):
        """
        指定されたレースIDリストの結果情報を更新（並列処理可能）

        Args:
            race_ids (list): レースIDのリスト
            with_parallel (bool): 並列処理を使用するかどうか
            use_transaction (bool): トランザクションを使用するかどうか

        Returns:
            tuple: (成功したかどうか, 成功したレースIDのリスト)
        """
        if not race_ids:
            self.logger.warning("更新するレースIDが指定されていません")
            return False, []

        # 結果格納用変数
        all_success = True
        success_race_ids = []

        try:
            # 並列処理を使用する場合
            if with_parallel and len(race_ids) > 1 and self.max_workers > 1:
                self.logger.info(
                    f"結果情報の取得を並列処理で開始します（レース数: {len(race_ids)}件）"
                )

                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    # 各レースIDに対して処理をスケジュール
                    future_to_race = {
                        executor.submit(
                            self.update_race_result, race_id, use_transaction
                        ): race_id
                        for race_id in race_ids
                    }

                    # 完了した処理を順次処理
                    for future in as_completed(future_to_race):
                        race_id = future_to_race[future]

                        try:
                            success = future.result()

                            if not success:
                                all_success = False
                            else:
                                success_race_ids.append(race_id)

                            # API呼び出しの間隔を空ける
                            # time.sleep(self.rate_limit_wait) # 削除

                        except Exception as e:
                            self.logger.error(
                                f"レースID {race_id} の処理中にエラーが発生しました: {str(e)}",
                                exc_info=True,
                            )
                            all_success = False

            # 並列処理を使用しない場合
            else:
                self.logger.info(
                    f"結果情報の取得を順次処理で開始します（レース数: {len(race_ids)}件）"
                )

                for race_id in race_ids:
                    success = self.update_race_result(race_id, use_transaction)

                    if not success:
                        all_success = False
                    else:
                        success_race_ids.append(race_id)

                    # API呼び出しの間隔を空ける
                    # time.sleep(self.rate_limit_wait) # 削除

            # 結果の集計
            success_count = len(success_race_ids)
            total_count = len(race_ids)
            self.logger.info(
                f"結果情報の更新が完了しました（成功: {success_count}/{total_count}件）"
            )

            return all_success, success_race_ids

        except Exception as e:
            self.logger.error(
                f"結果情報の更新中にエラーが発生しました: {str(e)}", exc_info=True
            )
            return False, success_race_ids

    def update_races_results_from_db(
        self,
        start_date,
        end_date,
        with_parallel=True,
        status_filter="RESULT",
        use_transaction=True,
    ):
        """
        データベースから指定期間のレースIDを取得して結果情報を更新

        Args:
            start_date (str): 開始日（YYYY-MM-DD形式）
            end_date (str): 終了日（YYYY-MM-DD形式）
            with_parallel (bool): 並列処理を使用するかどうか
            status_filter (str): ステータスフィルター（例：'RESULT'）
            use_transaction (bool): トランザクションを使用するかどうか

        Returns:
            tuple: (成功したかどうか, 成功したレースIDのリスト)
        """
        try:
            # データベースから指定期間のレースIDを取得
            query = f"""
                SELECT r.race_id 
                FROM races r
                JOIN schedules s ON r.schedule_id = s.schedule_id
                WHERE s.date >= '{start_date}' AND s.date <= '{end_date}'
            """

            if status_filter:
                query += f" AND r.status = '{status_filter}'"

            query += " ORDER BY s.date, r.number"

            race_data = self.db.read_from_json_export("races", query)

            if not race_data:
                self.logger.warning(
                    f"期間 {start_date} 〜 {end_date} のレース情報がデータベースに存在しません"
                )
                return False, []

            # レースIDのリストを抽出
            race_ids = [race["race_id"] for race in race_data]
            self.logger.info(
                f"期間 {start_date} 〜 {end_date} のレースID数: {len(race_ids)}件"
            )

            # 結果情報を更新
            return self.update_races_results(race_ids, with_parallel, use_transaction)

        except Exception as e:
            self.logger.error(
                f"期間 {start_date} 〜 {end_date} の結果情報の更新中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False, []

    def update_cup_results(
        self, cup_id, with_parallel=True, status_filter="RESULT", use_transaction=True
    ):
        """
        指定された開催IDの結果情報を更新

        Args:
            cup_id (str): 開催ID
            with_parallel (bool): 並列処理を使用するかどうか
            status_filter (str): ステータスフィルター（例：'RESULT'）
            use_transaction (bool): トランザクションを使用するかどうか

        Returns:
            tuple: (成功したかどうか, 成功したレースIDのリスト)
        """
        try:
            # データベースから指定開催のレースIDを取得
            query = f"""
                SELECT race_id 
                FROM races 
                WHERE cup_id = '{cup_id}'
            """

            if status_filter:
                query += f" AND status = '{status_filter}'"

            query += " ORDER BY number"

            race_data = self.db.read_from_json_export("races", query)

            if not race_data:
                self.logger.warning(
                    f"開催ID {cup_id} のレース情報がデータベースに存在しません"
                )
                return False, []

            # レースIDのリストを抽出
            race_ids = [race["race_id"] for race in race_data]
            self.logger.info(f"開催ID {cup_id} のレースID数: {len(race_ids)}件")

            # 結果情報を更新
            return self.update_races_results(race_ids, with_parallel, use_transaction)

        except Exception as e:
            self.logger.error(
                f"開催ID {cup_id} の結果情報の更新中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            return False, []

    def update_race_result_from_json(self, race_id, data, use_transaction=True):
        """JSONデータからレース結果情報を更新する

        Args:
            race_id (str): レースID
            data (dict): 結果データ
            use_transaction (bool, optional): トランザクションを使用するかどうか. Defaults to True.

        Returns:
            bool: 更新成功時はTrue、失敗時はFalse
        """
        thread_id = threading.current_thread().ident
        self.logger.debug(
            f"スレッド {thread_id}: レースID {race_id} の結果を更新します"
        )

        if not data:
            self.logger.warning(f"レースID {race_id} のデータが空です")
            return False

        # 重要なデータの存在チェック
        if "winticket_data" not in data:
            self.logger.warning(f"レースID {race_id} のWinticketデータがありません")
            return False

        if "result" not in data.get("winticket_data", {}):
            self.logger.warning(f"レースID {race_id} の結果データがありません")
            return False

        # DBからレースを取得
        race = self.db.get_race_by_id(race_id)
        if not race:
            self.logger.warning(f"レースID {race_id} がデータベースに存在しません")
            return False

        # レースステータスのチェック
        if race.race_status == "RESULT":
            self.logger.info(
                f"レースID {race_id} は既に結果が登録済みです。再更新します。"
            )
        elif race.race_status != "ODDS":
            self.logger.warning(
                f"レースID {race_id} のステータスが不適切です: {race.race_status}"
            )

        # トランザクション開始
        if use_transaction:
            transaction = self.db.session.begin_nested()

        try:
            results = {}

            # 基本情報の取得
            winticket_data = data.get("winticket_data", {})
            result_data = winticket_data.get("result", {})
            payout_data = winticket_data.get("payouts", {})

            # メモリ効率化のためのローカル変数
            race_date = race.race_date
            race_number = race.race_number
            venue_name = race.venue.name if race.venue else "不明"

            self.logger.debug(
                f"レース日: {race_date}, 会場: {venue_name}, レース番号: {race_number} の結果を更新します"
            )

            # 1. レースステータスを更新
            race.race_status = "RESULT"
            self.db.session.add(race)
            self.logger.debug(
                f"レースID {race_id} のステータスを 'RESULT' に更新しました"
            )

            # 2. 結果データの保存
            try:
                save_result = self._save_race_result(race, result_data)
                results["save_result"] = save_result
                if not save_result:
                    self.logger.error(
                        f"レースID {race_id} の結果データの保存に失敗しました"
                    )
            except Exception as e:
                self.logger.error(
                    f"レースID {race_id} の結果データの保存中にエラー: {str(e)}",
                    exc_info=True,
                )
                results["save_result"] = False

            # 3. 払戻データの保存
            try:
                if payout_data:
                    save_payout = self._save_race_payout(race, payout_data)
                    results["save_payout"] = save_payout
                    if not save_payout:
                        self.logger.error(
                            f"レースID {race_id} の払戻データの保存に失敗しました"
                        )
                else:
                    self.logger.warning(f"レースID {race_id} の払戻データがありません")
                    results["save_payout"] = False
            except Exception as e:
                self.logger.error(
                    f"レースID {race_id} の払戻データの保存中にエラー: {str(e)}",
                    exc_info=True,
                )
                results["save_payout"] = False

            # 4. ラップタイムデータの保存（オプション）
            try:
                if "lap_list" in winticket_data:
                    save_lap = self._save_race_lap(
                        race, winticket_data.get("lap_list", [])
                    )
                    results["save_lap"] = save_lap
                    if not save_lap:
                        self.logger.warning(
                            f"レースID {race_id} のラップタイムデータの保存に失敗しました"
                        )
                else:
                    self.logger.debug(
                        f"レースID {race_id} のラップタイムデータはありません"
                    )
                    results["save_lap"] = None
            except Exception as e:
                self.logger.warning(
                    f"レースID {race_id} のラップタイムデータの保存中にエラー: {str(e)}"
                )
                results["save_lap"] = False

            # 5. ポジションデータの保存（オプション）
            try:
                if "position_list" in winticket_data:
                    save_position = self._save_race_position(
                        race, winticket_data.get("position_list", [])
                    )
                    results["save_position"] = save_position
                    if not save_position:
                        self.logger.warning(
                            f"レースID {race_id} のポジションデータの保存に失敗しました"
                        )
                else:
                    self.logger.debug(
                        f"レースID {race_id} のポジションデータはありません"
                    )
                    results["save_position"] = None
            except Exception as e:
                self.logger.warning(
                    f"レースID {race_id} のポジションデータの保存中にエラー: {str(e)}"
                )
                results["save_position"] = False

            # コミット
            if use_transaction:
                transaction.commit()
                self.db.session.commit()

            # 必須項目だけ成功していれば成功とみなす
            success = results.get("save_result", False) and results.get(
                "save_payout", False
            )
            self.logger.info(
                f"レースID {race_id} の結果更新: {'成功' if success else '失敗'}"
            )

            return success

        except Exception as e:
            self.logger.error(
                f"レースID {race_id} の結果更新中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            if use_transaction:
                transaction.rollback()
            return False

    def update_races_results_from_json(self, data, parallel=True, max_workers=None):
        """複数のレース結果をJSONデータから更新する

        Args:
            data (dict): レース結果データのマップ。キーはレースID、値は結果データ
            parallel (bool, optional): 並列処理を行うかどうか。Defaults to True.
            max_workers (int, optional): 並列処理時の最大ワーカー数。Noneの場合はCPU数×5。

        Returns:
            dict: 更新結果のマップ。キーはレースID、値は更新成功/失敗
        """
        if not data or not isinstance(data, dict):
            self.logger.warning("更新対象のレース結果データが空または無効な形式です")
            return {}

        race_ids = list(data.keys())
        race_count = len(race_ids)

        self.logger.info(f"レース結果更新: {race_count}件のレース結果を処理します")
        start_time = time.time()

        # 1件だけ、または並列処理が無効な場合は逐次処理
        if race_count == 1 or not parallel:
            results = {}
            for race_id, race_data in data.items():
                self.logger.debug(f"レースID {race_id} の結果を更新します（逐次処理）")
                success = self.update_race_result_from_json(race_id, race_data)
                results[race_id] = success

            elapsed = time.time() - start_time
            success_count = sum(1 for v in results.values() if v)
            self.logger.info(
                f"レース結果更新完了: {success_count}/{race_count}件成功 (所要時間: {elapsed:.2f}秒)"
            )
            return results

        # 並列処理の場合
        results = {}
        total_processed = 0

        # デフォルトのワーカー数を設定（CPU数×5、ただし最大20）
        if max_workers is None:
            max_workers = min(os.cpu_count() * 5, 20)

        self.logger.info(
            f"並列処理でレース結果を更新します (ワーカー数: {max_workers})"
        )

        # ThreadPoolExecutorを使用して並列処理
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 進捗表示用のロック
            progress_lock = threading.Lock()

            # 個別レース処理関数
            def _process_race_result(race_id, race_data):
                thread_id = threading.current_thread().ident
                try:
                    # 各ワーカーの開始をログ出力
                    self.logger.debug(
                        f"スレッド {thread_id}: レースID {race_id} の処理を開始します"
                    )

                    # レース結果の更新
                    success = self.update_race_result_from_json(race_id, race_data)

                    # 進捗を更新
                    nonlocal total_processed
                    with progress_lock:
                        total_processed += 1
                        if total_processed % 10 == 0 or total_processed == race_count:
                            progress = (total_processed / race_count) * 100
                            self.logger.info(
                                f"進捗: {total_processed}/{race_count} ({progress:.1f}%)"
                            )

                    return race_id, success
                except Exception as e:
                    self.logger.error(
                        f"スレッド {thread_id}: レースID {race_id} の処理中に例外が発生しました: {str(e)}",
                        exc_info=True,
                    )
                    return race_id, False

            # 各レースの処理をスケジュール
            future_to_race = {
                executor.submit(_process_race_result, race_id, race_data): race_id
                for race_id, race_data in data.items()
            }

            # 結果を収集
            for future in concurrent.futures.as_completed(future_to_race):
                race_id, success = future.result()
                results[race_id] = success

        # 集計と結果レポート
        elapsed = time.time() - start_time
        success_count = sum(1 for v in results.values() if v)
        success_rate = (success_count / race_count) * 100 if race_count > 0 else 0

        self.logger.info(
            f"レース結果更新完了: {success_count}/{race_count}件成功 ({success_rate:.1f}%) [所要時間: {elapsed:.2f}秒]"
        )

        return results

    def update_results(self, start_date, end_date, initial_data=None, callback=None):
        """
        指定期間のレース結果を更新

        Args:
            start_date (str): 開始日（YYYY-MM-DD形式）
            end_date (str): 終了日（YYYY-MM-DD形式）
            initial_data (dict, optional): 初期データ
            callback (callable, optional): 進捗コールバック関数

        Returns:
            dict: 更新結果
        """
        thread_id = threading.current_thread().ident
        self.logger.info(
            f"スレッド {thread_id}: 期間 {start_date} から {end_date} のレース結果情報を更新します"
        )

        results = {"success": False, "message": "", "count": 0, "processed_races": []}

        try:
            # 進捗状況の追跡用変数
            total_count = 0
            processed_count = 0

            # 初期データからレース結果情報を処理
            initial_data_processed = False
            if initial_data and isinstance(initial_data, dict):
                self.logger.info(
                    f"スレッド {thread_id}: 初期データを使用してレース結果情報を処理します"
                )

                # 結果データがあれば処理
                race_results = initial_data.get("race_results", [])
                if (
                    race_results
                    and isinstance(race_results, list)
                    and len(race_results) > 0
                ):
                    self.logger.info(
                        f"スレッド {thread_id}: 初期データに {len(race_results)} 件のレース結果情報があります"
                    )
                    initial_data_processed = True

                    # レースID別に結果データを整理
                    race_data_map = {}
                    for result in race_results:
                        race_id = result.get("race_id")
                        if not race_id:
                            continue

                        if race_id not in race_data_map:
                            race_data_map[race_id] = {
                                "winticket_data": {"result": [], "payouts": []},
                                "yenjoy_data": {"lap_data": [], "position_data": []},
                            }

                        # winticket結果データを追加
                        race_data_map[race_id]["winticket_data"]["result"].append(
                            result
                        )

                    # 払戻情報を追加
                    payouts = initial_data.get("payouts", [])
                    if payouts and isinstance(payouts, list) and len(payouts) > 0:
                        self.logger.info(
                            f"スレッド {thread_id}: 初期データに {len(payouts)} 件の払戻情報があります"
                        )
                        for payout in payouts:
                            race_id = payout.get("race_id")
                            if race_id and race_id in race_data_map:
                                race_data_map[race_id]["winticket_data"][
                                    "payouts"
                                ].append(payout)

                    # ラップタイムデータを追加
                    lap_times = initial_data.get("lap_times", [])
                    if lap_times and isinstance(lap_times, list) and len(lap_times) > 0:
                        self.logger.info(
                            f"スレッド {thread_id}: 初期データに {len(lap_times)} 件のラップタイム情報があります"
                        )
                        for lap in lap_times:
                            race_id = lap.get("race_id")
                            if race_id and race_id in race_data_map:
                                race_data_map[race_id]["yenjoy_data"][
                                    "lap_data"
                                ].append(lap)

                    # ポジションデータを追加
                    positions = initial_data.get("positions", [])
                    if positions and isinstance(positions, list) and len(positions) > 0:
                        self.logger.info(
                            f"スレッド {thread_id}: 初期データに {len(positions)} 件のポジション情報があります"
                        )
                        for position in positions:
                            race_id = position.get("race_id")
                            if race_id and race_id in race_data_map:
                                race_data_map[race_id]["yenjoy_data"][
                                    "position_data"
                                ].append(position)

                    # 処理するレース数を追加
                    race_count = len(race_data_map)
                    if race_count > 0:
                        total_count += race_count

                        # 初期進捗コールバック
                        if callback:
                            callback(
                                "step5",
                                0,
                                total_count,
                                "レース結果情報の更新を開始します",
                            )

                        self.logger.info(
                            f"初期データから {race_count} 件のレース結果情報を処理します"
                        )

                        # レースごとに処理
                        for race_id, data in race_data_map.items():
                            # 進捗カウンターを更新
                            processed_count += 1

                            # 進捗コールバック
                            if callback:
                                progress = (
                                    int((processed_count / total_count) * 100)
                                    if total_count > 0
                                    else 0
                                )
                                callback(
                                    "step5",
                                    processed_count,
                                    total_count,
                                    f"レース {race_id} の結果情報を処理中 ({processed_count}/{total_count})",
                                )

                            # JSONデータから結果を更新
                            success = self.update_race_result_from_json(
                                race_id, data, True
                            )
                            if success:
                                results["processed_races"].append(race_id)
                    else:
                        self.logger.info(
                            f"スレッド {thread_id}: 初期データに有効なレース結果情報が見つかりません"
                        )
                else:
                    self.logger.info(
                        f"スレッド {thread_id}: 初期データにレース結果情報が見つかりません"
                    )
            else:
                if initial_data is None:
                    self.logger.info(
                        f"スレッド {thread_id}: 初期データが指定されていないため、APIからレース結果情報を取得します"
                    )
                elif not isinstance(initial_data, dict):
                    self.logger.warning(
                        f"スレッド {thread_id}: 初期データが辞書型ではありません。型: {type(initial_data)}"
                    )

            # データベースからレース情報を取得して更新
            query = f"""
                SELECT r.race_id 
                FROM races r
                JOIN schedules s ON r.schedule_id = s.schedule_id
                WHERE s.date BETWEEN '{start_date}' AND '{end_date}'
                AND r.status = 'RESULT'
                AND r.race_id NOT IN (
                    SELECT DISTINCT race_id FROM results
                )
                ORDER BY s.date, r.number
            """

            race_data = self.db.read_from_json_export("races", query)

            if race_data:
                missing_race_ids = [race["race_id"] for race in race_data]
                missing_count = len(missing_race_ids)
                self.logger.info(
                    f"スレッド {thread_id}: {missing_count} 件の結果情報未取得のレースを検出しました"
                )

                # 結果が未取得のレースを更新
                if missing_race_ids:
                    # APIから取得する必要のあるレースIDのリスト
                    api_race_ids = [
                        race_id
                        for race_id in missing_race_ids
                        if race_id not in results["processed_races"]
                    ]
                    api_count = len(api_race_ids)

                    if api_count > 0:
                        # 全体件数を更新
                        total_count += api_count

                        # 進捗コールバックを更新
                        if callback:
                            callback(
                                "step5",
                                processed_count,
                                total_count,
                                f"API経由で{api_count}件のレース結果情報を更新します",
                            )

                        self.logger.info(
                            f"スレッド {thread_id}: API経由で{api_count}件のレース結果情報を更新します"
                        )

                        # APIからレース結果を取得
                        for i, race_id in enumerate(api_race_ids):
                            # 進捗カウンターを更新
                            processed_count += 1

                            # 進捗コールバック
                            if callback:
                                progress = (
                                    int((processed_count / total_count) * 100)
                                    if total_count > 0
                                    else 0
                                )
                                callback(
                                    "step5",
                                    processed_count,
                                    total_count,
                                    f"レース {race_id} の結果情報を取得中 ({processed_count}/{total_count})",
                                )

                            # APIから結果を取得して更新
                            success = self.update_race_result(race_id, True)
                            if success:
                                results["processed_races"].append(race_id)

                            # ログを定期的に出力
                            if (i + 1) % 10 == 0 or i == api_count - 1:
                                self.logger.info(
                                    f"スレッド {thread_id}: {i + 1}/{api_count} 件のレース結果情報を処理しました"
                                )
                    else:
                        self.logger.info(
                            f"スレッド {thread_id}: 全てのレース結果は既に初期データから処理済みです"
                        )
            else:
                self.logger.info(
                    f"スレッド {thread_id}: 期間 {start_date} から {end_date} の未処理のレース結果情報は見つかりませんでした"
                )

            # 結果を設定
            success_count = len(results["processed_races"])
            results["count"] = success_count

            # 最終進捗コールバック
            if callback:
                callback(
                    "step5",
                    total_count,
                    total_count,
                    f"レース結果情報の更新が完了しました（成功: {success_count}/{total_count}）",
                )

            if success_count > 0:
                results["success"] = True
                results["message"] = f"{success_count} 件のレース結果情報を更新しました"
            elif total_count == 0:
                self.logger.info(
                    f"スレッド {thread_id}: 期間 {start_date} から {end_date} のレース結果情報は全て取得済みです"
                )
                results["success"] = True
                results["message"] = "レース結果情報は全て取得済みです"
            else:
                results["message"] = "レース結果情報の更新に失敗しました"

            return results

        except Exception as e:
            self.logger.error(
                f"スレッド {thread_id}: レース結果情報の更新中にエラーが発生しました: {str(e)}",
                exc_info=True,
            )
            results["message"] = f"エラー: {str(e)}"
            return results

    def _fetch_html(self, url: str) -> Optional[str]:
        """指定されたURLからHTMLを取得"""
        thread_id = threading.get_ident()
        # ★ レートリミット: リクエスト前に待機
        self.logger.debug(
            f"[Thread-{thread_id}] Waiting for {self.rate_limit_wait} seconds before fetching..."
        )
        time.sleep(self.rate_limit_wait)

        self.logger.info(f"[Thread-{thread_id}] HTMLを取得中: {url}")  # URL をログ出力
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()  # HTTPエラーチェック
            # エンコーディングを推定または指定 (yen-joyはEUC-JPの可能性)
            # response.encoding = 'EUC-JP' # または response.apparent_encoding
            response.encoding = "UTF-8"  # ★ UTF-8 に変更
            html_content = response.text
            self.logger.debug(
                f"HTML取得成功: {url} (サイズ: {len(html_content)} bytes)"
            )
            return html_content

        except requests.exceptions.Timeout:
            self.logger.error(f"HTML取得タイムアウト: {url}")
            return None
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTPエラー ({e.response.status_code}): {e}. URL: {url}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTML取得エラー: {e}. URL: {url}", exc_info=True)
            return None
        except Exception as e:
            self.logger.error(
                f"HTML取得中に予期せぬエラー: {e}. URL: {url}", exc_info=True
            )
            return None

    def _build_yenjoy_url(self, race_url_info: Dict) -> Optional[str]:
        """
        Extractor からの情報に基づき、yen-joy.net の結果詳細ページの URL を構築する。
        URL format: /kaisai/race/result/detail/{start_date_ym}/{venue_id}/{start_date_ymd}/{race_date_ymd}/{race_number}
        """
        try:
            start_date_ym = race_url_info.get("start_date_ym")
            venue_id = race_url_info.get("venue_id")
            start_date_ymd = race_url_info.get("start_date_ymd")
            race_date_ymd = race_url_info.get("race_date_ymd")
            race_number = race_url_info.get("race_number")

            if not all(
                [
                    start_date_ym,
                    venue_id,
                    start_date_ymd,
                    race_date_ymd,
                    race_number is not None,
                ]
            ):
                self.logger.warning(
                    f"URL構築に必要な情報が不足しています: {race_url_info}"
                )
                return None

            # venue_id が期待される形式か確認 (例: '01')
            # venue_id = str(venue_id).zfill(2) # 必要であればゼロパディング

            path = f"/kaisai/race/result/detail/{start_date_ym}/{venue_id}/{start_date_ymd}/{race_date_ymd}/{race_number}"
            return urljoin(YENJOY_BASE_URL, path)
        except Exception as e:
            self.logger.error(
                f"Yen-joy URL構築中にエラー: {e}. 情報: {race_url_info}", exc_info=True
            )
            return None

    def _fetch_and_parse_result_worker(self, race_url_info: Dict) -> Optional[Dict]:
        """指定されたレース情報のURLを構築し、HTMLを取得して解析する（ワーカースレッド用）"""
        race_id = race_url_info.get("race_id")
        thread_id = threading.get_ident()

        # URL構築
        # ★★★ 詳細ログ追加 ★★★
        self.logger.debug(
            f"[Thread-{thread_id}][Step5 Worker PRE-URL] Race ID {race_id}: URL構築開始. Info: {race_url_info}"
        )
        url = self._build_yenjoy_url(race_url_info)
        if not url:
            self.logger.warning(
                f"[Thread-{thread_id}][Step5 Worker URL-FAIL] Race ID {race_id}: 結果ページのURL構築に失敗しました。スキップします。 Info: {race_url_info}"
            )
            return None  # URL構築失敗はNoneを返す (is_emptyではない)

        self.logger.info(
            f"[Thread-{thread_id}][Step5 Worker URL-OK] Race ID {race_id}: 結果ページURL: {url}"
        )

        # HTML取得
        self.logger.debug(
            f"[Thread-{thread_id}][Step5 Worker PRE-FETCH] Race ID {race_id}: HTML取得開始. URL: {url}"
        )
        html_content = self._fetch_html(url)
        self.logger.debug(
            f"[Thread-{thread_id}][Step5 Worker POST-FETCH] Race ID {race_id}: HTML取得完了. Content_length: {len(html_content) if html_content else 'N/A'}"
        )
        # ★★★ 追加ここまで ★★★

        if not html_content:
            self.logger.warning(
                f"[Thread-{thread_id}][Step5 Worker FETCH-FAIL] Race {race_id}: HTML取得に失敗しました。 URL: {url}"
            )
            return None  # HTML取得失敗はNone (is_emptyではない)

        try:
            # ★★★ 詳細ログ追加 ★★★
            self.logger.debug(
                f"[Thread-{thread_id}][Step5 Worker PRE-PARSE] Race {race_id}: HTML解析開始. URL: {url}"
            )
            parsed_data = self._parse_yenjoy_result_html(html_content, race_id)
            self.logger.debug(
                f"[Thread-{thread_id}][Step5 Worker POST-PARSE] Race {race_id}: HTML解析完了. Parsed keys: {list(parsed_data.keys()) if parsed_data else 'N/A'}, IsEmpty: {parsed_data.get('is_empty') if parsed_data else 'N/A'}"
            )
            # ★★★ 追加ここまで ★★★

            if parsed_data and parsed_data.get("problematic_rows"):
                self.logger.warning(
                    f"[Thread-{thread_id}] Race {race_id}: Found problematic rows during parsing:"
                )
                for problematic_row_html in parsed_data["problematic_rows"]:
                    # HTMLが長すぎる可能性があるので、最初の500文字程度に制限
                    self.logger.warning(
                        f"  Problematic Row HTML (partial): {problematic_row_html[:500]}"
                    )

            # レートリミットのための待機
            # time.sleep(self.rate_limit_wait) # 削除

            # 解析結果が空、または必須の race_results がない場合は失敗とみなす -> is_empty フラグで判定するよう変更済み
            if not parsed_data or parsed_data.get(
                "parse_error", False
            ):  # parse_error が True の場合も失敗とみなす
                self.logger.warning(
                    f"[Thread-{thread_id}][Step5 Worker PARSE-FAIL] Race {race_id}: HTML解析でエラーが発生したか、必須データが得られませんでした。 URL: {url}"
                )
                # parsed_data に problematic_rows が含まれている場合でも、主要データがなければ None を返す
                # -> parse_error があれば、その内容を返す (is_empty=True, parse_error=True が含まれる)
                return (
                    parsed_data
                    if parsed_data and parsed_data.get("parse_error")
                    else None
                )  # parse_error時はそれを、それ以外(None)はNone

            parsed_data["race_id"] = race_id
            return parsed_data

        except Exception as e:
            # ★★★ 詳細ログ追加 ★★★
            self.logger.error(
                f"[Thread-{thread_id}][Step5 Worker ERROR] Race {race_id} の結果取得・解析中に予期せぬエラー: {type(e).__name__} - {e}. URL: {url}",
                exc_info=True,
            )
            # ★★★ 追加ここまで ★★★
            return None  # ワーカー内での予期せぬエラーはNoneを返す

    def _parse_yenjoy_result_html(
        self, html_content: str, race_id: str
    ) -> Dict[str, List[Dict]]:
        """
        yen-joy.net の結果詳細ページの HTML を解析し、必要なデータを抽出する。
        (払戻金情報の抽出は削除)
        """
        parsed_data = {
            "race_results": [],
            "race_comments": [],
            "inspection_reports": [],
            "lap_data_by_section": {},  # セクションごとの JSON データを格納
            "problematic_rows": [],  # ★ 問題行格納用リスト追加
        }
        thread_id = threading.get_ident()
        self.logger.debug(f"[Thread-{thread_id}] Race {race_id}: HTML解析開始...")

        if not html_content:
            self.logger.warning(
                f"[Thread-{thread_id}] Race {race_id}: 解析する HTML コンテンツがありません。"
            )
            return {"is_empty": True}  # ★ 空コンテンツの場合は is_empty = True で返す

        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # --- 1. race_results テーブル用データの抽出 ---
            try:
                result_table = soup.find("table", class_="result-table-detail")
                if result_table:
                    tbody = result_table.find("tbody")  # ★ tbody を探す
                    if tbody:  # ★ tbody が存在するか確認
                        rows = tbody.find_all("tr")[2:]  # ★ ヘッダー行を2行除外
                        for row_index, row in enumerate(rows):  # ★ 行インデックス追加
                            cells = row.find_all("td")
                            row_log_prefix = f"[Thread-{thread_id}] Race {race_id} Result Row {row_index+1}:"  # ★ ログ用プレフィックス

                            # ★ セル数のチェックを >= 14 に変更 (勝敗因、個人状況まで)
                            if len(cells) >= 14:
                                rank_text = self._normalize_text(cells[0].get_text())
                                # ★★★ 修正点: 'i' タグも検索対象に追加 ★★★
                                bracket_number_tag = cells[1].find(
                                    ["span", "div", "i"]
                                )  # クラス名で特定できない場合がある
                                bracket_number_str = (
                                    self._normalize_text(bracket_number_tag.get_text())
                                    if bracket_number_tag
                                    else None
                                )

                                # ★★★ bracket_number のチェックと変換 ★★★
                                bracket_number = self._safe_cast(
                                    bracket_number_str, int
                                )
                                if bracket_number is None:
                                    self.logger.error(
                                        f"{row_log_prefix} bracket_number が取得または変換できませんでした。元文字列: '{bracket_number_str}'。この行をスキップします。 Row HTML: {row}"
                                    )
                                    parsed_data["problematic_rows"].append(
                                        f"bracket_number is None: {row}"
                                    )  # 問題リストにも追加
                                    continue  # ★★★ この行の処理をスキップ ★★★
                                # ★★★ チェックここまで ★★★

                                # ★ 印 (Mark) を cells[2] から取得
                                mark = self._normalize_text(cells[2].get_text())

                                player_name_tag = cells[3].find("a")  # aタグ内の選手名
                                player_name = (
                                    self._normalize_text(player_name_tag.get_text())
                                    if player_name_tag
                                    else self._normalize_text(cells[3].get_text())
                                )
                                player_link = (
                                    player_name_tag["href"]
                                    if player_name_tag
                                    and player_name_tag.has_attr("href")
                                    else None
                                )
                                player_id_match = (
                                    re.search(r"/(\d+)$", player_link)
                                    if player_link
                                    else None
                                )
                                player_id = (
                                    player_id_match.group(1)
                                    if player_id_match
                                    else None
                                )

                                # ★ 年齢 (Age) を cells[4] から取得 (コメント解除)
                                age = self._safe_cast(
                                    self._normalize_text(cells[4].get_text()), int
                                )
                                prefecture = self._normalize_text(cells[5].get_text())
                                # ★ 期別 (Period) を cells[6] から取得 (コメント解除、'期'を削除)
                                period_str = self._normalize_text(
                                    cells[6].get_text()
                                ).replace("期", "")
                                period = self._safe_cast(period_str, int)
                                player_class = self._normalize_text(
                                    cells[7].get_text()
                                )  # 級班
                                diff = self._normalize_text(cells[8].get_text())  # 着差
                                time_str = self._normalize_text(
                                    cells[9].get_text()
                                )  # 上りタイム
                                last_lap_time = self._normalize_text(
                                    cells[9].get_text()
                                )  # 上りタイムを流用 (仕様確認)
                                winning_technique = self._normalize_text(
                                    cells[10].get_text()
                                )  # 決まり手
                                symbols_jhb = self._normalize_text(
                                    cells[11].get_text()
                                )  # S/JH/B
                                # ★ 勝敗因 (Win Factor) を cells[12] から取得
                                win_factor = self._normalize_text(cells[12].get_text())
                                # ★ 個人状況 (Personal Status) を cells[13] から取得
                                personal_status = self._normalize_text(
                                    cells[13].get_text()
                                )

                                # ランクを数値に変換試行
                                rank = self._safe_cast(rank_text, int)

                                result_entry = {
                                    "race_id": race_id,
                                    "bracket_number": bracket_number,
                                    "rank": rank,
                                    "rank_text": rank_text,  # 失格などの文字情報保持用
                                    "mark": mark,  # ★ 追加
                                    "player_name": player_name,
                                    "player_id": player_id,  # リンクから抽出試行
                                    "age": age,  # ★ 追加
                                    "prefecture": prefecture,
                                    "period": period,  # ★ 追加
                                    "class": player_class,
                                    "diff": diff,
                                    "time": self._safe_cast(
                                        time_str, float
                                    ),  # 上りタイムをfloatに
                                    "last_lap_time": last_lap_time,  # 文字列のまま
                                    "winning_technique": winning_technique,
                                    "symbols": symbols_jhb,  # S/JH/B
                                    "win_factor": win_factor,
                                    "personal_status": personal_status,
                                    # 'recent_results': None, <-- 削除
                                }
                                parsed_data["race_results"].append(result_entry)
                            else:
                                # ★ 期待セル数を 14 に変更
                                self.logger.warning(
                                    f"{row_log_prefix} 結果テーブルの行のセル数が予期したものではありません ({len(cells)}件)。期待値: 14以上"
                                )
                                try:
                                    parsed_data["problematic_rows"].append(
                                        f"Incorrect cell count ({len(cells)}): {row}"
                                    )
                                except Exception as str_ex:
                                    self.logger.error(
                                        f"{row_log_prefix} Error converting problematic row to string: {str_ex}"
                                    )
                    else:
                        self.logger.warning(
                            f"[Thread-{thread_id}] Race {race_id}: 結果テーブル内に <tbody> が見つかりませんでした。"
                        )
                else:
                    self.logger.warning(
                        f"[Thread-{thread_id}] Race {race_id}: 結果テーブル <table class='result-table-detail'> が見つかりませんでした。"
                    )

            except Exception as table_ex:
                self.logger.error(
                    f"[Thread-{thread_id}] Race {race_id}: 結果テーブル解析全体でエラー: {table_ex}",
                    exc_info=True,
                )

            # --- 2. race_comments テーブル用データの抽出 ---
            try:
                payout_table = soup.find("table", class_="result-pay")
                if payout_table:
                    # --- 3. レースコメント (tfoot からのみ取得) ---
                    tfoot = payout_table.find("tfoot")
                    if tfoot:
                        comment_td = tfoot.find("td")
                        if comment_td:
                            for br in comment_td.find_all("br"):
                                br.replace_with("\\n")
                            comment_text = self._normalize_text(comment_td.get_text())
                            if comment_text:
                                # 単純に追加
                                parsed_data["race_comments"].append(
                                    {"race_id": race_id, "comment": comment_text}
                                )
                        else:
                            self.logger.warning(
                                f"[Thread-{thread_id}] Race {race_id}: 払戻テーブルの tfoot 内に td が見つかりません。"
                            )
                    else:
                        self.logger.warning(
                            f"[Thread-{thread_id}] Race {race_id}: 払戻テーブルに tfoot が見つかりません。"
                        )

                    # --- 2. 払戻情報 (tbody) ---
                    # 払戻情報の抽出ロジック全体を削除 (ここから)
                    # tbody = payout_table.find('tbody')
                    # if tbody:
                    # ... (tbody内の解析ロジックすべて) ...
                    # parsed_data['payouts'].append(payout_entry)
                    # ...
                    # else:
                    #      self.logger.warning(f"[Thread-{thread_id}] Race {race_id}: 払戻テーブルに tbody が見つかりません。")
                    # 払戻情報の抽出ロジック全体を削除 (ここまで)
                else:
                    # payout_table が見つからなくてもコメントだけ抽出する可能性があるので、ここは警告のみ
                    self.logger.info(
                        f"[Thread-{thread_id}] Race {race_id}: 払戻/コメントテーブル <table class='result-pay'> が見つかりませんでした。コメントのみ抽出を試みます。"
                    )

            except Exception as payout_ex:
                self.logger.error(
                    f"[Thread-{thread_id}] Race {race_id}: コメント解析中にエラー: {payout_ex}",
                    exc_info=True,
                )  # エラーメッセージ変更

            # --- 4. inspection_reports テーブル用データの抽出 ---
            try:
                report_div = soup.find("div", class_="result-kensya")
                if report_div:
                    player_reports = report_div.find_all(
                        "div", class_="result-kensyajyou-report-wrap"
                    )  # 選手ごとのブロックを探す
                    if player_reports:
                        for report_block in player_reports:
                            player_name_tag = report_block.find("h4")
                            comment_p = report_block.find("p")
                            if player_name_tag and comment_p:
                                player_name_full = self._normalize_text(
                                    player_name_tag.get_text()
                                )
                                # 名前と所属/期別を分離する (例: "山田 庸平 佐賀 94期")
                                match = re.match(
                                    r"^(.*?)\\s+([\\S]+)\\s+(\\d+期)$", player_name_full
                                )
                                player_name = (
                                    match.group(1).strip()
                                    if match
                                    else player_name_full
                                )  # マッチしなければ全体を名前とする

                                comment = self._normalize_text(comment_p.get_text())
                                if player_name and comment:
                                    parsed_data["inspection_reports"].append(
                                        {
                                            "race_id": race_id,
                                            "player": player_name,  # 名前のみ抽出
                                            "comment": comment,
                                        }
                                    )
                                else:
                                    self.logger.warning(
                                        f"[Thread-{thread_id}] Race {race_id}: 検車場レポートの選手名またはコメントが空です。 Block: {report_block}"
                                    )

                            else:
                                self.logger.warning(
                                    f"[Thread-{thread_id}] Race {race_id}: 検車場レポートの h4 または p が見つかりません。 Block: {report_block}"
                                )
                    else:
                        # 検車場レポートの単一pタグを探す(複数選手のコメントが一つのpタグに入っている場合)
                        report_p = report_div.find(
                            "p", class_="result-kensya-report-text"
                        )
                        if report_p:
                            comment_text = self._normalize_text(report_p.get_text())
                            # 【選手名】でコメントが区切られているか確認
                            player_comments = re.findall(
                                r"【(.*?)】(.*?)(?=【|$)", comment_text
                            )

                            if player_comments:
                                for player_name, comment in player_comments:
                                    player_name = player_name.strip()
                                    comment = comment.strip()
                                    if player_name and comment:
                                        parsed_data["inspection_reports"].append(
                                            {
                                                "race_id": race_id,
                                                "player": player_name,
                                                "comment": comment,
                                            }
                                        )
                            else:
                                self.logger.warning(
                                    f"[Thread-{thread_id}] Race {race_id}: 検車場レポートから選手コメントを抽出できませんでした。"
                                )
                        else:
                            # サンプルページのような画像と文章の形式の場合
                            report_items = report_div.find_all(["h4", "p"])
                            current_player = None
                            for item in report_items:
                                if item.name == "h4":
                                    player_name_full = self._normalize_text(
                                        item.get_text()
                                    )
                                    match = re.match(
                                        r"^(.*?)\\s+([\\S]+)\\s+(\\d+期)$",
                                        player_name_full,
                                    )
                                    current_player = (
                                        match.group(1).strip()
                                        if match
                                        else player_name_full
                                    )
                                elif item.name == "p" and current_player:
                                    comment = self._normalize_text(item.get_text())
                                    # 【選手名（着順）】形式の除去
                                    comment = re.sub(r"^【.*?】\\s*", "", comment)
                                    if comment:
                                        parsed_data["inspection_reports"].append(
                                            {
                                                "race_id": race_id,
                                                "player": current_player,
                                                "comment": comment,
                                            }
                                        )
                                    # 次のh4が現れるまで同じ選手のコメントとみなすか、pタグごとにリセットするか？ -> h4が現れたらリセット
                                    # current_player = None # リセットしない方が安全か？ 一旦コメントアウト
                            if not parsed_data["inspection_reports"]:
                                self.logger.warning(
                                    f"[Thread-{thread_id}] Race {race_id}: 検車場レポートの構造が予期したものと異なります (class='result-kensyajyou-report-wrap' も h4/p の組み合わせも見つからないか、データ抽出失敗)。DIV: {report_div}"
                                )

                else:
                    self.logger.warning(
                        f"[Thread-{thread_id}] Race {race_id}: 検車場レポートのdiv <div class='result-kensya'> が見つかりませんでした。"
                    )
            except Exception as parse_ex:
                self.logger.error(
                    f"[Thread-{thread_id}] Race {race_id}: 検車場レポート解析中にエラー: {parse_ex}",
                    exc_info=True,
                )

            # --- 5. 周回データ (セクション別 JSON) の抽出 --- ★ 修正箇所
            try:
                lap_wrapper_div = soup.find("div", class_="result-b-hyo-lap-wrapper")
                # セクション名とカラム名のマッピング (必要に応じて調整)
                section_name_map = {
                    "周回": "lap_shuukai",
                    "周": "lap_shuukai",  # 短縮形も考慮
                    "赤板": "lap_akaban",
                    "赤": "lap_akaban",
                    "打鐘": "lap_dasho",
                    "打": "lap_dasho",
                    "HS": "lap_hs",  # ホームストレッチ
                    "H": "lap_hs",  # 短縮形も考慮
                    "BS": "lap_bs",  # バックストレッチ
                    "B": "lap_bs",  # 短縮形も考慮
                    # 他に必要なセクションがあれば追加
                }

                if lap_wrapper_div:
                    b_hyo_divs = lap_wrapper_div.find_all(
                        "div", class_="b-hyo", recursive=False
                    )
                    for b_hyo_div in b_hyo_divs:
                        table = b_hyo_div.find("table", class_="mawari")
                        if table:
                            # セクション名 (周回, 赤板, etc.) を th から取得
                            th = table.find("th")
                            section_name_raw = (
                                self._normalize_text(th.get_text(separator="").strip())
                                if th
                                else "Unknown"
                            )

                            # マッピングされたカラム名を取得、なければ Raw 名を使うかスキップ
                            section_key = section_name_map.get(section_name_raw)
                            if not section_key:
                                self.logger.warning(
                                    f"[Thread-{thread_id}] Race {race_id}: 不明な周回セクション名 '{section_name_raw}'。スキップします。"
                                )
                                continue

                            # このセクションの選手データリストを初期化
                            section_player_list = []

                            # 各選手の情報を抽出
                            bike_icons = table.find_all(
                                "span", class_="bike-icon-wrapper"
                            )
                            for icon_wrapper in bike_icons:
                                classes = icon_wrapper.get("class", [])
                                bike_no_class = next(
                                    (c for c in classes if c.startswith("bikeno-")),
                                    None,
                                )
                                x_pos_class = next(
                                    (c for c in classes if c.startswith("x-")), None
                                )
                                y_pos_class = next(
                                    (c for c in classes if c.startswith("y-")), None
                                )

                                if bike_no_class and x_pos_class and y_pos_class:
                                    try:
                                        bike_no_str = bike_no_class.split("-")[1]
                                        # bikeno-0 も含めるのでスキップしない
                                        bracket_number = self._safe_cast(
                                            bike_no_str, int
                                        )
                                        x_position = self._safe_cast(
                                            x_pos_class.split("-")[1], int
                                        )
                                        y_position = self._safe_cast(
                                            y_pos_class.split("-")[1], int
                                        )

                                        racer_name_span = icon_wrapper.find(
                                            "span", class_="racer-nm"
                                        )
                                        # 誘導員の場合、名前が取れないことがあるのでデフォルト値を設定
                                        racer_name = (
                                            self._normalize_text(
                                                racer_name_span.get_text()
                                            )
                                            if racer_name_span
                                            else (
                                                "誘導員"
                                                if bike_no_str == "0"
                                                else "不明"
                                            )
                                        )

                                        # arrow クラスの有無をチェック
                                        bike_icon_span = icon_wrapper.find(
                                            "span", class_="bike-icon"
                                        )
                                        has_arrow = bool(
                                            bike_icon_span
                                            and "arrow"
                                            in bike_icon_span.get("class", [])
                                        )

                                        # bracket_number は必須とする (誘導員は0になる)
                                        if (
                                            bracket_number is not None
                                            and x_position is not None
                                            and y_position is not None
                                        ):
                                            # [bracket_number, racer_name, X, Y, has_arrow] のリスト形式で追加
                                            section_player_list.append(
                                                [
                                                    bracket_number,
                                                    racer_name,
                                                    x_position,
                                                    y_position,
                                                    has_arrow,
                                                ]
                                            )
                                        else:
                                            self.logger.warning(
                                                f"[Thread-{thread_id}] Race {race_id}: 周回データの一部が欠損しています。Icon: {icon_wrapper}"
                                            )

                                    except (IndexError, ValueError) as parse_err:
                                        self.logger.error(
                                            f"[Thread-{thread_id}] Race {race_id}: 周回データのクラス属性解析エラー: {parse_err}. Icon: {icon_wrapper}",
                                            exc_info=True,
                                        )

                            # セクションのデータリストを全体の辞書に追加
                            if section_player_list:
                                try:
                                    # ★ JSON 文字列に変換 (ensure_ascii=False で日本語を保持)
                                    json_string = json.dumps(
                                        section_player_list, ensure_ascii=False
                                    )
                                    parsed_data["lap_data_by_section"][
                                        section_key
                                    ] = json_string
                                except Exception as json_err:
                                    self.logger.error(
                                        f"[Thread-{thread_id}] Race {race_id}: 周回データ ({section_key}) の JSON 変換エラー: {json_err}",
                                        exc_info=True,
                                    )
                            else:
                                self.logger.warning(
                                    f"[Thread-{thread_id}] Race {race_id}: セクション '{section_key}' で選手データが見つかりませんでした。"
                                )
                        else:
                            self.logger.warning(
                                f"[Thread-{thread_id}] Race {race_id}: 周回セクション <table class='mawari'> が見つかりません。DIV: {b_hyo_div}"
                            )
                else:
                    self.logger.warning(
                        f"[Thread-{thread_id}] Race {race_id}: 周回データラッパー <div class='result-b-hyo-lap-wrapper'> が見つかりません。"
                    )

            except Exception as lap_ex:
                self.logger.error(
                    f"[Thread-{thread_id}] Race {race_id}: 周回データ解析中にエラー: {lap_ex}",
                    exc_info=True,
                )

            # === HTML 解析ロジックここまで ===

            for key, data_list in parsed_data.items():
                # data_list が辞書の場合 (lap_data_by_section) は件数ログを変更
                count = (
                    len(data_list)
                    if isinstance(data_list, list)
                    else len(data_list.keys())
                )
                # payouts を削除したので、payouts でなければログ出力
                # ★ problematic_rows もログ出力対象から除外
                if key not in ["payouts", "problematic_rows"] and count > 0:
                    self.logger.debug(
                        f"[Thread-{thread_id}] Race {race_id}: 解析結果 {key}: {count} 件/セクション"
                    )

            # ★★★ 追加: データが実質的に空かどうかの判定 ★★★
            is_effectively_empty = True
            # problematic_rows は判定対象外とする
            if parsed_data.get("race_results"):
                is_effectively_empty = False
            if parsed_data.get("race_comments"):
                is_effectively_empty = False
            if parsed_data.get("inspection_reports"):
                is_effectively_empty = False
            if parsed_data.get("lap_data_by_section"):
                is_effectively_empty = False

            parsed_data["is_empty"] = is_effectively_empty
            if is_effectively_empty:
                self.logger.info(
                    f"[Thread-{thread_id}] Race {race_id}: 解析の結果、有効なデータが見つかりませんでした (is_empty=True)。"
                )
            # ★★★ 追加ここまで ★★★

            return parsed_data

        except Exception as e:
            self.logger.error(
                f"[Thread-{thread_id}] Race {race_id}: HTML解析中に予期せぬエラー: {e}",
                exc_info=True,
            )
            # ★★★ エラー時は is_empty を True とし、他のデータは空のリスト/辞書で返す ★★★
            return {
                "race_results": [],
                "race_comments": [],
                "inspection_reports": [],
                "lap_data_by_section": {},
                "problematic_rows": [],
                "is_empty": True,  # 解析エラーも実質データなしとみなす
                "parse_error": True,  # エラー識別のためのフラグ
            }

    def _normalize_text(self, text: str) -> str:
        """
        テキストを正規化する
        """
        if not text:
            return ""
        return text.strip()

    def _safe_cast(self, value: str, cast_type: type) -> Optional[Any]:
        """
        文字列を指定された型に安全にキャストする
        """
        if not value:
            return None
        try:
            return cast_type(value)
        except (ValueError, TypeError):
            return None

    def _save_race_result(self, race, result_data):
        # 実装が必要
        return True

    def _save_race_payout(self, race, payout_data):
        # 実装が必要
        return True

    def _save_race_lap(self, race, lap_data):
        # 実装が必要
        return True

    def _save_race_position(self, race, position_data):
        # 実装が必要
        return True

    def update_results_bulk(
        self, start_date: str, end_date: str, force: bool = False
    ) -> Dict:
        """
        指定された期間の結果情報を一括で更新する

        Args:
            start_date (str): 開始日 (YYYY-MM-DD)
            end_date (str): 終了日 (YYYY-MM-DD)
            force (bool, optional): race_status の step5_status を無視して抽出するかどうか。
                                    Defaults to False.

        Returns:
            Dict: 処理結果 (成功した件数、失敗した件数、エラーリストなど)
        """
        thread_id = threading.current_thread().ident
        # ★★★ ログに force 状態を追加 ★★★
        self.logger.info(
            f"[Thread-{thread_id}] Step 5 結果情報の一括更新を開始します。期間: {start_date} - {end_date}, 強制: {force}"
        )

        # Extractor を使用して更新対象のレース情報を取得
        try:
            # ★★★ force 引数を渡すように修正 ★★★
            races_to_update = self.step5_extractor.extract(
                start_date_filter=start_date,
                end_date_filter=end_date,
                force=force,  # force 引数を渡す
            )
            self.logger.info(
                f"[Thread-{thread_id}] Step 5 Extractor が {len(races_to_update)} 件の更新対象レースを抽出しました。"
            )
        except Exception as e:
            self.logger.error(
                f"[Thread-{thread_id}] Step 5 Extractor でエラーが発生しました: {e}",
                exc_info=True,
            )
            return {
                "processed": 0,
                "success": 0,
                "failed": 0,
                "errors": [f"Extractor error: {e}"],
                "message": "データ抽出エラー",
            }

        if not races_to_update:
            self.logger.info(
                f"[Thread-{thread_id}] Step 5 更新対象のレースが見つかりません。"
            )
            return {
                "processed": 0,
                "success": 0,
                "failed": 0,
                "errors": [],
                "message": "更新対象なし",
            }

        # 既に処理済みのラップ/ポジションデータをフィルタリング（オプション）
        # if self.filter_processed:
        #     existing_lap_ids = self._get_processed_lap_race_ids([r[\'race_id\'] for r in races_to_update])
        #     races_to_update = [r for r in races_to_update if r[\'race_id\'] not in existing_lap_ids]
        #     self.logger.info(f"[Thread-{thread_id}] 処理済みを除外後、{len(races_to_update)} 件のレースを更新します。")
        #     if not races_to_update:
        #         return {"processed": 0, "success": 0, "failed": 0, "errors": [], "message": "処理済みを除外した結果、更新対象なし"}

        # 結果格納用
        total_processed = len(races_to_update)
        success_count = 0
        failed_count = 0
        error_messages = []
        # 解析結果を一時的に保存するリスト
        all_parsed_data = []
        skipped_no_data_count = 0  # ★ データなしでスキップした件数

        # 並列処理で各レースの結果を取得・保存
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_race = {
                executor.submit(
                    self._fetch_and_parse_result_worker, race_info
                ): race_info
                for race_info in races_to_update
            }

            for future in as_completed(future_to_race):
                race_info = future_to_race[future]
                race_id = race_info.get("race_id")
                try:
                    result_data = (
                        future.result()
                    )  # result_data には is_empty, parse_error が含まれる可能性
                    if result_data:
                        if result_data.get(
                            "parse_error", False
                        ):  # 解析中に例外が発生した場合
                            failed_count += 1
                            self.logger.error(
                                f"[Thread-{thread_id}] race_id {race_id} のHTML解析中に内部エラーが発生しました。"
                            )
                            error_messages.append(
                                f"{race_id}: HTML Parse internal error"
                            )
                            try:
                                self.step5_saver.update_race_status(race_id, "error")
                            except Exception as status_err:
                                self.logger.error(
                                    f"[Thread-{thread_id}] race_id {race_id} の解析内部エラー状態への更新に失敗: {status_err}"
                                )

                        elif result_data.get(
                            "is_empty", False
                        ):  # データなしまたは空の場合
                            skipped_no_data_count += 1  # ★ スキップカウントを増やす
                            self.logger.info(
                                f"[Thread-{thread_id}] race_id {race_id} はデータ未アップロードまたは内容が空のためスキップします。"
                            )
                            try:
                                # 'data_not_available' ステータスをsaverに渡す
                                self.step5_saver.update_race_status(
                                    race_id, "data_not_available"
                                )
                            except Exception as status_err:
                                self.logger.error(
                                    f"[Thread-{thread_id}] race_id {race_id} の 'data_not_available' 状態への更新に失敗: {status_err}"
                                )
                            # all_parsed_data には追加しない

                        else:  # 有効なデータがある場合
                            all_parsed_data.append(result_data)

                    else:  # _fetch_and_parse_result_worker が None を返した場合 (HTML取得失敗など)
                        failed_count += 1
                        self.logger.warning(
                            f"[Thread-{thread_id}] race_id {race_id} の結果情報の取得に失敗しました (HTML取得失敗など)。"
                        )
                        error_messages.append(
                            f"{race_id}: Fetch failed (e.g. HTML not retrieved)"
                        )
                        try:
                            self.step5_saver.update_race_status(race_id, "error")
                        except Exception as status_err:
                            self.logger.error(
                                f"[Thread-{thread_id}] race_id {race_id} の取得失敗エラー状態への更新に失敗: {status_err}"
                            )

                except Exception as exc:
                    failed_count += 1
                    self.logger.error(
                        f"[Thread-{thread_id}] race_id {race_id} の処理中に予期せぬエラー: {exc}",
                        exc_info=True,
                    )
                    error_messages.append(f"{race_id}: Unexpected error - {exc}")
                    try:
                        self.step5_saver.update_race_status(race_id, "error")
                    except Exception as status_err:
                        self.logger.error(
                            f"[Thread-{thread_id}] race_id {race_id} のエラー状態への更新に失敗: {status_err}"
                        )

                # レートリミット (必要であれば解析後に配置)
                # time.sleep(self.rate_limit_wait)

        # --- ループ完了後、一括保存処理 ---
        actual_bulk_save_success_count = 0  # ★ 一括保存で実際に成功したレース数
        if all_parsed_data:
            self.logger.info(
                f"[Thread-{thread_id}] {len(all_parsed_data)} 件の解析済みレースデータを一括保存します。"
            )
            # bulk_save_results_etc に渡すためのデータを作成
            bulk_race_results = []
            bulk_race_comments = []
            bulk_inspection_reports = []
            bulk_lap_data = {}

            for data in all_parsed_data:
                race_id = data.get("race_id")
                if not race_id:
                    continue
                # 各データリスト/辞書に追加
                if data.get("race_results"):
                    bulk_race_results.extend(data["race_results"])
                if data.get("race_comments"):
                    bulk_race_comments.extend(data["race_comments"])
                if data.get("inspection_reports"):
                    bulk_inspection_reports.extend(data["inspection_reports"])
                if data.get("lap_data_by_section"):
                    bulk_lap_data[race_id] = data["lap_data_by_section"]

            # 一括保存メソッドを呼び出し
            try:
                bulk_success, saved_counts = self.step5_saver.bulk_save_results_etc(
                    race_results=bulk_race_results,
                    race_comments=bulk_race_comments,
                    inspection_reports=bulk_inspection_reports,
                    lap_data=bulk_lap_data,
                )

                if bulk_success:
                    # bulk_save_results_etc は race_id 単位の成否を直接返さないため、
                    # ここでは all_parsed_data に含まれるレースIDの数を「一括保存試行成功」とみなす。
                    # より正確には、saved_counts の中身を見て判断する必要がある。
                    # 例: saved_counts['race_results'] などが0より大きければ、その種類のデータは一部保存された。
                    # ここでは簡略化し、bulk_success フラグで判断。
                    actual_bulk_save_success_count = len(
                        all_parsed_data
                    )  # 仮に全件成功したとする
                    success_count = actual_bulk_save_success_count  # ★ 変更
                    self.logger.info(
                        f"[Thread-{thread_id}] 一括保存成功。 保存件数: {saved_counts}"
                    )
                else:
                    self.logger.error(
                        f"[Thread-{thread_id}] 一括保存処理が失敗しました。 保存件数: {saved_counts}"
                    )
                    # 失敗した場合、all_parsed_data 全てが失敗扱いになる可能性がある
                    # failed_count に加算するかは、エラーの粒度による。ここでは bulk_save_results_etc が失敗したら、
                    # all_parsed_data のレースは個別の成功とはみなさない。
                    # failed_count は主に fetch/parse error をカウントしているので、ここでの failed_count の扱いは注意。
                    error_messages.append(
                        "Bulk save operation failed for some/all parsed data."
                    )
                    # バルク保存失敗の場合、個別のレースのステータスを 'error' に更新するか検討
                    # for data_item in all_parsed_data:
                    #    if data_item.get('race_id'):
                    #        try:
                    #            self.step5_saver.update_race_status(data_item['race_id'], 'error')
                    #        except Exception as status_update_err:
                    #            self.logger.error(f"Error updating status for {data_item['race_id']} after bulk save failure: {status_update_err}")

            except Exception as bulk_err:
                self.logger.error(
                    f"[Thread-{thread_id}] 一括保存呼び出し中にエラー: {bulk_err}",
                    exc_info=True,
                )
                # failed_count += len(all_parsed_data) # エラー時は全件失敗扱い -> これは重複カウントになる可能性
                error_messages.append(f"Bulk save call error: {bulk_err}")
        else:
            self.logger.info(
                f"[Thread-{thread_id}] 一括保存対象の有効なデータがありませんでした。"
            )

        # 最終結果を組み立て
        final_message = (
            f"Step 5 処理完了: "
            f"Total Extracted={total_processed}, "
            f"Parsed with Data={len(all_parsed_data)}, "
            f"Skipped (No Data/Empty)={skipped_no_data_count}, "
            f"Bulk Save Success Races={success_count}, "
            f"Failed (Fetch/Parse/Save Error)={failed_count}"
        )
        self.logger.info(f"[Thread-{thread_id}][Step5 Summary] {final_message}")
        if error_messages:
            self.logger.warning(
                f"[Thread-{thread_id}][Step5 Summary] エラーが発生したレースがあります ({len(error_messages)}件)。エラーメッセージサンプル (最初の5件): {error_messages[:5]}"
            )

        # ★★★ 新しい成功判定ロジック ★★★
        # 何も成功しなかった場合のみ失敗とする
        # success_count はバルク保存が成功したレース数 (Updater内部判断)
        # failed_count はAPI取得/解析エラーのレース数
        # skipped_no_data_count はデータなしでスキップしたレース数

        overall_step_success = False
        if (
            total_processed == 0
        ):  # 処理対象がなければ成功（何もしなかったがエラーではない）
            overall_step_success = True
        elif success_count > 0:  # 1件でもバルク保存が成功していれば成功
            overall_step_success = True
        elif (
            skipped_no_data_count == total_processed
        ):  # 全てデータなしでスキップされた場合も成功
            overall_step_success = True
        # 上記以外 (処理対象あり、成功0件、かつ全件スキップでもない場合) は overall_step_success = False のまま

        self.logger.info(
            f"[Thread-{thread_id}][Step5 Overall判定] Total: {total_processed}, SuccessInternal: {success_count}, Skipped: {skipped_no_data_count}, FailedInternal: {failed_count} => OverallStepSuccess: {overall_step_success}"
        )
        # ★★★ ここまで ★★★

        return {
            "processed": total_processed,
            "parsed_with_data": len(all_parsed_data),
            "skipped_no_data": skipped_no_data_count,
            "success_internal_saves": success_count,  # バルク保存成功レース数 (内部的な成功)
            "failed_fetch_parse": failed_count,  # API取得/解析エラーレース数
            "errors": error_messages,
            "message": final_message,
            "success": overall_step_success,  # ★ UpdateService が見るべき最終的な成功フラグ
        }

    def _get_processed_lap_race_ids(self, race_ids: List[str]) -> set:
        """指定された race_id リストのうち、既にラップデータが保存されているものを返す"""
        if not race_ids:
            return set()

        placeholders = ",".join(["?"] * len(race_ids))
        query = (
            f"SELECT DISTINCT race_id FROM race_laps WHERE race_id IN ({placeholders})"
        )
        params = tuple(race_ids)

        try:
            result = self.db.execute_query(query, params=params, fetch_all=True)
            return set(row["race_id"] for row in result) if result else set()
        except Exception as e:
            self.logger.error(
                f"処理済みラップデータの取得中にエラー: {e}", exc_info=True
            )
            return set()  # エラー時は空セットを返す
