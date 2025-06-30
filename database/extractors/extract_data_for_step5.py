import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional  # noqa: F401


class Step5DataExtractor:
    """
    Step 5: レース結果取得に必要な情報をデータベースから抽出するクラス。
    yen-joy.net の URL 構築に必要な情報を races, schedules, cups テーブルから取得する。
    """

    def __init__(self, database, logger: Optional[logging.Logger] = None):
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    def extract(
        self,
        start_date_filter: Optional[str] = None,
        end_date_filter: Optional[str] = None,
        force: bool = False,
    ) -> List[Dict]:
        """
        指定された日付範囲のレース情報から、結果取得用 URL 構築に必要な情報を抽出する。

        Args:
            start_date_filter (Optional[str]): 抽出対象のレース開催日の開始日 (YYYY-MM-DD)
            end_date_filter (Optional[str]): 抽出対象のレース開催日の終了日 (YYYY-MM-DD)
            force (bool, optional): 処理済み (lap_data_status.is_processed=1) のレースも強制的に抽出するかどうか。
                                    Defaults to False.

        Returns:
            List[Dict]: 抽出されたレース情報のリスト。各辞書には URL 構築に必要なキーが含まれる。
                        日付範囲が無効またはデータがない場合は空リスト。
        """
        thread_id = threading.get_ident()
        self.logger.info(
            f"[Thread-{thread_id}] Step 5 データ抽出開始。"
            f"期間: {start_date_filter} - {end_date_filter}, 強制: {force}"
        )

        if not start_date_filter or not end_date_filter:
            self.logger.error(
                f"[Thread-{thread_id}] Step 5 データ抽出エラー: "
                f"開始日または終了日が指定されていません。全件取得は安全でないため中止します。"
            )
            return []

        # ★ Input filters ('YYYY-MM-DD' strings) を datetime オブジェクトに変換 ★
        try:
            start_dt = datetime.strptime(start_date_filter, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date_filter, "%Y-%m-%d")
        except (ValueError, TypeError) as e:
            self.logger.error(
                f"[Thread-{thread_id}] Step 5 データ抽出エラー: "
                f"無効な日付形式です ({start_date_filter}, {end_date_filter})。エラー: {e}"
            )
            return []

        # ★ datetime オブジェクトを 'YYYYMMDD' 形式に変換してクエリに使用 ★
        start_date_ymd = start_dt.strftime("%Y%m%d")
        end_date_ymd = end_dt.strftime("%Y%m%d")

        conn = None
        cursor = None
        extracted_data = []

        try:
            conn = self.database.connect()
            cursor = conn.cursor()

            # races, schedules, cups を JOIN して必要な情報を取得
            query_base = """
            SELECT
                r.race_id,          -- 結果とレースを紐付けるID
                r.number AS race_number, -- URLの /race_number
                s.date AS race_date,     -- URLの /race_date (YYYYMMDD)
                c.start_date,       -- URLの /start_date (YYYY-MM-DD)
                c.venue_id,         -- URLの /venue_id
                DATE_FORMAT(s.date, '%Y-%m-%d') AS race_date_db -- Step5Updater用の日付フォーマット
            FROM races r
            JOIN schedules s ON r.schedule_id = s.schedule_id
            JOIN cups c ON s.cup_id = c.cup_id
            LEFT JOIN lap_data_status lds ON r.race_id = lds.race_id
            """

            # WHERE句の条件リスト
            where_clauses = [
                "s.date BETWEEN ? AND ?",  # YYYYMMDD 形式の文字列で比較
                "r.status = 3",  # 変更: 終了ステータスを 3 に修正
                # "r.cancel = 0" -- キャンセルされたレースを除外する場合
            ]

            # force フラグが False の場合のみ、処理済みを除外する条件を追加
            if not force:
                where_clauses.append(
                    "(lds.is_processed IS NULL OR lds.is_processed = 0)"
                )

            # WHERE句を結合
            where_statement = " AND ".join(where_clauses)

            # 完全なクエリを構築
            query = f"""
            {query_base}
            WHERE {where_statement}
            ORDER BY s.date, c.venue_id, r.number;
            """

            self.logger.debug(
                f"[Thread-{thread_id}] Step 5 Query (force={force}): {query}"
            )  # クエリログを追加

            # パラメータは日付のみ
            params = (start_date_ymd, end_date_ymd)

            cursor.execute(query, params)  # パラメータを渡す
            rows = cursor.fetchall()

            # Dict 形式に変換し、追加情報を付与
            columns = [description[0] for description in cursor.description]
            for row in rows:
                race_info = dict(zip(columns, row))

                # start_date (YYYY-MM-DD) から YYYYMM と YYYYMMDD を生成
                cup_start_date_str = race_info.get("start_date")
                if cup_start_date_str:
                    try:
                        # MySQL から取得した日付文字列 (YYYY-MM-DD) を datetime オブジェクトに変換
                        dt_start_date = datetime.strptime(
                            cup_start_date_str, "%Y-%m-%d"
                        )
                        race_info["start_date_ym"] = dt_start_date.strftime(
                            "%Y%m"
                        )  # YYYYMM 形式
                        race_info["start_date_ymd"] = dt_start_date.strftime(
                            "%Y%m%d"
                        )  # YYYYMMDD 形式
                    except ValueError:
                        self.logger.warning(
                            f"Race ID {race_info.get('race_id')}: "
                            f"Cup start_date '{cup_start_date_str}' の形式が不正です。スキップします。"
                        )
                        continue
                else:
                    self.logger.warning(
                        f"Race ID {race_info.get('race_id')}: Cup start_date がありません。スキップします。"
                    )
                    continue

                # race_date (YYYYMMDD) から YYYYMMDD を生成 (形式変換は不要だが存在確認)
                schedule_date_str = race_info.get(
                    "race_date"
                )  # schedules.date (YYYYMMDD)
                if schedule_date_str:
                    # ★ 型と値の詳細なログを追加 ★
                    self.logger.info(
                        f"Parsing schedule_date_str: Value='{schedule_date_str}', "
                        f"Type={type(schedule_date_str)}, Length={len(schedule_date_str)}"
                    )
                    try:
                        # MySQL から取得した日付文字列 (YYYYMMDD) をパース
                        dt_schedule_date = datetime.strptime(
                            schedule_date_str, "%Y%m%d"
                        )
                        race_info["race_date_ymd"] = dt_schedule_date.strftime(
                            "%Y%m%d"
                        )  # そのまま YYYYMMDD を使う

                        # race_date_dbがクエリからセットされていなければ、ここでセット
                        if (
                            "race_date_db" not in race_info
                            or not race_info["race_date_db"]
                        ):
                            race_info["race_date_db"] = dt_schedule_date.strftime(
                                "%Y-%m-%d"
                            )
                            self.logger.debug(
                                f"Race ID {race_info.get('race_id')}: race_date_db をPythonコードで設定: {race_info['race_date_db']}"
                            )
                        else:
                            self.logger.debug(
                                f"Race ID {race_info.get('race_id')}: race_date_db はSQLクエリでセット済み: {race_info['race_date_db']}"
                            )
                    except ValueError as ve:
                        # ★ エラー時も詳細ログを追加 ★
                        self.logger.warning(
                            f"Race ID {race_info.get('race_id')}: Failed to parse Schedule date. "
                            f"Value='{schedule_date_str}', Type={type(schedule_date_str)}. "
                            f"Error: {ve}"
                        )
                        self.logger.warning(
                            f"Race ID {race_info.get('race_id')}: "
                            f"Schedule date '{schedule_date_str}' の形式が不正です "
                            f"(YYYYMMDD 期待)。スキップします。"
                        )  # 元の警告も残す
                        continue
                else:
                    self.logger.warning(
                        f"Race ID {race_info.get('race_id')}: Schedule date がありません。スキップします。"
                    )
                    continue

                # venue_id と race_number が存在することも確認
                if (
                    not race_info.get("venue_id")
                    or race_info.get("race_number") is None
                ):
                    self.logger.warning(
                        f"Race ID {race_info.get('race_id')}: "
                        f"venue_id または race_number がありません。スキップします。"
                    )
                    continue

                # 不要になった元の YYYY-MM-DD 形式の日付を削除 (任意)
                if "start_date" in race_info:
                    del race_info["start_date"]
                # ★ race_date は YYYYMMDD なので削除しない (必要なら race_date_ymd にリネーム等) ★
                # if 'race_date' in race_info: del race_info['race_date']

                extracted_data.append(race_info)

            status_msg = "(強制更新モード)" if force else "未処理 (Step5)"
            self.logger.info(
                f"[Thread-{thread_id}] Step 5 データ抽出完了。"
                f"{len(extracted_data)} 件の{status_msg}レース情報を取得しました。"
                f"期間: {start_date_filter} - {end_date_filter}"
            )

            # race_date_dbが全てのレース情報に含まれているか確認
            missing_date_db = [
                race["race_id"]
                for race in extracted_data
                if "race_date_db" not in race or not race["race_date_db"]
            ]
            if missing_date_db:
                self.logger.warning(
                    f"[Thread-{thread_id}] 警告: {len(missing_date_db)}件のレースでrace_date_dbが欠落しています。"
                    f"例: {missing_date_db[:5]}"
                )
            else:
                self.logger.info(
                    f"[Thread-{thread_id}] 確認: 全ての抽出レース({len(extracted_data)}件)にrace_date_dbが含まれています。"
                )

        except Exception as e:
            self.logger.error(
                f"[Thread-{thread_id}] Step 5 データ抽出中にエラーが発生しました: {e}",
                exc_info=True,
            )
            extracted_data = []  # エラー時は空リストを返す
        finally:
            if cursor:
                cursor.close()
            if conn:
                self.database.close_connection()

        return extracted_data
