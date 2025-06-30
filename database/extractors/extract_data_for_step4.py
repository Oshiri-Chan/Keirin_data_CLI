import logging
import threading
from typing import Any, Dict, List, Optional  # noqa: F401


class Step4DataExtractor:
    """
    Step 4 の更新に必要なデータをデータベースから抽出するクラス。
    主に races テーブルと schedules テーブルから race_id と日付を取得する。
    """

    def __init__(self, database, logger: Optional[logging.Logger] = None):
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    def extract(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        cup_id: Optional[str] = None,
        force_update_all: bool = False,
    ) -> List[Dict]:
        """
        指定された日付範囲または開催IDのレース情報を抽出する。
        Step 4 の API 呼び出しに必要な情報を返す (race_id, cup_id, number, race_index など)。

        Args:
            start_date (Optional[str]): 開始日 (YYYY-MM-DD)
            end_date (Optional[str]): 終了日 (YYYY-MM-DD)
            cup_id (Optional[str]): 特定の開催IDを指定する場合。
                                     指定された場合、start_date/end_dateは無視される。
            force_update_all (bool, optional): race_status の step4_status を無視して抽出するかどうか。
                                    Defaults to False.

        Returns:
            List[Dict]: 抽出されたレース情報のリスト。各辞書には必要な情報が含まれる。
                        条件に合うデータがない場合は空リスト。
        """
        thread_id = threading.current_thread().ident
        self.logger.info(
            f"[Thread-{thread_id}] Step 4 データ抽出開始。"
            f"期間: {start_date} - {end_date}, cup_id: {cup_id}, 強制: {force_update_all}"
        )

        extracted_data = []
        params = ()

        try:
            # クエリのベース部分
            query_base = """
            SELECT
                r.race_id,
                r.cup_id,
                r.schedule_id,
                r.number,
                c.venue_id,
                s.date,
                s.schedule_index AS race_index,
                r.status AS race_table_status
            FROM races r
            JOIN schedules s ON r.schedule_id = s.schedule_id
            JOIN cups c ON r.cup_id = c.cup_id
            JOIN race_status rs ON r.race_id = rs.race_id
            """

            # 条件句を動的に生成
            where_clauses = []

            # cup_id 指定の場合
            if cup_id:
                where_clauses.append("r.cup_id = %s")
                params = (cup_id,)
                self.logger.info(
                    f"[Thread-{thread_id}] 開催ID ({cup_id}) で抽出します。"
                )

            # 期間指定の場合 (cup_id 指定がない場合のみ)
            elif start_date and end_date:
                try:
                    start_date_ymd = start_date.replace("-", "")
                    end_date_ymd = end_date.replace("-", "")
                    where_clauses.append("s.date BETWEEN %s AND %s")
                    params = (start_date_ymd, end_date_ymd)
                    self.logger.info(
                        f"[Thread-{thread_id}] 期間 ({start_date} - {end_date}) で抽出します。"
                    )
                except AttributeError:
                    self.logger.error(
                        f"[Thread-{thread_id}] Step 4 データ抽出エラー: 日付の形式が無効です "
                        f"(文字列ではありません)。 start: {type(start_date)}, end: {type(end_date)}"
                    )
                    return []
            # どちらも指定されていない場合はエラー
            else:
                self.logger.error(
                    f"[Thread-{thread_id}] Step 4 データ抽出エラー: 期間またはcup_idが指定されていません。"
                )
                return []

            # schedules.day IS NOT NULL を schedules.schedule_index IS NOT NULL に変更
            where_clauses.append("s.schedule_index IS NOT NULL")

            # force_update_all フラグに基づいてステータス条件を追加
            if not force_update_all:
                # 強制更新でない場合の抽出条件:
                # 1. レース未終了（races.status != '3'）
                # 2. レース終了済みでも過去にオッズ更新履歴があるもの（odds_statusesテーブルに記録あり）
                where_clauses.append(
                    """(
                        r.status IS NULL OR r.status != '3' OR 
                        EXISTS (SELECT 1 FROM odds_statuses os WHERE os.race_id = r.race_id)
                    )"""
                )
            # WHERE句を結合
            where_statement = " AND ".join(where_clauses)

            # 完全なクエリを構築
            query = f"""
            {query_base}
            WHERE {where_statement}
            ORDER BY s.date, r.schedule_id, r.number;
            """

            self.logger.debug(
                f"[Thread-{thread_id}] Step 4 Query (force={force_update_all}): {query}"
            )
            self.logger.debug(f"[Thread-{thread_id}] Step 4 Params: {params}")

            # データベース接続とクエリ実行
            rows = self.database.execute_query(
                query, params
            )  # KeirinDataAccessor.execute_query を使用

            # Dict 形式に変換 (execute_query が辞書のリストを返す場合はこの処理は不要かもしれない)
            # execute_query の戻り値が辞書のリストであれば、以下の行は削除またはコメントアウト
            if rows and not isinstance(rows[0], dict):
                # KeirinDataAccessorのexecute_queryがタプルのリストを返す場合のフォールバック
                # (通常はaccessor側で辞書に変換することを期待)
                # この部分は実際のexecute_queryの実装に合わせて調整が必要
                # 以下はexecute_queryがcursorを返すと仮定した場合の例だが、
                # 実際にはexecute_queryが直接データ(rows)を返すので、
                # descriptionの取得方法を考える必要がある。
                # しかし、既存の他のExtractorではexecute_queryの結果をそのまま使っているため、
                # ここでも同様に扱えるはず。
                # KeirinDataAccessorが辞書のリストを返すと仮定し、この変換処理は不要とする。
                # もし問題があれば、KeirinDataAccessorのexecute_queryの戻り値を確認。
                pass  # このブロックは実際には不要になる可能性が高い

            if rows:
                extracted_data = rows  # execute_query が辞書のリストを返すと想定
            else:
                extracted_data = []

            status_msg = "(強制更新モード)" if force_update_all else "未処理 (Step4)"
            self.logger.info(
                f"[Thread-{thread_id}] Step 4 データ抽出完了。"
                f"{len(extracted_data)} 件の{status_msg}レース情報を取得しました。"
            )

        except Exception as e:
            self.logger.error(
                f"[Thread-{thread_id}] Step 4 データ抽出中にエラーが発生しました: {e}",
                exc_info=True,
            )
            extracted_data = []  # エラー時は空リストを返す
        finally:
            # KeirinDataAccessorが接続管理をするため、ここでのcloseは不要
            pass

        return extracted_data

    # Step 4 で他に事前にDBから取得しておきたい情報があれば、ここに追加メソッドを定義
    # 例: 既存のオッズデータをチェックするなど
    # def _extract_existing_odds_info(self, race_ids: List[str]) -> Set[str]:
    #     # ... 実装 ...
    #     pass
