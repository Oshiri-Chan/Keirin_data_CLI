import json
import logging
import threading


class WinticketOddsAPI:
    """WinticketのオッズAPIにアクセスするクラス"""

    # APIのベースURL
    BASE_URL = "https://www.winticket.jp/api/v1/keirin/odds"

    def __init__(self, request_manager):
        """初期化

        Args:
            request_manager: リクエストマネージャーオブジェクト
        """
        self.request_manager = request_manager
        self.logger = logging.getLogger(__name__)

    def get_odds_data(self, cup_id, day_index, race_number):
        """オッズデータを取得する

        Args:
            cup_id (str): 開催ID
            day_index (int): 開催日インデックス
            race_number (int): レース番号

        Returns:
            str: オッズデータのJSON文字列
        """
        try:
            self.logger.info(
                f"スレッド {threading.current_thread().ident}: オッズ情報の取得を開始します（cup_id: {cup_id}, day: {day_index}, race: {race_number}）"
            )

            # URLを構築
            url = f"{self.BASE_URL}?cup_id={cup_id}&day={day_index}&race={race_number}"

            # リクエスト送信
            response = self.request_manager.get(url)

            # レスポンスチェック
            if not response:
                self.logger.error(
                    f"スレッド {threading.current_thread().ident}: オッズデータの取得に失敗しました（cup_id: {cup_id}, day: {day_index}, race: {race_number}）"
                )
                return None

            if response.status_code != 200:
                self.logger.error(
                    f"スレッド {threading.current_thread().ident}: オッズデータの取得に失敗しました（cup_id: {cup_id}, day: {day_index}, race: {race_number}）: ステータスコード {response.status_code}"
                )
                return None

            # レスポンスボディを取得
            response_json = response.text

            # デバッグ情報を記録
            try:
                json_data = json.loads(response_json)
                ticket_types_count = []

                if "quinella" in json_data:
                    ticket_types_count.append(f"二車連: {len(json_data['quinella'])}件")

                if "exacta" in json_data:
                    ticket_types_count.append(f"二車単: {len(json_data['exacta'])}件")

                if "trio" in json_data:
                    ticket_types_count.append(f"三連複: {len(json_data['trio'])}件")

                if "trifecta" in json_data:
                    ticket_types_count.append(f"三連単: {len(json_data['trifecta'])}件")

                if "quinellaPlace" in json_data:
                    ticket_types_count.append(
                        f"ワイド: {len(json_data['quinellaPlace'])}件"
                    )

                if "bracketQuinella" in json_data:
                    ticket_types_count.append(
                        f"枠連: {len(json_data['bracketQuinella'])}件"
                    )

                if "bracketExacta" in json_data:
                    ticket_types_count.append(
                        f"枠単: {len(json_data['bracketExacta'])}件"
                    )

                self.logger.info(
                    f"スレッド {threading.current_thread().ident}: オッズ情報を取得しました（cup_id: {cup_id}, day: {day_index}, race: {race_number}）: {', '.join(ticket_types_count)}"
                )

            except json.JSONDecodeError:
                self.logger.error(
                    f"スレッド {threading.current_thread().ident}: JSONのパースに失敗しました（cup_id: {cup_id}, day: {day_index}, race: {race_number}）"
                )

            # レスポンスを返す
            return response_json

        except Exception as e:
            self.logger.error(
                f"スレッド {threading.current_thread().ident}: オッズデータの取得中にエラーが発生しました（cup_id: {cup_id}, day: {day_index}, race: {race_number}）: {e}",
                exc_info=True,
            )
            return None
