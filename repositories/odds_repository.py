import logging
import threading
from datetime import datetime

import pandas as pd


class OddsRepository:
    """オッズ情報リポジトリクラス"""

    def __init__(self, database):
        self.database = database
        self.logger = logging.getLogger(__name__)

    def save_quinella_odds(self, race_id, odds_data):
        """二車連オッズデータを保存する

        Args:
            race_id (str): レースID
            odds_data (list): オッズデータのリスト
                [
                    {
                        'numbers': '1-2',
                        'odds_value': 10.5,
                        'min_odds': 9.8,
                        'max_odds': 11.2,
                        'popularity_order': 3,
                        'unit_price': 100,
                        'payoff_price': 1050,
                        'is_absent': 0
                    },
                    ...
                ]

        Returns:
            bool: 保存に成功したかどうか
        """
        try:
            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の二車連オッズを保存します"
            )

            if not odds_data:
                self.logger.info(
                    f"スレッド {threading.current_thread().ident}: race_id {race_id} の二車連オッズデータがありません"
                )
                return True

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for data in odds_data:
                data["race_id"] = race_id
                data["created_at"] = now
                data["updated_at"] = now

            # データフレームに変換
            df = pd.DataFrame(odds_data)

            # データベースに保存
            df.to_sql(
                "payouts_quinella",
                self.database.engine,
                if_exists="append",
                index=False,
            )

            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の二車連オッズを保存しました: {len(odds_data)}件"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の二車連オッズ保存中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False

    def save_exacta_odds(self, race_id, odds_data):
        """二車単オッズデータを保存する

        Args:
            race_id (str): レースID
            odds_data (list): オッズデータのリスト
                [
                    {
                        'numbers': '1-2',
                        'odds_value': 21.3,
                        'min_odds': 19.6,
                        'max_odds': 23.0,
                        'popularity_order': 5,
                        'unit_price': 100,
                        'payoff_price': 2130,
                        'is_absent': 0
                    },
                    ...
                ]

        Returns:
            bool: 保存に成功したかどうか
        """
        try:
            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の二車単オッズを保存します"
            )

            if not odds_data:
                self.logger.info(
                    f"スレッド {threading.current_thread().ident}: race_id {race_id} の二車単オッズデータがありません"
                )
                return True

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for data in odds_data:
                data["race_id"] = race_id
                data["created_at"] = now
                data["updated_at"] = now

            # データフレームに変換
            df = pd.DataFrame(odds_data)

            # データベースに保存
            df.to_sql(
                "payouts_exacta", self.database.engine, if_exists="append", index=False
            )

            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の二車単オッズを保存しました: {len(odds_data)}件"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の二車単オッズ保存中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False

    def save_trio_odds(self, race_id, odds_data):
        """三連複オッズデータを保存する

        Args:
            race_id (str): レースID
            odds_data (list): オッズデータのリスト
                [
                    {
                        'numbers': '1-2-3',
                        'odds_value': 34.5,
                        'min_odds': 32.8,
                        'max_odds': 36.2,
                        'popularity_order': 7,
                        'unit_price': 100,
                        'payoff_price': 3450,
                        'is_absent': 0
                    },
                    ...
                ]

        Returns:
            bool: 保存に成功したかどうか
        """
        try:
            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の三連複オッズを保存します"
            )

            if not odds_data:
                self.logger.info(
                    f"スレッド {threading.current_thread().ident}: race_id {race_id} の三連複オッズデータがありません"
                )
                return True

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for data in odds_data:
                data["race_id"] = race_id
                data["created_at"] = now
                data["updated_at"] = now

            # データフレームに変換
            df = pd.DataFrame(odds_data)

            # データベースに保存
            df.to_sql(
                "payouts_trio", self.database.engine, if_exists="append", index=False
            )

            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の三連複オッズを保存しました: {len(odds_data)}件"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の三連複オッズ保存中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False

    def save_trifecta_odds(self, race_id, odds_data):
        """三連単オッズデータを保存する

        Args:
            race_id (str): レースID
            odds_data (list): オッズデータのリスト
                [
                    {
                        'numbers': '1-2-3',
                        'odds_value': 123.5,
                        'min_odds': 115.8,
                        'max_odds': 130.2,
                        'popularity_order': 15,
                        'unit_price': 100,
                        'payoff_price': 12350,
                        'is_absent': 0
                    },
                    ...
                ]

        Returns:
            bool: 保存に成功したかどうか
        """
        try:
            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の三連単オッズを保存します"
            )

            if not odds_data:
                self.logger.info(
                    f"スレッド {threading.current_thread().ident}: race_id {race_id} の三連単オッズデータがありません"
                )
                return True

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for data in odds_data:
                data["race_id"] = race_id
                data["created_at"] = now
                data["updated_at"] = now

            # データフレームに変換
            df = pd.DataFrame(odds_data)

            # データベースに保存
            df.to_sql(
                "payouts_trifecta",
                self.database.engine,
                if_exists="append",
                index=False,
            )

            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の三連単オッズを保存しました: {len(odds_data)}件"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の三連単オッズ保存中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False

    def save_quinella_place_odds(self, race_id, odds_data):
        """ワイドオッズデータを保存する

        Args:
            race_id (str): レースID
            odds_data (list): オッズデータのリスト
                [
                    {
                        'numbers': '1-2',
                        'odds_value': 5.3,
                        'min_odds': 4.9,
                        'max_odds': 5.7,
                        'popularity_order': 2,
                        'unit_price': 100,
                        'payoff_price': 530,
                        'is_absent': 0
                    },
                    ...
                ]

        Returns:
            bool: 保存に成功したかどうか
        """
        try:
            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} のワイドオッズを保存します"
            )

            if not odds_data:
                self.logger.info(
                    f"スレッド {threading.current_thread().ident}: race_id {race_id} のワイドオッズデータがありません"
                )
                return True

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for data in odds_data:
                data["race_id"] = race_id
                data["created_at"] = now
                data["updated_at"] = now

            # データフレームに変換
            df = pd.DataFrame(odds_data)

            # データベースに保存
            df.to_sql(
                "payouts_quinella_place",
                self.database.engine,
                if_exists="append",
                index=False,
            )

            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} のワイドオッズを保存しました: {len(odds_data)}件"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} のワイドオッズ保存中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False

    def save_bracket_quinella_odds(self, race_id, odds_data):
        """枠連オッズデータを保存する

        Args:
            race_id (str): レースID
            odds_data (list): オッズデータのリスト
                [
                    {
                        'brackets': '1-2',
                        'odds_value': 7.8,
                        'min_odds': 7.2,
                        'max_odds': 8.4,
                        'popularity_order': 3,
                        'unit_price': 100,
                        'payoff_price': 780,
                        'is_absent': 0
                    },
                    ...
                ]

        Returns:
            bool: 保存に成功したかどうか
        """
        try:
            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の枠連オッズを保存します"
            )

            if not odds_data:
                self.logger.info(
                    f"スレッド {threading.current_thread().ident}: race_id {race_id} の枠連オッズデータがありません"
                )
                return True

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for data in odds_data:
                data["race_id"] = race_id
                data["created_at"] = now
                data["updated_at"] = now

            # データフレームに変換
            df = pd.DataFrame(odds_data)

            # データベースに保存
            df.to_sql(
                "payouts_bracket_quinella",
                self.database.engine,
                if_exists="append",
                index=False,
            )

            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の枠連オッズを保存しました: {len(odds_data)}件"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の枠連オッズ保存中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False

    def save_bracket_exacta_odds(self, race_id, odds_data):
        """枠単オッズデータを保存する

        Args:
            race_id (str): レースID
            odds_data (list): オッズデータのリスト
                [
                    {
                        'brackets': '1-2',
                        'odds_value': 15.2,
                        'min_odds': 14.3,
                        'max_odds': 16.1,
                        'popularity_order': 6,
                        'unit_price': 100,
                        'payoff_price': 1520,
                        'is_absent': 0
                    },
                    ...
                ]

        Returns:
            bool: 保存に成功したかどうか
        """
        try:
            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の枠単オッズを保存します"
            )

            if not odds_data:
                self.logger.info(
                    f"スレッド {threading.current_thread().ident}: race_id {race_id} の枠単オッズデータがありません"
                )
                return True

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for data in odds_data:
                data["race_id"] = race_id
                data["created_at"] = now
                data["updated_at"] = now

            # データフレームに変換
            df = pd.DataFrame(odds_data)

            # データベースに保存
            df.to_sql(
                "payouts_bracket_exacta",
                self.database.engine,
                if_exists="append",
                index=False,
            )

            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の枠単オッズを保存しました: {len(odds_data)}件"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} の枠単オッズ保存中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False

    def save_odds_stats(self, race_id, stats_data):
        """オッズ統計情報を保存する

        Args:
            race_id (str): レースID
            stats_data (dict): 統計情報
                {
                    'is_final': 0/1,
                    'payout_status': 0/1/2,
                    'updated_at': unixtime
                }

        Returns:
            bool: 保存に成功したかどうか
        """
        try:
            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} のオッズ統計情報を保存します"
            )

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            stats_data["race_id"] = race_id
            stats_data["created_at"] = now
            stats_data["updated_at"] = now

            # データフレームに変換
            df = pd.DataFrame([stats_data])

            # データベースに保存
            df.to_sql(
                "odds_stats", self.database.engine, if_exists="append", index=False
            )

            self.logger.info(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} のオッズ統計情報を保存しました"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"スレッド {threading.current_thread().ident}: race_id {race_id} のオッズ統計情報保存中にエラーが発生しました: {e}",
                exc_info=True,
            )
            return False
