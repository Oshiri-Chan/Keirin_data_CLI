import logging


class OddsParser:
    """オッズデータをパースするクラス"""

    def __init__(self):
        """初期化"""
        self.logger = logging.getLogger(__name__)

    def parse_quinella_odds(self, odds_data):
        """二車連オッズをパースする

        Args:
            odds_data (dict): APIから取得したオッズデータ

        Returns:
            list: 二車連オッズのリスト
        """
        try:
            if not odds_data or "quinella" not in odds_data:
                return []

            quinella_data = odds_data["quinella"]
            result = []

            for item in quinella_data:
                odds_value = item.get("odds", 0)

                # 欠場等で値が無い場合はスキップ
                if odds_value == 0:
                    continue

                odds_info = {
                    "numbers": f"{item.get('number1', 0)}-{item.get('number2', 0)}",
                    "odds_value": odds_value,
                    "min_odds": item.get("minOdds", odds_value),
                    "max_odds": item.get("maxOdds", odds_value),
                    "popularity_order": item.get("popularityOrder", 0),
                    "unit_price": 100,  # 単位金額は100円固定
                    "payoff_price": int(odds_value * 100),  # 払戻金額は単位金額×オッズ
                    "is_absent": item.get("isAbsent", 0),
                }
                result.append(odds_info)

            return result

        except Exception as e:
            self.logger.error(
                f"二車連オッズのパース中にエラーが発生しました: {e}", exc_info=True
            )
            return []

    def parse_exacta_odds(self, odds_data):
        """二車単オッズをパースする

        Args:
            odds_data (dict): APIから取得したオッズデータ

        Returns:
            list: 二車単オッズのリスト
        """
        try:
            if not odds_data or "exacta" not in odds_data:
                return []

            exacta_data = odds_data["exacta"]
            result = []

            for item in exacta_data:
                odds_value = item.get("odds", 0)

                # 欠場等で値が無い場合はスキップ
                if odds_value == 0:
                    continue

                odds_info = {
                    "numbers": f"{item.get('number1', 0)}-{item.get('number2', 0)}",
                    "odds_value": odds_value,
                    "min_odds": item.get("minOdds", odds_value),
                    "max_odds": item.get("maxOdds", odds_value),
                    "popularity_order": item.get("popularityOrder", 0),
                    "unit_price": 100,
                    "payoff_price": int(odds_value * 100),
                    "is_absent": item.get("isAbsent", 0),
                }
                result.append(odds_info)

            return result

        except Exception as e:
            self.logger.error(
                f"二車単オッズのパース中にエラーが発生しました: {e}", exc_info=True
            )
            return []

    def parse_trio_odds(self, odds_data):
        """三連複オッズをパースする

        Args:
            odds_data (dict): APIから取得したオッズデータ

        Returns:
            list: 三連複オッズのリスト
        """
        try:
            if not odds_data or "trio" not in odds_data:
                return []

            trio_data = odds_data["trio"]
            result = []

            for item in trio_data:
                odds_value = item.get("odds", 0)

                # 欠場等で値が無い場合はスキップ
                if odds_value == 0:
                    continue

                odds_info = {
                    "numbers": f"{item.get('number1', 0)}-{item.get('number2', 0)}-{item.get('number3', 0)}",
                    "odds_value": odds_value,
                    "min_odds": item.get("minOdds", odds_value),
                    "max_odds": item.get("maxOdds", odds_value),
                    "popularity_order": item.get("popularityOrder", 0),
                    "unit_price": 100,
                    "payoff_price": int(odds_value * 100),
                    "is_absent": item.get("isAbsent", 0),
                }
                result.append(odds_info)

            return result

        except Exception as e:
            self.logger.error(
                f"三連複オッズのパース中にエラーが発生しました: {e}", exc_info=True
            )
            return []

    def parse_trifecta_odds(self, odds_data):
        """三連単オッズをパースする

        Args:
            odds_data (dict): APIから取得したオッズデータ

        Returns:
            list: 三連単オッズのリスト
        """
        try:
            if not odds_data or "trifecta" not in odds_data:
                return []

            trifecta_data = odds_data["trifecta"]
            result = []

            for item in trifecta_data:
                odds_value = item.get("odds", 0)

                # 欠場等で値が無い場合はスキップ
                if odds_value == 0:
                    continue

                odds_info = {
                    "numbers": f"{item.get('number1', 0)}-{item.get('number2', 0)}-{item.get('number3', 0)}",
                    "odds_value": odds_value,
                    "min_odds": item.get("minOdds", odds_value),
                    "max_odds": item.get("maxOdds", odds_value),
                    "popularity_order": item.get("popularityOrder", 0),
                    "unit_price": 100,
                    "payoff_price": int(odds_value * 100),
                    "is_absent": item.get("isAbsent", 0),
                }
                result.append(odds_info)

            return result

        except Exception as e:
            self.logger.error(
                f"三連単オッズのパース中にエラーが発生しました: {e}", exc_info=True
            )
            return []

    def parse_quinella_place_odds(self, odds_data):
        """ワイドオッズをパースする

        Args:
            odds_data (dict): APIから取得したオッズデータ

        Returns:
            list: ワイドオッズのリスト
        """
        try:
            if not odds_data or "quinellaPlace" not in odds_data:
                return []

            quinella_place_data = odds_data["quinellaPlace"]
            result = []

            for item in quinella_place_data:
                odds_value = item.get("odds", 0)

                # 欠場等で値が無い場合はスキップ
                if odds_value == 0:
                    continue

                odds_info = {
                    "numbers": f"{item.get('number1', 0)}-{item.get('number2', 0)}",
                    "odds_value": odds_value,
                    "min_odds": item.get("minOdds", odds_value),
                    "max_odds": item.get("maxOdds", odds_value),
                    "popularity_order": item.get("popularityOrder", 0),
                    "unit_price": 100,
                    "payoff_price": int(odds_value * 100),
                    "is_absent": item.get("isAbsent", 0),
                }
                result.append(odds_info)

            return result

        except Exception as e:
            self.logger.error(
                f"ワイドオッズのパース中にエラーが発生しました: {e}", exc_info=True
            )
            return []

    def parse_bracket_quinella_odds(self, odds_data):
        """枠連オッズをパースする

        Args:
            odds_data (dict): APIから取得したオッズデータ

        Returns:
            list: 枠連オッズのリスト
        """
        try:
            if not odds_data or "bracketQuinella" not in odds_data:
                return []

            bracket_quinella_data = odds_data["bracketQuinella"]
            result = []

            for item in bracket_quinella_data:
                odds_value = item.get("odds", 0)

                # 欠場等で値が無い場合はスキップ
                if odds_value == 0:
                    continue

                odds_info = {
                    "brackets": f"{item.get('bracket1', 0)}-{item.get('bracket2', 0)}",
                    "odds_value": odds_value,
                    "min_odds": item.get("minOdds", odds_value),
                    "max_odds": item.get("maxOdds", odds_value),
                    "popularity_order": item.get("popularityOrder", 0),
                    "unit_price": 100,
                    "payoff_price": int(odds_value * 100),
                    "is_absent": item.get("isAbsent", 0),
                }
                result.append(odds_info)

            return result

        except Exception as e:
            self.logger.error(
                f"枠連オッズのパース中にエラーが発生しました: {e}", exc_info=True
            )
            return []

    def parse_bracket_exacta_odds(self, odds_data):
        """枠単オッズをパースする

        Args:
            odds_data (dict): APIから取得したオッズデータ

        Returns:
            list: 枠単オッズのリスト
        """
        try:
            if not odds_data or "bracketExacta" not in odds_data:
                return []

            bracket_exacta_data = odds_data["bracketExacta"]
            result = []

            for item in bracket_exacta_data:
                odds_value = item.get("odds", 0)

                # 欠場等で値が無い場合はスキップ
                if odds_value == 0:
                    continue

                odds_info = {
                    "brackets": f"{item.get('bracket1', 0)}-{item.get('bracket2', 0)}",
                    "odds_value": odds_value,
                    "min_odds": item.get("minOdds", odds_value),
                    "max_odds": item.get("maxOdds", odds_value),
                    "popularity_order": item.get("popularityOrder", 0),
                    "unit_price": 100,
                    "payoff_price": int(odds_value * 100),
                    "is_absent": item.get("isAbsent", 0),
                }
                result.append(odds_info)

            return result

        except Exception as e:
            self.logger.error(
                f"枠単オッズのパース中にエラーが発生しました: {e}", exc_info=True
            )
            return []
