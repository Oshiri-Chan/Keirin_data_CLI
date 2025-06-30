"""
日付ユーティリティ

このモジュールには、日付操作に関連するユーティリティ関数が含まれています。
"""

from datetime import datetime, timedelta
from typing import List, Tuple


def get_yesterday() -> str:
    """
    昨日の日付を「YYYYMMDD」形式で取得する

    Returns:
        str: 昨日の日付（YYYYMMDD形式）
    """
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y%m%d")


def get_date_range(start_date_str: str, end_date_str: str) -> List[str]:
    """
    開始日から終了日までの日付リストを取得する

    Args:
        start_date_str: 開始日（YYYYMMDD形式）
        end_date_str: 終了日（YYYYMMDD形式）

    Returns:
        List[str]: 日付リスト（YYYYMMDD形式）
    """
    # 日付文字列をdatetimeオブジェクトに変換
    start_date = datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.strptime(end_date_str, "%Y%m%d")

    # 日付範囲の作成
    date_range = []
    current_date = start_date
    while current_date <= end_date:
        date_range.append(current_date.strftime("%Y%m%d"))
        current_date += timedelta(days=1)

    return date_range


def parse_date(date_str: str) -> datetime:
    """
    日付文字列をdatetimeオブジェクトに変換する

    Args:
        date_str: 日付文字列（YYYYMMDD形式）

    Returns:
        datetime: 変換されたdatetimeオブジェクト
    """
    return datetime.strptime(date_str, "%Y%m%d")


def format_date(date: datetime) -> str:
    """
    datetimeオブジェクトを日付文字列に変換する

    Args:
        date: datetimeオブジェクト

    Returns:
        str: 変換された日付文字列（YYYYMMDD形式）
    """
    return date.strftime("%Y%m%d")


def format_date_display(date_str: str) -> str:
    """
    日付文字列を表示用に整形する

    Args:
        date_str: 日付文字列（YYYYMMDD形式）

    Returns:
        str: 表示用に整形された日付文字列（YYYY/MM/DD形式）
    """
    date = parse_date(date_str)
    return date.strftime("%Y/%m/%d")


def is_valid_date_range(start_date_str: str, end_date_str: str) -> Tuple[bool, str]:
    """
    日付範囲が有効かどうかを検証する

    Args:
        start_date_str: 開始日（YYYYMMDD形式）
        end_date_str: 終了日（YYYYMMDD形式）

    Returns:
        Tuple[bool, str]: 検証結果と、エラーがある場合はエラーメッセージ
    """
    try:
        # 日付文字列をdatetimeオブジェクトに変換
        start_date = parse_date(start_date_str)
        end_date = parse_date(end_date_str)

        # 開始日が終了日より後の場合はエラー
        if start_date > end_date:
            return False, "開始日が終了日より後になっています"

        # 日付範囲が1年を超える場合は警告
        if (end_date - start_date).days > 365:
            return False, "日付範囲が1年を超えています"

        return True, ""
    except ValueError:
        return False, "日付形式が正しくありません（YYYYMMDD形式で入力してください）"
