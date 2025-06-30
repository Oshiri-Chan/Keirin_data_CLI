"""
競輪データのモデルクラス

このモジュールには、競輪データに関連するデータモデルクラスが含まれています。
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class 開催データ:
    """開催に関するデータクラス"""

    開催キー: str
    開催名称: str
    競輪場コード: str
    競輪場名: str
    開始日: str
    終了日: str
    開催区分コード: str
    主催者: str
    発売フラグ: int
    登録日時: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class 開催日データ:
    """開催日に関するデータクラス"""

    開催キー: str
    開催日付: str
    開催日: int
    登録日時: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class レースデータ:
    """レースに関するデータクラス"""

    レースキー: str
    開催キー: str
    開催日: str
    レース番号: int
    レース名: str
    発走時刻: str
    レース距離: int
    周回数: int
    出走車数: int
    クラス: Optional[str] = None
    レース種別: Optional[str] = None
    レース種別3区分: Optional[str] = None
    グレードレース: bool = False
    ステータス: Optional[str] = None
    天候: Optional[str] = None
    風速: Optional[float] = None
    キャンセル: bool = False
    キャンセル理由: Optional[str] = None
    締切時刻: Optional[int] = None
    確定時刻: Optional[int] = None
    ダイジェスト動画有無: bool = False
    ダイジェスト動画: Optional[str] = None
    ダイジェスト動画プロバイダー: Optional[str] = None
    出走表未確定: bool = False
    登録日時: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class 出走データ:
    """出走に関するデータクラス"""

    レースキー: str
    枠番: int
    選手番号: str
    選手名: str
    年齢: int
    期別: str
    府県: str
    登録日時: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class 並び想定データ:
    """並び想定に関するデータクラス"""

    レースキー: str
    枠番: int
    ライン番号: int
    登録日時: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class オッズデータ:
    """オッズに関するデータクラス"""

    レースキー: str
    式別: str
    組番1: int
    組番2: int
    組番3: int
    倍率: float
    人気順: int
    登録日時: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class レース時刻データ:
    """レース時刻に関するデータクラス"""

    競輪場: str
    開催日付: str
    開催日: int
    レース番号: int
    発走時刻: str
    レース終了時刻: Optional[str] = None
    結果利用可能時刻: Optional[str] = None
    登録日時: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class 結果データ:
    """レース結果に関するデータクラス"""

    レースキー: str
    枠番: int
    着順: int
    登録日時: str = field(default_factory=lambda: datetime.now().isoformat())
