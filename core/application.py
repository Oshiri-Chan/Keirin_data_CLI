#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
競輪データ更新ツールのコアアプリケーションモジュール
"""

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


class KeirinUpdaterCore:
    """
    競輪データ更新ツールのコアクラス
    アプリケーションのメインロジックを担当
    """

    def __init__(self):
        """初期化"""
        self.base_dir = Path(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        )
        self.data_dir = self.base_dir / "data"
        self.config_dir = self.base_dir / "config"

        # 必要なディレクトリの作成
        self._ensure_directories()

        logger.info("KeirinUpdaterCoreが初期化されました")

    def _ensure_directories(self):
        """必要なディレクトリの存在を確認し、なければ作成する"""
        for directory in [self.data_dir, self.config_dir]:
            if not directory.exists():
                directory.mkdir(parents=True)
                logger.info(f"ディレクトリを作成しました: {directory}")

    def startup(self):
        """アプリケーションの起動処理"""
        logger.info("アプリケーションコアを起動しています...")
        # ここに起動時の初期化処理を追加

    def shutdown(self):
        """アプリケーションの終了処理"""
        logger.info("アプリケーションコアをシャットダウンしています...")
        # ここに終了時のクリーンアップ処理を追加
