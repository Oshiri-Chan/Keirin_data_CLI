"""
ロギングユーティリティ

このモジュールには、アプリケーション全体で使用されるロギング関連のユーティリティ関数が含まれています。
"""

import logging
import os
import sys
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    console: bool = True,
    file_log: bool = True,
) -> logging.Logger:
    """
    ロガーをセットアップする

    Args:
        name: ロガー名
        log_file: ログファイルパス（Noneの場合はデフォルトの'logs/{name}.log'が使用される）
        level: ログレベル
        console: コンソール出力を有効にするかどうか
        file_log: ファイル出力を有効にするかどうか

    Returns:
        logging.Logger: 設定済みのロガー
    """
    # ロガーの取得
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 既存のハンドラをクリア
    logger.handlers = []

    # フォーマッタの作成
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )

    # ファイルハンドラの設定
    if file_log:
        # ログファイルパスの決定
        if log_file is None:
            log_dir = Path("logs")
            log_dir.mkdir(exist_ok=True)
            log_file = log_dir / f"{name}.log"
        else:
            log_dir = Path(os.path.dirname(log_file))
            log_dir.mkdir(exist_ok=True)

        # ファイルハンドラの追加
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # コンソールハンドラの設定
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def setup_application_logger() -> logging.Logger:
    """
    アプリケーション全体のロガーをセットアップする

    Returns:
        logging.Logger: 設定済みのアプリケーションロガー
    """
    # ログディレクトリの確保
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # ルートロガーの設定
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # 既存のハンドラをクリア
    logger.handlers = []

    # ファイルハンドラ
    file_handler = logging.FileHandler(log_dir / "application.log", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    # コンソールハンドラ
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    console_handler.setFormatter(console_formatter)

    # ハンドラの追加
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger
