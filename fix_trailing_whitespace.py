"""
行末の空白を削除するユーティリティスクリプト
"""

import os
import re
import sys


def fix_trailing_whitespace(file_path):
    """ファイル内の行末の空白を削除する"""
    print(f"Processing file: {file_path}")

    # ファイルの内容を読み込む
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # 行末の空白を削除
    fixed_content = re.sub(r"[ \t]+$", "", content, flags=re.MULTILINE)

    # 変更があればファイルを更新
    if content != fixed_content:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(fixed_content)
        print(f"Fixed trailing whitespace in {file_path}")
    else:
        print(f"No trailing whitespace found in {file_path}")


def main():
    """メイン処理"""
    if len(sys.argv) < 2:
        print("Usage: python fix_trailing_whitespace.py <file_path>")
        sys.exit(1)

    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        sys.exit(1)

    fix_trailing_whitespace(file_path)
    print("Done.")


if __name__ == "__main__":
    main()
