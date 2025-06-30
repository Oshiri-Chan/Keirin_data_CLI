# 競輪データ更新ツール プロジェクト概要

## プロジェクト構成

本プロジェクトは以下のような構成になっています：

```
.
├── main.py                  # メインスクリプト
├── core/
│   └── application.py       # アプリケーションコア
├── gui/
│   ├── keirin_updater_gui.py # GUIメインクラス
│   ├── log_manager.py       # ログ管理
│   ├── ui_builder.py        # UI構築
│   └── update_manager.py    # 更新処理管理
├── database/
│   ├── keirin_database.py   # データベース操作
│   └── db_initializer.py    # DB初期化
├── api/
│   ├── winticket_api.py     # Winticket API
│   └── yenjoy_api.py        # Yenjoy API
├── services/
│   └── update_service.py    # 更新サービス
├── utils/
│   └── logger.py            # ロギングユーティリティ
└── config.py                # 設定管理
```

## 主要コンポーネントの説明

### 1. メイン処理 (main.py)

アプリケーションのエントリーポイントです。以下の処理を行います：
- ロギングの初期化
- アプリケーションコアの初期化
- GUIの起動
- 例外処理

### 2. アプリケーションコア (core/application.py)

アプリケーションの中核となるコンポーネントで、以下の責務を持ちます：
- 必要なディレクトリの作成
- 起動処理と終了処理の管理
- 基本設定の管理

### 3. GUI (gui/keirin_updater_gui.py)

ユーザーインターフェースを提供するコンポーネントです：
- Tkinterを利用したGUI表示
- ユーザー操作の処理
- 更新処理の制御
- 設定の保存と読み込み

### 4. ロギング (utils/logger.py, gui/log_manager.py)

アプリケーションのログ管理を行います：
- コンソールとファイルへのログ出力
- GUI上でのログ表示
- エラー処理のサポート

### 5. データベース (database/keirin_database.py)

データ保存と取得を担当します：
- SQLiteデータベースの操作
- テーブルの作成と管理
- データのCRUD操作

### 6. API連携 (api/winticket_api.py, api/yenjoy_api.py)

外部サービスとの連携を行います：
- Winticketからのデータ取得
- Yenjoyからのデータ取得
- レート制限の管理

### 7. 更新サービス (services/update_service.py)

データ更新の具体的なロジックを実装します：
- 各種データの取得と保存
- 更新状態の管理
- エラーハンドリング

## 起動フロー

1. main.pyが実行されると、まずlogger.pyのsetup_application_logger()が呼び出されログ環境が整います
2. KeirinUpdaterCoreが初期化され、必要なディレクトリの存在確認と作成が行われます
3. app_core.startup()が呼び出され、アプリケーションの起動処理が実行されます
4. GUIモジュールがインポートされ、tkinterのルートウィンドウが作成されます
5. KeirinUpdaterGUIクラスのインスタンスが作成され、GUIの初期化が行われます
   - LogManagerの初期化
   - 変数の初期化
   - 各種マネージャーの初期化
   - 設定ファイルの読み込み
   - UIの構築
6. root.mainloop()が呼び出され、GUIのイベントループが開始します
7. アプリケーション終了時にはapp_core.shutdown()が呼び出され、終了処理が実行されます

詳細なフロー図は以下のファイルを参照してください：
- flowchart.md: 起動からGUI表示までのフローチャート
- component_flowchart.md: コンポーネント間の関係図
- sequence_diagram.md: 処理のシーケンス図 