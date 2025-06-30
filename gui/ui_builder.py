"""
UI構築を担当するモジュール
"""

import tkinter as tk
from datetime import datetime, timedelta
from tkinter import scrolledtext, ttk

from tkcalendar import DateEntry


class UIBuilder:
    """
    UI構築を責務とするクラス

    KeirinUpdaterGUIのUI部分を構築する責務を持つ
    """

    def __init__(self, controller):
        """
        初期化

        Args:
            controller: GUIコントローラーオブジェクト
        """
        self.controller = controller
        self.root = controller.root

    def build_ui(self):
        """UIの構築"""
        # メインフレームの作成
        self.main_frame = ttk.Frame(self.root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        # タブコントロールの作成
        self.tab_control = ttk.Notebook(self.main_frame)
        self.tab_control.pack(fill=tk.X, pady=5)

        # 単一日タブ
        self.single_tab = ttk.Frame(self.tab_control)
        self.tab_control.add(self.single_tab, text="単一日更新")

        # 期間更新タブ
        self.period_tab = ttk.Frame(self.tab_control)
        self.tab_control.add(self.period_tab, text="期間更新")

        # 全期間更新タブ
        self.all_tab = ttk.Frame(self.tab_control)
        self.tab_control.add(self.all_tab, text="全期間更新")

        # UIコンポーネントの構築
        self._build_db_section()
        self._build_single_date_section(self.single_tab)
        self._build_period_section(self.period_tab)

        # 更新オプションセクション
        self.update_options_frame = self.controller._create_update_options(
            self.main_frame
        )

        # self._build_auto_update_section() # 古い自動更新セクションは削除
        self._build_status_section()
        self._build_control_section()  # コントロールボタンをログの前に配置
        self._build_log_section()

        # 初期表示状態の設定 (古い自動更新のトグル呼び出しは削除)
        # self.controller._toggle_auto_update()

        # タブコントロールへの参照を保持
        self.controller.tab_control = self.tab_control

        # 初期タブを選択
        self.tab_control.select(0)

    def _build_db_section(self):
        """データベース選択セクションの構築"""
        db_frame = ttk.LabelFrame(self.main_frame, text="データベース設定")
        db_frame.pack(fill=tk.X, pady=5)

        # データベースパスの入力
        path_frame = ttk.Frame(db_frame)
        path_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(path_frame, text="データベースファイル:").pack(side=tk.LEFT, padx=5)

        # パス入力欄
        path_entry = ttk.Entry(
            path_frame, textvariable=self.controller.db_path, width=40
        )
        path_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        # 参照ボタン
        ttk.Button(
            path_frame, text="参照", command=self.controller._browse_db_file
        ).pack(side=tk.LEFT, padx=5)

        # 新規作成ボタン
        ttk.Button(
            path_frame, text="新規作成", command=self.controller._create_new_database
        ).pack(side=tk.LEFT, padx=5)

        return db_frame

    def _build_single_date_section(self, parent):
        """単一日付選択部分の構築"""
        self.single_date_frame = ttk.Frame(parent)
        self.single_date_frame.pack(fill=tk.X, pady=5)

        # 日付入力フレーム
        date_frame = ttk.LabelFrame(self.single_date_frame, text="日付指定")
        date_frame.pack(fill=tk.X, pady=5)

        input_frame = ttk.Frame(date_frame)
        input_frame.pack(fill=tk.X, padx=5, pady=5)

        # 日付入力（YYYYMMDD）
        ttk.Label(input_frame, text="日付 (YYYYMMDD):").pack(side=tk.LEFT, padx=5)

        # 日付入力欄
        self.date_var = tk.StringVar(value=self.controller.target_date.get())
        self.date_entry = ttk.Entry(input_frame, textvariable=self.date_var, width=10)
        self.date_entry.pack(side=tk.LEFT, padx=5)

        # カレンダーボタン
        self.calendar_button = ttk.Button(
            input_frame, text="カレンダー", command=self.controller._show_calendar
        )
        self.calendar_button.pack(side=tk.LEFT, padx=5)

        # クイック選択ボタン
        quick_frame = ttk.Frame(date_frame)
        quick_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(quick_frame, text="クイック選択:").pack(side=tk.LEFT, padx=5)

        quick_btn_frame = ttk.Frame(quick_frame)
        quick_btn_frame.pack(side=tk.LEFT, fill=tk.X, padx=5)

        ttk.Button(
            quick_btn_frame, text="今日", command=lambda: self.controller._set_date(0)
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            quick_btn_frame, text="昨日", command=lambda: self.controller._set_date(1)
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            quick_btn_frame, text="2日前", command=lambda: self.controller._set_date(2)
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            quick_btn_frame, text="3日前", command=lambda: self.controller._set_date(3)
        ).pack(side=tk.LEFT, padx=2)
        ttk.Button(
            quick_btn_frame,
            text="1週間前",
            command=lambda: self.controller._set_date(7),
        ).pack(side=tk.LEFT, padx=2)

        # ★★★ 日付セットボタンを追加 ★★★
        set_date_button = ttk.Button(
            date_frame, text="日付セット", command=self.controller._set_single_date
        )
        set_date_button.pack(side=tk.LEFT, padx=5)

        return self.single_date_frame

    def _build_period_section(self, parent):
        """期間指定部分の構築"""
        self.period_frame = ttk.Frame(parent)
        self.period_frame.pack(fill=tk.X, pady=5)

        period_frame = ttk.LabelFrame(self.period_frame, text="期間指定")
        period_frame.pack(fill=tk.X, pady=5)

        # 開始日
        start_frame = ttk.Frame(period_frame)
        start_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(start_frame, text="開始日:").pack(side=tk.LEFT, padx=5)

        # 開始日の初期値（昨日）
        yesterday = datetime.now() - timedelta(days=1)
        self.controller.start_date = yesterday.date()

        # tkcalendarを使用した日付選択
        self.start_date_entry = DateEntry(
            start_frame,
            width=10,
            background="darkblue",
            foreground="white",
            borderwidth=2,
            date_pattern="yyyy/mm/dd",
            locale="ja_JP",
            year=yesterday.year,
            month=yesterday.month,
            day=yesterday.day,
        )
        self.start_date_entry.pack(side=tk.LEFT, padx=5)

        # 終了日
        end_frame = ttk.Frame(period_frame)
        end_frame.pack(fill=tk.X, padx=5, pady=5)

        ttk.Label(end_frame, text="終了日:").pack(side=tk.LEFT, padx=5)

        # 終了日の初期値（今日）
        today = datetime.now()
        self.controller.end_date = today.date()

        # tkcalendarを使用した日付選択
        self.end_date_entry = DateEntry(
            end_frame,
            width=10,
            background="darkblue",
            foreground="white",
            borderwidth=2,
            date_pattern="yyyy/mm/dd",
            locale="ja_JP",
            year=today.year,
            month=today.month,
            day=today.day,
        )
        self.end_date_entry.pack(side=tk.LEFT, padx=5)

        # "今日まで"のチェックボックス
        self.until_now_checkbox = ttk.Checkbutton(
            end_frame,
            text="今日まで",
            variable=self.controller.until_now,
            command=self.controller._toggle_until_now,
        )
        self.until_now_checkbox.pack(side=tk.LEFT, padx=5)

        # ★★★ 期間セットボタンを追加 ★★★
        set_period_button = ttk.Button(
            period_frame, text="期間セット", command=self.controller._set_period_dates
        )
        # pack する場所を調整 (開始日と終了日の下)
        set_period_button.pack(pady=5)

        return self.period_frame

    def _build_status_section(self):
        """ステータス表示部分の構築"""
        status_frame = ttk.Frame(self.main_frame)
        status_frame.pack(fill=tk.X, pady=5)

        # ステータスラベル
        ttk.Label(status_frame, text="ステータス:").pack(side=tk.LEFT, padx=5)

        # プログレスバー
        self.progress = ttk.Progressbar(status_frame, mode="indeterminate", length=200)
        self.progress.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        self.controller.progress_bar = self.progress

        # ステータステキスト
        self.update_progress_label = ttk.Label(
            status_frame, textvariable=self.controller.status_var
        )
        self.update_progress_label.pack(side=tk.LEFT, padx=5)
        self.controller.update_progress_label = self.update_progress_label

        return status_frame

    def _build_control_section(self):
        """コントロールボタンセクションの構築"""
        control_frame = ttk.Frame(self.main_frame)
        control_frame.pack(fill=tk.X, pady=5)

        # 更新開始ボタン
        self.start_button = ttk.Button(
            control_frame, text="更新開始", command=self.controller._start_update
        )
        self.start_button.pack(side=tk.LEFT, padx=5)

        # キャンセルボタン（初期状態は無効）
        self.cancel_button = ttk.Button(
            control_frame,
            text="キャンセル",
            command=self.controller._cancel_update,
            state=tk.DISABLED,
        )
        self.cancel_button.pack(side=tk.LEFT, padx=5)

        # コンフィグボタン
        self.config_button = ttk.Button(
            control_frame, text="コンフィグ", command=self.controller.open_config_window
        )
        self.config_button.pack(side=tk.LEFT, padx=5)

        # DuckDB更新ボタン
        self.deploy_button = ttk.Button(
            control_frame,
            text="DuckDB更新",
            command=self.controller.run_deploy_to_duckdb,
        )
        self.deploy_button.pack(side=tk.LEFT, padx=5)

        # 手動更新ボタン
        self.manual_update_button = ttk.Button(
            control_frame,
            text="手動更新",
            command=self.controller._show_manual_update_dialog,
        )
        self.manual_update_button.pack(side=tk.LEFT, padx=5)

        # コントローラーがボタンを参照できるようにする
        self.controller.start_button = self.start_button
        self.controller.cancel_button = self.cancel_button
        self.controller.config_button = self.config_button
        self.controller.deploy_button = self.deploy_button
        self.controller.manual_update_button = self.manual_update_button

        return control_frame

    def _build_log_section(self):
        """ログ表示部分の構築"""
        log_frame = ttk.LabelFrame(self.main_frame, text="ログ")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        # ログ表示用テキストエリア
        self.log_text = scrolledtext.ScrolledText(log_frame, height=10, wrap=tk.WORD)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.log_text.config(state=tk.DISABLED)  # 読み取り専用

        return log_frame

    def update_progress(self, is_running, status_text=None):
        """
        プログレスバーとステータスの更新

        Args:
            is_running (bool): 処理中か否か
            status_text (str, optional): ステータステキスト
        """
        if is_running:
            self.progress.start()
            self.start_button.config(state=tk.DISABLED)
            self.cancel_button.config(state=tk.NORMAL)
        else:
            self.progress.stop()
            self.start_button.config(state=tk.NORMAL)
            self.cancel_button.config(state=tk.DISABLED)

        if status_text:
            self.controller.status_var.set(status_text)

    def update_progress_bar(self, progress_value):
        """
        プログレスバーの進捗率を更新

        Args:
            progress_value (int): 進捗率（0-100）
        """
        if not hasattr(self, "progress") or self.progress is None:
            return

        # プログレスバーをdeterminateモードに切り替え
        if self.progress.cget("mode") != "determinate":
            self.progress.stop()
            self.progress.configure(mode="determinate")

        # 進捗率を設定（0-100）
        progress_value = max(0, min(100, progress_value))  # 0-100の範囲に制限
        self.progress.configure(value=progress_value)

    def _on_date_selected(self, event=None):
        """日付選択イベントのハンドラー"""
        try:
            # DateEntryから日付を取得
            if hasattr(self, "date_entry") and self.date_entry:
                selected_date = self.date_entry.get_date()
                if isinstance(selected_date, datetime):
                    selected_date = selected_date.date()
                date_str = selected_date.strftime("%Y%m%d")
                self.controller.target_date.set(date_str)
                self.controller.log_manager.log(f"更新対象日を変更: {date_str}")
        except Exception as e:
            # エラーログ
            if hasattr(self.controller, "log_manager"):
                self.controller.log_manager.log_error(
                    "日付選択処理でエラーが発生しました", e
                )

    def enable_controls(self):
        """UIコントロールを有効化"""
        # 更新ボタンを有効化
        self.start_button.configure(state="normal")

        # キャンセルボタンを無効化
        self.cancel_button.configure(state="disabled")

        # データベース関連のコントロール
        for widget in self.main_frame.winfo_children():
            if (
                isinstance(widget, ttk.LabelFrame)
                and widget.cget("text") == "データベース設定"
            ):
                for child in widget.winfo_children():
                    if isinstance(child, (ttk.Button, ttk.Entry)):
                        child.configure(state="normal")

        # タブコントロールを有効化 - stateの代わりにタブの切り替えを許可
        self.tab_control.bind(
            "<<NotebookTabChanged>>", lambda e: None
        )  # デフォルトのイベントハンドラを削除

        # 日付選択コントロール
        self.date_entry.configure(state="normal")
        self.calendar_button.configure(state="normal")

        # 期間選択コントロール
        self.start_date_entry.configure(state="normal")
        self.end_date_entry.configure(state="normal")
        self.until_now_checkbox.configure(state="normal")

        # プログレスバーの状態を更新
        self.update_progress(False, "準備完了")

    def disable_controls(self):
        """UIコントロールを無効化（更新中）"""
        # 更新ボタンを無効化
        self.start_button.configure(state="disabled")

        # データベース関連のコントロール
        for widget in self.main_frame.winfo_children():
            if (
                isinstance(widget, ttk.LabelFrame)
                and widget.cget("text") == "データベース設定"
            ):
                for child in widget.winfo_children():
                    if isinstance(child, (ttk.Button, ttk.Entry)):
                        child.configure(state="disabled")

        # タブコントロールを無効化 - stateの代わりにタブの切り替えを禁止
        current_tab = self.tab_control.select()
        self.tab_control.bind(
            "<<NotebookTabChanged>>", lambda e: self.tab_control.select(current_tab)
        )

        # 日付選択コントロール
        self.date_entry.configure(state="disabled")
        self.calendar_button.configure(state="disabled")

        # 期間選択コントロール
        self.start_date_entry.configure(state="disabled")
        self.end_date_entry.configure(state="disabled")
        self.until_now_checkbox.configure(state="disabled")

        # プログレスバーの状態を更新
        self.update_progress(True, "更新中...")

    def enable_cancel_button(self):
        """キャンセルボタンを有効化"""
        self.cancel_button.configure(state="normal")
