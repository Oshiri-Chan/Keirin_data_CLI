#!/usr/bin/env python
# -*- coding: utf-8 -*-

with open("services/updaters/step2_updater.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

# 問題のあるfor文のあたりの行（90-120行付近）を探す
start_line = 0
end_line = 0
for i, line in enumerate(lines):
    if "if with_parallel and len(cup_ids) > 1 and self.max_workers > 0:" in line:
        start_line = i
    if "time.sleep(self.rate_limit_wait)" in line:
        end_line = i + 1
        break

if start_line > 0 and end_line > start_line:
    # 問題の部分を正しいインデントに修正した内容で置き換え
    correct_code = [
        "        if with_parallel and len(cup_ids) > 1 and self.max_workers > 0:\n",
        '            self.logger.info(f"スレッド {thread_id}: 開催詳細情報の一括取得を並列処理で開始 (最大ワーカー数: {self.max_workers})")\n',
        "            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:\n",
        "                future_to_cup = {executor.submit(self._fetch_cup_detail_worker, cup_id): cup_id for cup_id in cup_ids}\n",
        "                processed_count = 0\n",
        "                for future in as_completed(future_to_cup):\n",
        "                    cup_id_result, detail_data = future.result()\n",
        "                    processed_count += 1\n",
        "                    if detail_data:\n",
        "                        all_api_responses.append(detail_data)\n",
        "                        succeeded_cup_ids.append(cup_id_result)\n",
        "                    else:\n",
        "                        failed_cup_ids.append(cup_id_result)\n",
        "                    self.logger.debug(f\"スレッド {thread_id}: API取得進捗: {processed_count}/{len(cup_ids)} (Cup ID: {cup_id_result}, 結果: {'成功' if detail_data else '失敗'})\")\n",
        "                    if self.max_workers > 0:\n",
        "                        time.sleep(self.rate_limit_wait / self.max_workers)\n",
        "        else:\n",
        '            self.logger.info(f"スレッド {thread_id}: 開催詳細情報の一括取得を順次処理で開始")\n',
        "            for i, cup_id in enumerate(cup_ids):\n",
        "                cup_id_result, detail_data = self._fetch_cup_detail_worker(cup_id)\n",
        "                if detail_data:\n",
        "                    all_api_responses.append(detail_data)\n",
        "                    succeeded_cup_ids.append(cup_id_result)\n",
        "                else:\n",
        "                    failed_cup_ids.append(cup_id_result)\n",
        "                self.logger.debug(f\"スレッド {thread_id}: API取得進捗: {i+1}/{len(cup_ids)} (Cup ID: {cup_id_result}, 結果: {'成功' if detail_data else '失敗'})\")\n",
        "                time.sleep(self.rate_limit_wait)\n",
    ]

    # 元のコードを修正したコードで置き換え
    lines[start_line:end_line] = correct_code

    # race_save_resultの部分も修正（210-220行付近）
    start_line_2 = 0
    end_line_2 = 0
    for i, line in enumerate(lines):
        if "if race_save_result and isinstance(race_save_result, dict):" in line:
            start_line_2 = i
        if "except Exception as e_race_outer:" in line:
            end_line_2 = i
            break

    if start_line_2 > 0 and end_line_2 > start_line_2:
        # 修正するコード
        correct_code_2 = [
            "                    if race_save_result and isinstance(race_save_result, dict):\n",
            '                        saved_count_for_this_batch = race_save_result.get("count", 0)\n',
            "                        total_saved_races_count += saved_count_for_this_batch\n",
            '                        if race_save_result.get("error_details"):\n',
            "                            self.logger.error(f\"スレッド {thread_id}: Cup ID {cup_id_to_process} のレース保存中にSaverからエラー返却: {race_save_result['error_details']}\")\n",
            "                            overall_save_success = False\n",
            "                            # cup_data_save_successful_this_iteration は既にFalseの可能性もあるが、レースで失敗したら明確にFalse\n",
            "                            # ただし、一部成功・一部失敗のケースはSaver側の返り値 count で判断\n",
            "                            if saved_count_for_this_batch < len(current_races_for_saver):\n",
            '                                self.logger.warning(f"Cup ID {cup_id_to_process}: レース保存で一部失敗の可能性 (要求: {len(current_races_for_saver)}, 成功: {saved_count_for_this_batch})")\n',
            "                        else:\n",
            '                            self.logger.info(f"スレッド {thread_id}: Cup ID {cup_id_to_process} のレース {saved_count_for_this_batch}件の保存呼び出し完了。")\n',
            "                    else:\n",
            '                        self.logger.error(f"スレッド {thread_id}: Cup ID {cup_id_to_process} のレース保存の返り値が不正です: {race_save_result}")\n',
            "                        overall_save_success = False\n",
            "                        # この場合も cup_data_save_successful_this_iteration を False にすべきだが、\n",
            "                        # ループの最後にまとめて警告を出すため、ここでは overall_save_success のみ更新\n",
        ]

        # 元のコードを修正したコードで置き換え
        lines[start_line_2:end_line_2] = correct_code_2

    # _safe_int_convertと_safe_float_convertのインデント修正
    for i, line in enumerate(lines):
        if "_safe_int_convert" in line and "def " in line:
            start_line_3 = i + 1
            if "if value is None:" in lines[start_line_3]:
                lines[start_line_3] = "        if value is None: return default\n"
                lines[start_line_3 + 1] = "        try:\n"
                lines[start_line_3 + 2] = "            return int(value)\n"

        if "_safe_float_convert" in line and "def " in line:
            start_line_4 = i + 1
            if "if value is None:" in lines[start_line_4]:
                lines[start_line_4] = "        if value is None: return default\n"
                lines[start_line_4 + 1] = "        try:\n"
                lines[start_line_4 + 2] = "            return float(value)\n"

    # 修正したファイルを書き戻す
    with open("services/updaters/step2_updater.py", "w", encoding="utf-8") as f:
        f.writelines(lines)

    print("step2_updater.pyのインデント修正が完了しました")
else:
    print("step2_updater.pyの修正すべき箇所が見つかりませんでした")
