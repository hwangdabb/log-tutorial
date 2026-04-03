# ================================================================
# 생성 모델: openai/gpt-5.4
# 응답 시간: 47.77s
# 입력 토큰: 6425
# 출력 토큰: 4044
# ================================================================

import os
import re
import numpy as np
import pandas as pd


# Step 1: Data loading, multi-country handling, item selection, and event_type normalization
def step1_load_and_select(
    input_dir,
    country_codes,
    target_booklet,
    target_item
):
    print("Step 1: Data Loading and Item Selection")
    before_rows = 0
    loaded_frames = []

    # Load one or more country files and prepend country code to SEQID for uniqueness in multi-country mode
    for country_code in country_codes:
        file_path = os.path.join(input_dir, f"{country_code}_logdata.txt")
        temp_df = pd.read_csv(file_path, sep="\t")
        before_rows += len(temp_df)

        temp_df = temp_df.copy()
        if len(country_codes) > 1:
            temp_df["SEQID"] = temp_df["SEQID"].astype(str).apply(lambda x: f"{country_code}_{x}")
        else:
            temp_df["SEQID"] = temp_df["SEQID"]

        loaded_frames.append(temp_df)

    # Concatenate all loaded country data
    if len(loaded_frames) == 0:
        combined_df = pd.DataFrame(
            columns=["CNTRYID", "SEQID", "booklet_id", "item_id", "event_name", "event_type", "timestamp", "event_description"]
        )
    else:
        combined_df = pd.concat(loaded_frames, ignore_index=True)

    # Filter to the selected booklet and item only
    filtered_df = combined_df[
        (combined_df["booklet_id"] == target_booklet) &
        (combined_df["item_id"] == target_item)
    ].copy()

    # Normalize event_type to lowercase after filtering
    filtered_df["event_type"] = filtered_df["event_type"].astype(str).str.lower()

    after_rows = len(filtered_df)
    print(f"Rows before Step 1: {before_rows}")
    print(f"Rows after Step 1: {after_rows}")
    return filtered_df


# Step 2: Restart event timestamp correction
def step2_restart_correction(input_df):
    print("Step 2: Restart Event Preprocessing")
    before_rows = len(input_df)
    df = input_df.copy()

    processed_groups = []

    # Process each SEQID separately so restart correction does not cross respondents
    for seq_value, seq_group in df.groupby("SEQID", sort=False):
        seq_group = seq_group.copy()
        seq_group = seq_group.reset_index(drop=True)

        original_timestamps = seq_group["timestamp"].tolist()
        adjusted_timestamps = list(original_timestamps)
        event_types = seq_group["event_type"].tolist()

        restart_positions = []
        for pos in range(len(seq_group)):
            if event_types[pos] == "restart":
                restart_positions.append(pos)

        # Apply restart corrections in order so chained restarts use already-adjusted previous timestamps
        for restart_idx in restart_positions:
            if restart_idx == 0 or restart_idx == len(seq_group) - 1:
                continue

            prev_timestamp = adjusted_timestamps[restart_idx - 1]
            next_original_timestamp = original_timestamps[restart_idx + 1]
            restart_timestamp = prev_timestamp + round(next_original_timestamp / 2)

            adjusted_timestamps[restart_idx] = restart_timestamp

            end_idx = len(seq_group) - 1
            scan_idx = restart_idx + 1
            while scan_idx < len(seq_group):
                if event_types[scan_idx] == "end":
                    end_idx = scan_idx
                    break
                scan_idx += 1

            apply_idx = restart_idx + 1
            while apply_idx <= end_idx:
                adjusted_timestamps[apply_idx] = restart_timestamp + original_timestamps[apply_idx]
                apply_idx += 1

        seq_group["timestamp"] = adjusted_timestamps
        processed_groups.append(seq_group)

    if len(processed_groups) == 0:
        result_df = df.copy()
    else:
        result_df = pd.concat(processed_groups, ignore_index=True)

    after_rows = len(result_df)
    print(f"Rows before Step 2: {before_rows}")
    print(f"Rows after Step 2: {after_rows}")
    return result_df


# Step 3: Remove simple ancillary rows and merge predefined multi-row blocks
def step3_multiple_log_entries(
    input_df,
    remove_list=None,
    merge_blocks=None
):
    print("Step 3: Multiple Log Entries Handling")
    before_rows = len(input_df)
    df = input_df.copy()

    # Assign default simple removal list when omitted
    if remove_list is None:
        remove_list = [
            "doaction", "next_inquiry", "next_button", "confirmation_opened",
            "confirmation_closed", "environment", "mc_help_toolbar", "mail_sent",
            "mail_deleted", "mc_help_menuitem", "sort_menu", "copy", "paste",
            "new_folder", "mc_sort", "delete_folder"
        ]

    # Assign default merge block definitions when omitted
    if merge_blocks is None:
        merge_blocks = [
            {"sequence": ["folder_viewed", "mail_drop", "mail_moved"], "represent": "mail_drop"},
            {"sequence": ["folder_viewed", "mail_moved", "mail_drop"], "represent": "mail_drop"},
            {"sequence": ["button", "mail_moved"], "represent": "button"},
            {"sequence": ["button", "next_item", "end"], "represent": "button"},
            {"sequence": ["breakoff", "end"], "represent": "breakoff"},
        ]

    # Step 3A: Remove ancillary rows with no analytical meaning
    df_a = df[~df["event_type"].isin(remove_list)].copy()
    df_a = df_a.reset_index(drop=True)

    # Sort merge patterns by length descending for greedy longest-first matching
    sorted_blocks = sorted(merge_blocks, key=lambda x: len(x["sequence"]), reverse=True)

    merged_rows = []

    # Step 3B: Perform left-to-right greedy non-overlapping block merging within each SEQID
    for seq_value, seq_group in df_a.groupby("SEQID", sort=False):
        seq_group = seq_group.copy()
        seq_group = seq_group.reset_index(drop=True)

        pos = 0
        while pos < len(seq_group):
            matched = False

            for block in sorted_blocks:
                seq_pattern = block["sequence"]
                rep_event = block["represent"]
                pattern_len = len(seq_pattern)

                if pos + pattern_len - 1 >= len(seq_group):
                    continue

                window_types = []
                check_idx = pos
                while check_idx < pos + pattern_len:
                    window_types.append(seq_group.iloc[check_idx]["event_type"])
                    check_idx += 1

                if window_types == seq_pattern:
                    rep_relative_idx = None
                    rep_search_idx = 0
                    while rep_search_idx < pattern_len:
                        if seq_pattern[rep_search_idx] == rep_event:
                            rep_relative_idx = rep_search_idx
                            break
                        rep_search_idx += 1

                    if rep_relative_idx is None:
                        rep_relative_idx = 0

                    representative_row = seq_group.iloc[pos + rep_relative_idx].copy()
                    merged_rows.append(representative_row)
                    pos += pattern_len
                    matched = True
                    break

            if not matched:
                merged_rows.append(seq_group.iloc[pos].copy())
                pos += 1

    if len(merged_rows) == 0:
        result_df = df_a.copy().iloc[0:0].copy()
    else:
        result_df = pd.DataFrame(merged_rows).reset_index(drop=True)

    after_rows = len(result_df)
    print(f"Rows before Step 3: {before_rows}")
    print(f"Rows after Step 3: {after_rows}")
    return result_df


# Step 4: Remove complete duplicates with optional exclusion of keypress rows
def step4_remove_duplicates(input_df, exclude_keypress=True):
    print("Step 4: Duplicate Action Handling")
    before_rows = len(input_df)
    df = input_df.copy()

    if exclude_keypress:
        # Separate keypress rows so only non-keypress rows are deduplicated
        keypress_df = df[df["event_type"] == "keypress"].copy()
        non_keypress_df = df[df["event_type"] != "keypress"].copy()

        dedup_non_keypress = non_keypress_df.drop_duplicates().copy()

        # Recombine and preserve the current order using original indices
        recombined = pd.concat([dedup_non_keypress, keypress_df], axis=0)
        result_df = recombined.sort_index(kind="mergesort").reset_index(drop=True)
    else:
        # Deduplicate all rows including keypress rows
        result_df = df.drop_duplicates().reset_index(drop=True)

    after_rows = len(result_df)
    print(f"Rows before Step 4: {before_rows}")
    print(f"Rows after Step 4: {after_rows}")
    return result_df


# Step 5: Sort rows by timestamp within each SEQID using stable sorting
def step5_time_reversal_handling(input_df):
    print("Step 5: Time Reversal Handling")
    before_rows = len(input_df)
    df = input_df.copy()

    sorted_groups = []

    # Sort within each SEQID by timestamp while preserving equal-timestamp order
    for seq_value, seq_group in df.groupby("SEQID", sort=False):
        seq_group = seq_group.copy()
        seq_group = seq_group.sort_values(by="timestamp", kind="mergesort").reset_index(drop=True)
        sorted_groups.append(seq_group)

    if len(sorted_groups) == 0:
        result_df = df.copy()
    else:
        result_df = pd.concat(sorted_groups, ignore_index=True)

    after_rows = len(result_df)
    print(f"Rows before Step 5: {before_rows}")
    print(f"Rows after Step 5: {after_rows}")
    return result_df


# Step 6: Aggregate consecutive keypress rows into a single count row per block
def step6_keypress_aggregation(input_df):
    print("Step 6: Keypress Aggregation")
    before_rows = len(input_df)
    df = input_df.copy()

    processed_rows = []

    # Process each SEQID separately so keypress blocks do not cross respondents
    for seq_value, seq_group in df.groupby("SEQID", sort=False):
        seq_group = seq_group.copy()
        seq_group = seq_group.reset_index(drop=True)

        idx = 0
        while idx < len(seq_group):
            current_type = seq_group.iloc[idx]["event_type"]

            if current_type == "keypress":
                block_start = idx
                block_end = idx

                while block_end + 1 < len(seq_group) and seq_group.iloc[block_end + 1]["event_type"] == "keypress":
                    block_end += 1

                block_size = block_end - block_start + 1
                first_row = seq_group.iloc[block_start].copy()
                first_row["event_description"] = f"count={block_size}"
                processed_rows.append(first_row)

                idx = block_end + 1
            else:
                processed_rows.append(seq_group.iloc[idx].copy())
                idx += 1

    if len(processed_rows) == 0:
        result_df = df.copy().iloc[0:0].copy()
    else:
        result_df = pd.DataFrame(processed_rows).reset_index(drop=True)

    after_rows = len(result_df)
    print(f"Rows before Step 6: {before_rows}")
    print(f"Rows after Step 6: {after_rows}")
    return result_df


# Step 7: Parse event_description, remove unwanted components, and strip item prefix
def step7_parse_event_description(
    input_df,
    target_booklet,
    target_item,
    desc_remove_list=None,
    remove_details=None
):
    print("Step 7: Event Description Parsing and Filtering")
    before_rows = len(input_df)
    df = input_df.copy()

    # Assign default description removal list when omitted
    if desc_remove_list is None:
        desc_remove_list = ["test_time", "end", "value"]

    # Assign default detail removal list when omitted
    if remove_details is None:
        remove_details = ["nan", ",", "."]

    # Prefix mapping used to remove item-specific prefixes from values
    prefix_map = {
        "PS1_1": "u01a", "PS1_2": "u01b", "PS1_3": "u03a",
        "PS1_4": "u06a", "PS1_5": "u06b", "PS1_6": "u21",
        "PS1_7": "u04a", "PS2_1": "u19a", "PS2_2": "u19b",
        "PS2_3": "u07", "PS2_4": "u02", "PS2_5": "u16",
        "PS2_6": "u11b", "PS2_7": "u23"
    }

    prefix_key = f"{target_booklet}_{target_item}"
    item_prefix = prefix_map.get(prefix_key, "")

    parsed_descriptions = []

    # Parse each event_description row-by-row according to the specified rules
    for _, row in df.iterrows():
        raw_value = row["event_description"]

        if pd.isna(raw_value):
            parsed_descriptions.append(raw_value)
            continue

        lowered = str(raw_value).lower()
        elements = re.split(r"[\|\*\$]", lowered)

        cleaned_elements = []
        for element in elements:
            stripped = element.strip()
            if stripped != "":
                cleaned_elements.append(stripped)

        kept_components = []

        for element in cleaned_elements:
            if "=" in element:
                parts = element.split("=", 1)
                part1 = parts[0].strip()
                part2 = parts[1].strip()

                if part1 in desc_remove_list:
                    continue

                if item_prefix != "":
                    part2 = re.sub(rf"^{re.escape(item_prefix)}_", "", part2)

                kept_components.append(part2)
            elif element == "end":
                continue
            else:
                should_remove = False
                for detail in remove_details:
                    if detail in element:
                        should_remove = True
                        break

                if should_remove:
                    continue

                kept_components.append(element)

        parsed_descriptions.append("_".join(kept_components))

    df["event_description"] = parsed_descriptions

    after_rows = len(df)
    print(f"Rows before Step 7: {before_rows}")
    print(f"Rows after Step 7: {after_rows}")
    return df


# Step 8: Create action_event and export CSV
def step8_merge_and_export(
    input_df,
    output_dir,
    country_codes
):
    print("Step 8: Merge Column and File Export")
    before_rows = len(input_df)
    df = input_df.copy()

    action_event_values = []

    # Build action_event from event_type and parsed event_description
    for _, row in df.iterrows():
        event_type_value = row["event_type"]
        desc_value = row["event_description"]

        if pd.isna(desc_value):
            action_event_values.append(event_type_value)
            continue

        desc_str = str(desc_value)

        if desc_str == "":
            action_event_values.append(event_type_value)
            continue

        split_elements = re.split(r"[\$\|\*]", desc_str)
        extracted_parts = []

        for element in split_elements:
            stripped = element.strip()
            if stripped == "":
                continue

            if "=" in stripped:
                part2 = stripped.split("=", 1)[1].strip()
                extracted_parts.append(part2)
            else:
                extracted_parts.append(stripped)

        joined_part = "_".join(extracted_parts)

        if joined_part == "":
            action_event_values.append(event_type_value)
        else:
            action_event_values.append(f"{event_type_value}_{joined_part}")

    df["action_event"] = action_event_values

    # Export required columns only
    export_df = df[["SEQID", "event_type", "event_description", "action_event", "timestamp"]].copy()

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_country_code = country_codes[0] if len(country_codes) == 1 else "MULTI"
    output_file = os.path.join(output_dir, f"{output_country_code}_processed_logdata.csv")
    export_df.to_csv(output_file, index=False)

    after_rows = len(export_df)
    print(f"Rows before Step 8: {before_rows}")
    print(f"Rows after Step 8: {after_rows}")
    print(f"Exported file: {output_file}")
    return export_df


# Main orchestration function executing Steps 1 through 8 in exact order
def preprocess_piaac(
    input_dir,
    output_dir,
    country_codes,
    target_booklet,
    target_item,
    exclude_keypress=True,
    remove_list=None,
    merge_blocks=None,
    desc_remove_list=None,
    remove_details=None
):
    # Step 1
    df1 = step1_load_and_select(
        input_dir=input_dir,
        country_codes=country_codes,
        target_booklet=target_booklet,
        target_item=target_item
    )

    # Step 2
    df2 = step2_restart_correction(df1)

    # Step 3
    df3 = step3_multiple_log_entries(
        input_df=df2,
        remove_list=remove_list,
        merge_blocks=merge_blocks
    )

    # Step 4
    df4 = step4_remove_duplicates(
        input_df=df3,
        exclude_keypress=exclude_keypress
    )

    # Step 5
    df5 = step5_time_reversal_handling(df4)

    # Step 6
    df6 = step6_keypress_aggregation(df5)

    # Step 7
    df7 = step7_parse_event_description(
        input_df=df6,
        target_booklet=target_booklet,
        target_item=target_item,
        desc_remove_list=desc_remove_list,
        remove_details=remove_details
    )

    # Step 8
    df8 = step8_merge_and_export(
        input_df=df7,
        output_dir=output_dir,
        country_codes=country_codes
    )

    return df8


if __name__ == "__main__":
    # Example execution parameters; adjust as needed for the local environment
    final_df = preprocess_piaac(
        input_dir=".",
        output_dir=".",
        country_codes=["US"],
        target_booklet="PS1",
        target_item=1,
        exclude_keypress=False,
        remove_list=None,
        merge_blocks=None,
        desc_remove_list=None,
        remove_details=None
    )

    print("Preprocessing completed.")
    print(final_df.head())
