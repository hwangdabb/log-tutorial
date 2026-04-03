# ================================================================
# 생성 모델: anthropic/claude-sonnet-4.6
# 응답 시간: 125.86s
# 입력 토큰: 7408
# 출력 토큰: 10926
# ================================================================

import pandas as pd
import re
import os
import numpy as np


# =============================================================================
# Step 1: Data Loading and Item Selection
# =============================================================================
def step1_load_and_filter(input_dir, country_codes, target_booklet, target_item):
    """
    Load PIAAC log data for one or more countries, prepend country code to SEQID
    for multi-country uniqueness, filter to the specified booklet and item,
    and normalize event_type to lowercase.
    """
    dfs = []
    for code in country_codes:
        filepath = os.path.join(input_dir, f"{code}_logdata.txt")
        df_country = pd.read_csv(filepath, sep='\t')
        print(f"  [Step 1] Loaded '{code}_logdata.txt': {len(df_country)} rows")

        # Prepend country code to SEQID for unique identification across countries
        if len(country_codes) > 1:
            df_country = df_country.copy()
            df_country['SEQID'] = code + '_' + df_country['SEQID'].astype(str)
        dfs.append(df_country)

    # Concatenate all country DataFrames
    df_all = pd.concat(dfs, ignore_index=True)
    print(f"[Step 1] Total rows after loading: {len(df_all)}")

    # Filter to the specified booklet and item
    df_filtered = df_all[
        (df_all['booklet_id'] == target_booklet) &
        (df_all['item_id'] == target_item)
    ].copy()
    print(f"[Step 1] Rows after filtering booklet='{target_booklet}', item={target_item}: {len(df_filtered)}")

    # Normalize event_type to lowercase
    df_filtered['event_type'] = df_filtered['event_type'].str.lower()

    print(f"[Step 1] Output rows: {len(df_filtered)}")
    return df_filtered


# =============================================================================
# Step 2: Restart Event Preprocessing
# =============================================================================
def step2_restart_correction(df):
    """
    Correct timestamps after 'restart' events within each SEQID.
    The restart row's timestamp is set to prev_timestamp + round(next_original / 2),
    and all subsequent rows up to (and including) the next 'end' event are offset
    by restart_timestamp.
    """
    print(f"[Step 2] Input rows: {len(df)}")
    df_out = df.copy()

    # Work on each SEQID group independently
    # We'll build a mapping from df_out index -> adjusted timestamp
    # Initialize adjusted timestamps from original timestamps
    adjusted_timestamps = df_out['timestamp'].values.copy().astype(float)

    # Process each SEQID group
    for seqid_val, group in df_out.groupby('SEQID', sort=False):
        group_indices = group.index.tolist()
        n = len(group_indices)

        # Get original timestamps for this group (from df_out, which is a copy)
        orig_ts = {idx: df_out.at[idx, 'timestamp'] for idx in group_indices}

        # Find all restart row positions within this group
        restart_positions = []
        for pos, idx in enumerate(group_indices):
            if df_out.at[idx, 'event_type'] == 'restart':
                restart_positions.append(pos)

        # Process each restart in order
        for pos in restart_positions:
            # Skip if restart is the first or last row of the group
            if pos == 0 or pos == n - 1:
                continue

            restart_idx = group_indices[pos]
            prev_idx = group_indices[pos - 1]
            next_idx = group_indices[pos + 1]

            # prev_timestamp: already-adjusted timestamp of the immediately preceding row
            prev_timestamp = adjusted_timestamps[df_out.index.get_loc(prev_idx)]

            # next_original_timestamp: original timestamp of the immediately following row
            next_original_timestamp = orig_ts[next_idx]

            # Compute restart_timestamp
            restart_timestamp = prev_timestamp + round(next_original_timestamp / 2)

            # Set the restart row's adjusted timestamp
            restart_loc = df_out.index.get_loc(restart_idx)
            adjusted_timestamps[restart_loc] = restart_timestamp

            # Find the end boundary: first 'end' event after the restart row
            end_idx = group_indices[n - 1]  # default: last row of group
            for scan_pos in range(pos + 1, n):
                scan_idx = group_indices[scan_pos]
                if df_out.at[scan_idx, 'event_type'] == 'end':
                    end_idx = scan_idx
                    break

            # Apply offset to rows from (restart_pos + 1) through end_idx (inclusive)
            end_pos_in_group = group_indices.index(end_idx)
            for apply_pos in range(pos + 1, end_pos_in_group + 1):
                apply_idx = group_indices[apply_pos]
                apply_loc = df_out.index.get_loc(apply_idx)
                # Use original timestamp for the offset calculation
                adjusted_timestamps[apply_loc] = restart_timestamp + orig_ts[apply_idx]

    # Write adjusted timestamps back to df_out
    df_out['timestamp'] = adjusted_timestamps

    print(f"[Step 2] Output rows: {len(df_out)}")
    return df_out


# =============================================================================
# Step 3: Multiple Log Entries Handling
# =============================================================================
def step3_multiple_log_handling(df, remove_list=None, merge_blocks=None):
    """
    Step A: Remove ancillary action rows whose event_type is in remove_list.
    Step B: Merge consecutive block patterns within each SEQID into a single
            representative row.
    """
    print(f"[Step 3] Input rows: {len(df)}")

    # Default removal list
    if remove_list is None:
        remove_list = [
            'doaction', 'next_inquiry', 'next_button', 'confirmation_opened',
            'confirmation_closed', 'environment', 'mc_help_toolbar', 'mail_sent',
            'mail_deleted', 'mc_help_menuitem', 'sort_menu', 'copy', 'paste',
            'new_folder', 'mc_sort', 'delete_folder'
        ]

    # Default merge blocks
    if merge_blocks is None:
        merge_blocks = [
            {'sequence': ['folder_viewed', 'mail_drop', 'mail_moved'], 'represent': 'mail_drop'},
            {'sequence': ['folder_viewed', 'mail_moved', 'mail_drop'], 'represent': 'mail_drop'},
            {'sequence': ['button', 'mail_moved'], 'represent': 'button'},
            {'sequence': ['button', 'next_item', 'end'], 'represent': 'button'},
            {'sequence': ['breakoff', 'end'], 'represent': 'breakoff'},
        ]

    # --- Step A: Simple removal ---
    df_a = df[~df['event_type'].isin(remove_list)].copy()
    print(f"[Step 3A] Rows after simple removal: {len(df_a)}")

    # --- Step B: Block merging ---
    # Sort patterns by length descending so longer patterns are tried first
    sorted_blocks = sorted(merge_blocks, key=lambda b: len(b['sequence']), reverse=True)

    result_rows = []

    for seqid_val, group in df_a.groupby('SEQID', sort=False):
        # Work with list of row dicts for easy manipulation
        rows = group.reset_index(drop=False).to_dict('records')
        # 'index' key holds the original df_a index; we'll use positional scanning
        n = len(rows)
        pos = 0
        while pos < n:
            matched = False
            # Try each pattern (longest first)
            for block in sorted_blocks:
                seq = block['sequence']
                seq_len = len(seq)
                represent = block['represent']

                # Check if there are enough rows remaining
                if pos + seq_len > n:
                    continue

                # Check if the event_types match the pattern
                match = True
                for offset in range(seq_len):
                    if rows[pos + offset]['event_type'] != seq[offset]:
                        match = False
                        break

                if match:
                    # Find the representative row within the matched block
                    rep_row = None
                    for offset in range(seq_len):
                        if rows[pos + offset]['event_type'] == represent:
                            rep_row = rows[pos + offset].copy()
                            break
                    # If representative not found (shouldn't happen), use first row
                    if rep_row is None:
                        rep_row = rows[pos].copy()

                    result_rows.append(rep_row)
                    pos += seq_len
                    matched = True
                    break

            if not matched:
                result_rows.append(rows[pos])
                pos += 1

    if result_rows:
        df_b = pd.DataFrame(result_rows)
        # Restore original index column if present, then drop it
        if 'index' in df_b.columns:
            df_b = df_b.drop(columns=['index'])
        df_b = df_b.reset_index(drop=True)
    else:
        df_b = df_a.iloc[0:0].copy().reset_index(drop=True)

    print(f"[Step 3B] Rows after block merging: {len(df_b)}")
    print(f"[Step 3] Output rows: {len(df_b)}")
    return df_b


# =============================================================================
# Step 4: Duplicate Action Handling
# =============================================================================
def step4_duplicate_removal(df, exclude_keypress=True):
    """
    Remove completely identical duplicate rows.
    If exclude_keypress=True, keypress rows are excluded from deduplication
    (they are kept as-is and recombined after dedup of non-keypress rows).
    """
    print(f"[Step 4] Input rows: {len(df)}")

    if exclude_keypress:
        # Separate keypress and non-keypress rows
        mask_kp = df['event_type'] == 'keypress'
        df_keypress = df[mask_kp].copy()
        df_non_keypress = df[~mask_kp].copy()

        # Deduplicate only non-keypress rows
        df_non_keypress_dedup = df_non_keypress.drop_duplicates()

        # Recombine, preserving original order
        df_out = pd.concat([df_non_keypress_dedup, df_keypress], ignore_index=False)
        df_out = df_out.sort_index().reset_index(drop=True)
    else:
        # Deduplicate all rows
        df_out = df.drop_duplicates().reset_index(drop=True)

    print(f"[Step 4] Output rows: {len(df_out)}")
    return df_out


# =============================================================================
# Step 5: Time Reversal Handling
# =============================================================================
def step5_time_reversal(df):
    """
    Within each SEQID, sort rows by timestamp in ascending order using a stable
    sort (mergesort) to preserve relative order of equal timestamps.
    Reset the index after sorting.
    """
    print(f"[Step 5] Input rows: {len(df)}")

    df_out = df.copy()
    df_out = df_out.sort_values(
        by=['SEQID', 'timestamp'],
        kind='mergesort'
    ).reset_index(drop=True)

    print(f"[Step 5] Output rows: {len(df_out)}")
    return df_out


# =============================================================================
# Step 6: Keypress Aggregation
# =============================================================================
def step6_keypress_aggregation(df):
    """
    Aggregate consecutive keypress rows within each SEQID into a single row.
    The first keypress row of each block is kept; its event_description is
    overwritten with 'count=N' where N is the block size.
    All other keypress rows in the block are removed.
    """
    print(f"[Step 6] Input rows: {len(df)}")

    df_out = df.copy()

    # Identify consecutive keypress blocks using cumulative grouping
    # A new block starts whenever event_type changes OR SEQID changes
    is_keypress = df_out['event_type'] == 'keypress'

    # Create a block identifier: increment when event_type changes or SEQID changes
    seqid_change = df_out['SEQID'] != df_out['SEQID'].shift()
    event_change = df_out['event_type'] != df_out['event_type'].shift()
    block_id = (seqid_change | event_change).cumsum()

    df_out['_block_id'] = block_id
    df_out['_is_keypress'] = is_keypress

    # For keypress blocks, keep only the first row and set count
    rows_to_keep = []
    rows_to_drop = []

    for blk_id, blk_group in df_out.groupby('_block_id', sort=False):
        if blk_group['_is_keypress'].iloc[0]:
            # This is a keypress block
            n_kp = len(blk_group)
            first_idx = blk_group.index[0]
            # Mark first row to keep with updated description
            rows_to_keep.append(first_idx)
            df_out.at[first_idx, 'event_description'] = f'count={n_kp}'
            # Mark remaining rows to drop
            for idx in blk_group.index[1:]:
                rows_to_drop.append(idx)
        else:
            # Non-keypress block: keep all rows
            for idx in blk_group.index:
                rows_to_keep.append(idx)

    # Drop the extra keypress rows
    df_out = df_out.drop(index=rows_to_drop)

    # Remove helper columns
    df_out = df_out.drop(columns=['_block_id', '_is_keypress'])
    df_out = df_out.reset_index(drop=True)

    print(f"[Step 6] Output rows: {len(df_out)}")
    return df_out


# =============================================================================
# Step 7: Event Description Parsing and Filtering
# =============================================================================
def step7_event_description_parsing(df, target_booklet, target_item,
                                     desc_remove_list=None, remove_details=None):
    """
    Parse and filter the event_description column:
    1. Lowercase the value.
    2. Split by '|', '*', '$'.
    3. Strip whitespace and remove empty strings.
    4. For elements with '=': extract part2, remove prefix, filter by desc_remove_list.
       For elements without '=': filter by remove_details.
    5. Join remaining values with '_'.
    """
    print(f"[Step 7] Input rows: {len(df)}")

    # Default removal lists
    if desc_remove_list is None:
        desc_remove_list = ['test_time', 'end', 'value']
    if remove_details is None:
        remove_details = ['nan', ',', '.']

    # Prefix mapping
    prefix_map = {
        'PS1_1': 'u01a', 'PS1_2': 'u01b', 'PS1_3': 'u03a',
        'PS1_4': 'u06a', 'PS1_5': 'u06b', 'PS1_6': 'u21',
        'PS1_7': 'u04a', 'PS2_1': 'u19a', 'PS2_2': 'u19b',
        'PS2_3': 'u07',  'PS2_4': 'u02',  'PS2_5': 'u16',
        'PS2_6': 'u11b', 'PS2_7': 'u23'
    }

    # Determine the prefix to remove based on booklet and item
    key = f"{target_booklet}_{target_item}"
    prefix = prefix_map.get(key, '')
    prefix_pattern = prefix + '_' if prefix else ''

    df_out = df.copy()

    def parse_description(desc):
        # Handle NaN or non-string values
        if pd.isna(desc):
            return ''

        desc_str = str(desc)

        # Step 1: Lowercase
        desc_lower = desc_str.lower()

        # Step 2: Split by '|', '*', '$'
        elements = re.split(r'[\|\*\$]', desc_lower)

        # Step 3: Strip whitespace and remove empty strings
        elements = [e.strip() for e in elements]
        elements = [e for e in elements if e != '']

        # Step 4: Process each element
        kept_values = []
        for element in elements:
            if '=' in element:
                # Case A: element contains '='
                parts = element.split('=', 1)
                part1 = parts[0]
                part2 = parts[1] if len(parts) > 1 else ''

                # If part1 is in desc_remove_list, discard
                if part1 in desc_remove_list:
                    continue

                # Remove item-related prefix from part2
                if prefix_pattern and prefix_pattern in part2:
                    part2 = part2.replace(prefix_pattern, '', 1)

                # Keep only part2
                kept_values.append(part2)
            else:
                # Case B: element does NOT contain '='
                # Special handling: 'end' alone (as per spec "or element equals 'end'")
                # The spec says Case A applies if element contains '=' OR equals 'end'
                if element == 'end':
                    # Treat as Case A with part1='end', part2=''
                    if 'end' in desc_remove_list:
                        continue
                    kept_values.append('')
                else:
                    # Check against remove_details
                    if any(detail in element for detail in remove_details):
                        continue
                    kept_values.append(element)

        # Step 5: Join remaining values with '_'
        # Remove empty strings from kept_values before joining
        kept_values = [v for v in kept_values if v != '']
        result = '_'.join(kept_values)

        return result

    df_out['event_description'] = df_out['event_description'].apply(parse_description)

    print(f"[Step 7] Output rows: {len(df_out)}")
    return df_out


# =============================================================================
# Step 8: Merge Column and File Export
# =============================================================================
def step8_merge_and_export(df, country_codes, output_dir):
    """
    Create 'action_event' column by concatenating event_type with the parsed
    event_description values. Export selected columns to CSV.
    """
    print(f"[Step 8] Input rows: {len(df)}")

    df_out = df.copy()

    def build_action_event(row):
        evt_type = str(row['event_type'])
        evt_desc = str(row['event_description']) if not pd.isna(row['event_description']) else ''

        if evt_desc == '':
            return evt_type

        # Split event_description by original delimiters '$', '|', '*'
        parts = re.split(r'[\|\*\$]', evt_desc)
        part2_values = []
        for part in parts:
            part = part.strip()
            if '=' in part:
                split_part = part.split('=', 1)
                part2_values.append(split_part[1])
            else:
                if part != '':
                    part2_values.append(part)

        joined = '_'.join(part2_values)

        if joined == '':
            return evt_type
        else:
            return evt_type + '_' + joined

    df_out['action_event'] = df_out.apply(build_action_event, axis=1)

    # Determine output filename
    if len(country_codes) == 1:
        out_filename = f"{country_codes[0]}_processed_logdata.csv"
    else:
        out_filename = f"{'_'.join(country_codes)}_processed_logdata.csv"

    out_path = os.path.join(output_dir, out_filename)

    # Export selected columns
    export_cols = ['SEQID', 'event_type', 'event_description', 'action_event', 'timestamp']
    df_export = df_out[export_cols]
    df_export.to_csv(out_path, index=False)
    print(f"[Step 8] Exported {len(df_export)} rows to '{out_path}'")
    print(f"[Step 8] Output rows: {len(df_out)}")

    return df_out


# =============================================================================
# Main Orchestration Function
# =============================================================================
def preprocess_piaac(
    country_codes,
    target_booklet,
    target_item,
    exclude_keypress=True,
    input_dir='.',
    output_dir='.',
    remove_list=None,
    merge_blocks=None,
    desc_remove_list=None,
    remove_details=None
):
    """
    Orchestrate all 8 preprocessing steps for PIAAC log data.

    Parameters
    ----------
    country_codes : list of str
        List of country codes (e.g., ['US'] or ['US', 'KR']).
    target_booklet : str
        Booklet ID to filter (e.g., 'PS1').
    target_item : int
        Item ID to filter (e.g., 6).
    exclude_keypress : bool
        If True, keypress rows are excluded from deduplication in Step 4.
    input_dir : str
        Directory containing input log files.
    output_dir : str
        Directory for output CSV files.
    remove_list : list or None
        Event types to remove in Step 3A. Uses default if None.
    merge_blocks : list or None
        Block patterns to merge in Step 3B. Uses default if None.
    desc_remove_list : list or None
        Description keys to remove in Step 7. Uses default if None.
    remove_details : list or None
        Detail strings to remove in Step 7. Uses default if None.

    Returns
    -------
    pd.DataFrame
        Final processed DataFrame.
    """
    print("=" * 60)
    print("PIAAC Log Data Preprocessing Pipeline")
    print("=" * 60)

    # Step 1: Data Loading and Item Selection
    print("\n--- Step 1: Data Loading and Item Selection ---")
    df = step1_load_and_filter(
        input_dir=input_dir,
        country_codes=country_codes,
        target_booklet=target_booklet,
        target_item=target_item
    )

    # Step 2: Restart Event Preprocessing
    print("\n--- Step 2: Restart Event Preprocessing ---")
    df = step2_restart_correction(df)

    # Step 3: Multiple Log Entries Handling
    print("\n--- Step 3: Multiple Log Entries Handling ---")
    df = step3_multiple_log_handling(
        df,
        remove_list=remove_list,
        merge_blocks=merge_blocks
    )

    # Step 4: Duplicate Action Handling
    print("\n--- Step 4: Duplicate Action Handling ---")
    df = step4_duplicate_removal(df, exclude_keypress=exclude_keypress)

    # Step 5: Time Reversal Handling
    print("\n--- Step 5: Time Reversal Handling ---")
    df = step5_time_reversal(df)

    # Step 6: Keypress Aggregation
    print("\n--- Step 6: Keypress Aggregation ---")
    df = step6_keypress_aggregation(df)

    # Step 7: Event Description Parsing and Filtering
    print("\n--- Step 7: Event Description Parsing and Filtering ---")
    df = step7_event_description_parsing(
        df,
        target_booklet=target_booklet,
        target_item=target_item,
        desc_remove_list=desc_remove_list,
        remove_details=remove_details
    )

    # Step 8: Merge Column and File Export
    print("\n--- Step 8: Merge Column and File Export ---")
    df = step8_merge_and_export(
        df,
        country_codes=country_codes,
        output_dir=output_dir
    )

    print("\n" + "=" * 60)
    print("Preprocessing complete.")
    print("=" * 60)

    return df


# =============================================================================
# Verification: Test against the input/output examples in the specification
# =============================================================================
def run_verification():
    """
    Verify each step against the provided input/output examples.
    """
    print("\n" + "=" * 60)
    print("VERIFICATION TESTS")
    print("=" * 60)

    # -------------------------------------------------------------------------
    # Step 2 Verification
    # -------------------------------------------------------------------------
    print("\n--- Step 2 Verification ---")
    data_s2 = {
        'CNTRYID': ['X'] * 5,
        'SEQID': [1, 1, 1, 1, 1],
        'booklet_id': ['PS1'] * 5,
        'item_id': [1] * 5,
        'event_name': ['taoPIAAC'] * 5,
        'event_type': ['click', 'restart', 'click', 'end', 'click'],
        'timestamp': [8000.0, 0.0, 2000.0, 3000.0, 4000.0],
        'event_description': [''] * 5
    }
    df_s2_in = pd.DataFrame(data_s2)
    df_s2_out = step2_restart_correction(df_s2_in)
    expected_ts_s2 = [8000.0, 9000.0, 11000.0, 12000.0, 4000.0]
    actual_ts_s2 = df_s2_out['timestamp'].tolist()
    print(f"  Expected timestamps: {expected_ts_s2}")
    print(f"  Actual timestamps:   {actual_ts_s2}")
    assert actual_ts_s2 == expected_ts_s2, f"Step 2 FAILED: {actual_ts_s2}"
    print("  Step 2: PASSED")

    # -------------------------------------------------------------------------
    # Step 3A Verification (Simple Removal)
    # -------------------------------------------------------------------------
    print("\n--- Step 3A Verification (Simple Removal) ---")
    data_s3a = {
        'CNTRYID': ['X'] * 4,
        'SEQID': [417, 417, 417, 417],
        'booklet_id': ['PS1'] * 4,
        'item_id': [1] * 4,
        'event_name': ['taoPIAAC'] * 4,
        'event_type': ['view', 'doaction', 'capture', 'doaction'],
        'timestamp': [2500.0, 5500.0, 9000.0, 13000.0],
        'event_description': [''] * 4
    }
    df_s3a_in = pd.DataFrame(data_s3a)
    df_s3a_out = step3_multiple_log_handling(df_s3a_in)
    expected_types_s3a = ['view', 'capture']
    actual_types_s3a = df_s3a_out['event_type'].tolist()
    print(f"  Expected event_types: {expected_types_s3a}")
    print(f"  Actual event_types:   {actual_types_s3a}")
    assert actual_types_s3a == expected_types_s3a, f"Step 3A FAILED: {actual_types_s3a}"
    print("  Step 3A: PASSED")

    # -------------------------------------------------------------------------
    # Step 3B Verification (Block Merging)
    # -------------------------------------------------------------------------
    print("\n--- Step 3B Verification (Block Merging) ---")
    data_s3b = {
        'CNTRYID': ['X'] * 5,
        'SEQID': [1202, 1202, 1202, 1202, 1202],
        'booklet_id': ['PS1'] * 5,
        'item_id': [1] * 5,
        'event_name': ['taoPIAAC'] * 5,
        'event_type': ['click', 'folder_viewed', 'mail_drop', 'mail_moved', 'cancel'],
        'timestamp': [1000.0, 1800.0, 6500.0, 9500.0, 12000.0],
        'event_description': [''] * 5
    }
    df_s3b_in = pd.DataFrame(data_s3b)
    df_s3b_out = step3_multiple_log_handling(df_s3b_in)
    expected_types_s3b = ['click', 'mail_drop', 'cancel']
    expected_ts_s3b = [1000.0, 6500.0, 12000.0]
    actual_types_s3b = df_s3b_out['event_type'].tolist()
    actual_ts_s3b = df_s3b_out['timestamp'].tolist()
    print(f"  Expected event_types: {expected_types_s3b}")
    print(f"  Actual event_types:   {actual_types_s3b}")
    print(f"  Expected timestamps:  {expected_ts_s3b}")
    print(f"  Actual timestamps:    {actual_ts_s3b}")
    assert actual_types_s3b == expected_types_s3b, f"Step 3B types FAILED: {actual_types_s3b}"
    assert actual_ts_s3b == expected_ts_s3b, f"Step 3B timestamps FAILED: {actual_ts_s3b}"
    print("  Step 3B: PASSED")

    # -------------------------------------------------------------------------
    # Step 4 Verification
    # -------------------------------------------------------------------------
    print("\n--- Step 4 Verification ---")
    data_s4 = {
        'CNTRYID': ['X'] * 4,
        'SEQID': [309, 309, 309, 309],
        'booklet_id': ['PS1'] * 4,
        'item_id': [1] * 4,
        'event_name': ['taoPIAAC'] * 4,
        'event_type': ['click', 'click', 'keypress', 'keypress'],
        'timestamp': [5000.0, 5000.0, 8000.0, 8000.0],
        'event_description': [''] * 4
    }
    df_s4_in = pd.DataFrame(data_s4)

    # exclude_keypress=False: both click and keypress duplicates removed
    df_s4_out_false = step4_duplicate_removal(df_s4_in, exclude_keypress=False)
    expected_types_s4_false = ['click', 'keypress']
    actual_types_s4_false = df_s4_out_false['event_type'].tolist()
    print(f"  exclude_keypress=False Expected: {expected_types_s4_false}")
    print(f"  exclude_keypress=False Actual:   {actual_types_s4_false}")
    assert actual_types_s4_false == expected_types_s4_false, \
        f"Step 4 (exclude_keypress=False) FAILED: {actual_types_s4_false}"

    # exclude_keypress=True: only click duplicates removed; keypress kept as-is
    df_s4_out_true = step4_duplicate_removal(df_s4_in, exclude_keypress=True)
    expected_types_s4_true = ['click', 'keypress', 'keypress']
    actual_types_s4_true = df_s4_out_true['event_type'].tolist()
    print(f"  exclude_keypress=True Expected: {expected_types_s4_true}")
    print(f"  exclude_keypress=True Actual:   {actual_types_s4_true}")
    assert actual_types_s4_true == expected_types_s4_true, \
        f"Step 4 (exclude_keypress=True) FAILED: {actual_types_s4_true}"
    print("  Step 4: PASSED")

    # -------------------------------------------------------------------------
    # Step 5 Verification
    # -------------------------------------------------------------------------
    print("\n--- Step 5 Verification ---")
    data_s5 = {
        'CNTRYID': ['X'] * 5,
        'SEQID': [713, 713, 713, 713, 713],
        'booklet_id': ['PS1'] * 5,
        'item_id': [1] * 5,
        'event_name': ['taoPIAAC'] * 5,
        'event_type': ['mail_drag', 'folder_open', 'mail_click', 'folder_view', 'mail_view'],
        'timestamp': [100.0, 2000.0, 1500.0, 5500.0, 5500.0],
        'event_description': ['id=item101', 'id=folder_a', 'target=msg5', 'id=folder_b', 'id=item204']
    }
    df_s5_in = pd.DataFrame(data_s5)
    df_s5_out = step5_time_reversal(df_s5_in)
    expected_types_s5 = ['mail_drag', 'mail_click', 'folder_open', 'folder_view', 'mail_view']
    expected_ts_s5 = [100.0, 1500.0, 2000.0, 5500.0, 5500.0]
    actual_types_s5 = df_s5_out['event_type'].tolist()
    actual_ts_s5 = df_s5_out['timestamp'].tolist()
    print(f"  Expected event_types: {expected_types_s5}")
    print(f"  Actual event_types:   {actual_types_s5}")
    print(f"  Expected timestamps:  {expected_ts_s5}")
    print(f"  Actual timestamps:    {actual_ts_s5}")
    assert actual_types_s5 == expected_types_s5, f"Step 5 types FAILED: {actual_types_s5}"
    assert actual_ts_s5 == expected_ts_s5, f"Step 5 timestamps FAILED: {actual_ts_s5}"
    print("  Step 5: PASSED")

    # -------------------------------------------------------------------------
    # Step 6 Verification
    # -------------------------------------------------------------------------
    print("\n--- Step 6 Verification ---")
    data_s6 = {
        'CNTRYID': ['X'] * 7,
        'SEQID': [500, 500, 500, 500, 500, 500, 500],
        'booklet_id': ['PS1'] * 7,
        'item_id': [1] * 7,
        'event_name': ['taoPIAAC'] * 7,
        'event_type': ['click', 'keypress', 'keypress', 'keypress', 'click', 'keypress', 'mail_drag'],
        'timestamp': [1000.0, 2000.0, 2100.0, 2200.0, 5000.0, 7000.0, 9000.0],
        'event_description': ['target=btn1', 'key=a', 'key=b', 'key=c', 'target=btn2', 'key=x', 'id=item101']
    }
    df_s6_in = pd.DataFrame(data_s6)
    df_s6_out = step6_keypress_aggregation(df_s6_in)
    expected_types_s6 = ['click', 'keypress', 'click', 'keypress', 'mail_drag']
    expected_desc_s6 = ['target=btn1', 'count=3', 'target=btn2', 'count=1', 'id=item101']
    expected_ts_s6 = [1000.0, 2000.0, 5000.0, 7000.0, 9000.0]
    actual_types_s6 = df_s6_out['event_type'].tolist()
    actual_desc_s6 = df_s6_out['event_description'].tolist()
    actual_ts_s6 = df_s6_out['timestamp'].tolist()
    print(f"  Expected event_types:       {expected_types_s6}")
    print(f"  Actual event_types:         {actual_types_s6}")
    print(f"  Expected event_description: {expected_desc_s6}")
    print(f"  Actual event_description:   {actual_desc_s6}")
    print(f"  Expected timestamps:        {expected_ts_s6}")
    print(f"  Actual timestamps:          {actual_ts_s6}")
    assert actual_types_s6 == expected_types_s6, f"Step 6 types FAILED: {actual_types_s6}"
    assert actual_desc_s6 == expected_desc_s6, f"Step 6 desc FAILED: {actual_desc_s6}"
    assert actual_ts_s6 == expected_ts_s6, f"Step 6 timestamps FAILED: {actual_ts_s6}"
    print("  Step 6: PASSED")

    # -------------------------------------------------------------------------
    # Step 7 Verification
    # -------------------------------------------------------------------------
    print("\n--- Step 7 Verification ---")
    data_s7 = {
        'CNTRYID': ['X'] * 4,
        'SEQID': [1010, 1010, 1010, 1010],
        'booklet_id': ['PS1'] * 4,
        'item_id': [1] * 4,
        'event_name': ['taoPIAAC'] * 4,
        'event_type': ['a', 'b', 'c', 'd'],
        'timestamp': [1.0, 2.0, 3.0, 4.0],
        'event_description': ['test_time=a$target=bf', 'value=ox', 'key=dv$index=pir', 'id=u01a_item393']
    }
    df_s7_in = pd.DataFrame(data_s7)
    df_s7_out = step7_event_description_parsing(
        df_s7_in,
        target_booklet='PS1',
        target_item=1,
        desc_remove_list=['test_time', 'end', 'value'],
        remove_details=['nan', ',', '.']
    )
    expected_desc_s7 = ['bf', '', 'dv_pir', 'item393']
    actual_desc_s7 = df_s7_out['event_description'].tolist()
    print(f"  Expected event_description: {expected_desc_s7}")
    print(f"  Actual event_description:   {actual_desc_s7}")
    assert actual_desc_s7 == expected_desc_s7, f"Step 7 FAILED: {actual_desc_s7}"
    print("  Step 7: PASSED")

    # -------------------------------------------------------------------------
    # Step 8 Verification
    # -------------------------------------------------------------------------
    print("\n--- Step 8 Verification ---")
    data_s8 = {
        'CNTRYID': ['X'] * 4,
        'SEQID': [915, 915, 915, 915],
        'booklet_id': ['PS1'] * 4,
        'item_id': [1] * 4,
        'event_name': ['taoPIAAC'] * 4,
        'event_type': ['click_folder', 'menu_button', 'button_file', 'mail_drag'],
        'timestamp': [1.0, 2.0, 3.0, 4.0],
        'event_description': ['bf', '', 'dv_pir', 'item393']
    }
    df_s8_in = pd.DataFrame(data_s8)

    # Use a temp directory for output
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        df_s8_out = step8_merge_and_export(df_s8_in, country_codes=['X'], output_dir=tmpdir)

    expected_action_s8 = [
        'click_folder_bf',
        'menu_button',
        'button_file_dv_pir',
        'mail_drag_item393'
    ]
    actual_action_s8 = df_s8_out['action_event'].tolist()
    print(f"  Expected action_event: {expected_action_s8}")
    print(f"  Actual action_event:   {actual_action_s8}")
    assert actual_action_s8 == expected_action_s8, f"Step 8 FAILED: {actual_action_s8}"
    print("  Step 8: PASSED")

    print("\n" + "=" * 60)
    print("ALL VERIFICATION TESTS PASSED")
    print("=" * 60)


# =============================================================================
# Entry Point
# =============================================================================
if __name__ == '__main__':
    # Run verification tests against the specification examples
    run_verification()

    # Example usage of the full pipeline (uncomment and configure as needed):
    result_df = preprocess_piaac(
        country_codes=['US'],
        target_booklet='PS1',
        target_item=1,
        exclude_keypress=False,
        input_dir='.',
        output_dir='.',
        remove_list=None,       # uses default
        merge_blocks=None,      # uses default
        desc_remove_list=None,  # uses default
        remove_details=None     # uses default
    )
