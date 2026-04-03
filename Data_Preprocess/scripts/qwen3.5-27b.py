# ================================================================
# 생성 모델: qwen/qwen3.5-27b
# 응답 시간: 1003.68s
# 입력 토큰: 7053
# 출력 토큰: 86212
# ================================================================

import pandas as pd
import re
import os
import numpy as np

# Default configuration lists and maps
DEFAULT_REMOVE_LIST = [
    'doaction', 'next_inquiry', 'next_button', 'confirmation_opened',
    'confirmation_closed', 'environment', 'mc_help_toolbar', 'mail_sent',
    'mail_deleted', 'mc_help_menuitem', 'sort_menu', 'copy', 'paste',
    'new_folder', 'mc_sort', 'delete_folder'
]

DEFAULT_MERGE_BLOCKS = [
    {'sequence': ['folder_viewed', 'mail_drop', 'mail_moved'], 'represent': 'mail_drop'},
    {'sequence': ['folder_viewed', 'mail_moved', 'mail_drop'], 'represent': 'mail_drop'},
    {'sequence': ['button', 'mail_moved'], 'represent': 'button'},
    {'sequence': ['button', 'next_item', 'end'], 'represent': 'button'},
    {'sequence': ['breakoff', 'end'], 'represent': 'breakoff'},
]

DEFAULT_DESC_REMOVE_LIST = ['test_time', 'end', 'value']
DEFAULT_REMOVE_DETAILS = ['nan', ',', '.']

DEFAULT_PREFIX_MAP = {
    'PS1_1': 'u01a', 'PS1_2': 'u01b', 'PS1_3': 'u03a',
    'PS1_4': 'u06a', 'PS1_5': 'u06b', 'PS1_6': 'u21',
    'PS1_7': 'u04a', 'PS2_1': 'u19a', 'PS2_2': 'u19b',
    'PS2_3': 'u07',  'PS2_4': 'u02',  'PS2_5': 'u16',
    'PS2_6': 'u11b', 'PS2_7': 'u23'
}


def step1_load_and_filter(input_dir, country_codes, target_booklet, target_item):
    """
    Step 1: Data Loading and Item Selection
    Loads tab-delimited files, handles multi-country SEQID prepending,
    filters by booklet and item, and lowercases event_type.
    """
    dfs = []
    # Ensure country_codes is a list
    if isinstance(country_codes, str):
        country_codes = [country_codes]
    
    for code in country_codes:
        filename = f"{code}_logdata.txt"
        filepath = os.path.join(input_dir, filename)
        if os.path.exists(filepath):
            df = pd.read_csv(filepath, sep='\t')
            # Prepend country code to SEQID for unique identification
            # df['SEQID'] = f"{code}_{df['SEQID']}" ## 버그가 존재한다고 말하기, 혹은 재생성 시도
            df['SEQID'] = code + "_" + df['SEQID'].astype(str)
            dfs.append(df)
        else:
            print(f"Warning: File {filepath} not found.")
    
    if not dfs:
        return pd.DataFrame()
    
    df = pd.concat(dfs, ignore_index=True)
    print(f"Step 1 - Before filtering: {len(df)} rows")
    
    # Filter by target_booklet and target_item
    df = df[(df['booklet_id'] == target_booklet) & (df['item_id'] == target_item)]
    
    # String normalization: lowercase event_type
    df['event_type'] = df['event_type'].str.lower()
    
    print(f"Step 1 - After filtering: {len(df)} rows")
    return df


def step2_restart_correction(df):
    """
    Step 2: Restart Event Preprocessing
    Corrects timestamps when 'restart' events occur within a SEQID group.
    """
    df = df.copy()
    print(f"Step 2 - Before restart correction: {len(df)} rows")
    
    # Group by SEQID to process each person's log independently
    for seqid, group in df.groupby('SEQID'):
        # Get indices of this group in the main DataFrame
        group_indices = group.index.tolist()
        if len(group_indices) < 2:
            continue
            
        # Create a list of original timestamps for this group to reference
        orig_ts = df.loc[group_indices, 'timestamp'].tolist()
        # Create a list for adjusted timestamps, initialized with original
        adj_ts = orig_ts.copy()
        
        # Find indices of 'restart' events within this group
        restart_local_indices = [i for i, et in enumerate(group['event_type']) if et == 'restart']
        
        for r_idx in restart_local_indices:
            # Skip if first or last row of the group
            if r_idx == 0 or r_idx == len(group) - 1:
                continue
            
            # Get global index of restart row
            restart_global_idx = group_indices[r_idx]
            
            # prev_timestamp: already-adjusted timestamp of preceding row
            prev_ts = adj_ts[r_idx - 1]
            
            # next_original_timestamp: original timestamp of following row
            next_orig_ts = orig_ts[r_idx + 1]
            
            # Calculate restart_timestamp
            restart_ts = prev_ts + round(next_orig_ts / 2)
            
            # Update the restart row's adjusted timestamp
            adj_ts[r_idx] = restart_ts
            
            # Find correction end boundary (next 'end' event or end of group)
            end_local_idx = len(group) - 1
            for k in range(r_idx + 1, len(group)):
                if group['event_type'].iloc[k] == 'end':
                    end_local_idx = k
                    break
            
            # Apply offset to rows from (restart_idx + 1) through end_idx
            for k in range(r_idx + 1, end_local_idx + 1):
                # adjusted_timestamp = restart_timestamp + original_timestamp
                adj_ts[k] = restart_ts + orig_ts[k]
        
        # Update the DataFrame with adjusted timestamps
        df.loc[group_indices, 'timestamp'] = adj_ts

    print(f"Step 2 - After restart correction: {len(df)} rows")
    return df


def step3_multiple_logs(df, remove_list=None, merge_blocks=None):
    """
    Step 3: Multiple Log Entries Handling
    Step A: Remove ancillary actions.
    Step B: Merge block patterns.
    """
    df = df.copy()
    print(f"Step 3 - Before multiple log handling: {len(df)} rows")
    
    # Set defaults
    if remove_list is None:
        remove_list = DEFAULT_REMOVE_LIST
    if merge_blocks is None:
        merge_blocks = DEFAULT_MERGE_BLOCKS
    
    # Step A: Simple removal
    df = df[~df['event_type'].isin(remove_list)]
    
    # Step B: Block merging
    # Sort patterns by length descending
    merge_blocks_sorted = sorted(merge_blocks, key=lambda x: len(x['sequence']), reverse=True)
    
    # We need to process each SEQID group
    # We will collect indices to keep
    keep_indices = []
    
    for seqid, group in df.groupby('SEQID'):
        group_indices = group.index.tolist()
        group_events = group['event_type'].tolist()
        n = len(group_indices)
        
        i = 0
        while i < n:
            matched = False
            # Try patterns in length-sorted order
            for pattern in merge_blocks_sorted:
                seq = pattern['sequence']
                rep = pattern['represent']
                seq_len = len(seq)
                
                if i + seq_len <= n:
                    # Check if sequence matches
                    if group_events[i:i+seq_len] == seq:
                        # Find index of representative row within the block
                        try:
                            rep_local_idx = seq.index(rep)
                            rep_global_idx = group_indices[i + rep_local_idx]
                            keep_indices.append(rep_global_idx)
                            # Consume the block
                            i += seq_len
                            matched = True
                            break
                        except ValueError:
                            # Representative not found in sequence (should not happen based on spec)
                            pass
            
            if not matched:
                # No pattern matched, keep current row
                keep_indices.append(group_indices[i])
                i += 1
    
    df = df.loc[keep_indices].copy()
    df.reset_index(drop=True, inplace=False) # Reset index without inplace
    
    print(f"Step 3 - After multiple log handling: {len(df)} rows")
    return df


def step4_duplicate_handling(df, exclude_keypress=False):
    """
    Step 4: Duplicate Action Handling
    Removes completely identical duplicate rows.
    Optionally excludes 'keypress' rows from deduplication.
    """
    df = df.copy()
    print(f"Step 4 - Before duplicate handling: {len(df)} rows")
    
    if exclude_keypress:
        # Separate keypress and non-keypress
        keypress_df = df[df['event_type'] == 'keypress']
        non_keypress_df = df[df['event_type'] != 'keypress']
        
        # Drop duplicates only on non-keypress
        non_keypress_df = non_keypress_df.drop_duplicates()
        
        # Recombine
        df = pd.concat([non_keypress_df, keypress_df], ignore_index=True)
    else:
        df = df.drop_duplicates()
    
    print(f"Step 4 - After duplicate handling: {len(df)} rows")
    return df


def step5_time_reversal(df):
    """
    Step 5: Time Reversal Handling
    Sorts rows within each SEQID group by timestamp in ascending order.
    """
    df = df.copy()
    print(f"Step 5 - Before time reversal handling: {len(df)} rows")
    
    # Sort by SEQID and timestamp, stable sort
    df = df.sort_values(by=['SEQID', 'timestamp'], kind='mergesort')
    df.reset_index(drop=True, inplace=False)
    
    print(f"Step 5 - After time reversal handling: {len(df)} rows")
    return df


def step6_keypress_aggregation(df):
    """
    Step 6: Keypress Aggregation
    Aggregates consecutive 'keypress' rows within a SEQID into a single row.
    """
    df = df.copy()
    print(f"Step 6 - Before keypress aggregation: {len(df)} rows")
    
    # Identify blocks of consecutive keypress rows within SEQID
    # We use a helper column to mark blocks
    # First, mark if current row is keypress
    df['is_keypress'] = (df['event_type'] == 'keypress')
    
    # Create a group ID for consecutive keypresses within SEQID
    # Shift event_type within SEQID to detect changes
    df['event_type_shift'] = df.groupby('SEQID')['event_type'].shift(1)
    df['is_change'] = (df['event_type'] != df['event_type_shift'])
    
    # Cumulative sum of changes within SEQID to create block IDs
    df['block_id'] = df.groupby('SEQID')['is_change'].cumsum()
    
    # Filter to only keypress rows to count block sizes
    keypress_rows = df[df['is_keypress']].copy()
    
    # Count size of each block
    block_counts = keypress_rows.groupby(['SEQID', 'block_id']).size().reset_index(name='count')
    
    # Merge count back to keypress rows
    keypress_rows = keypress_rows.merge(block_counts, on=['SEQID', 'block_id'], how='left')
    
    # Keep only the first row of each block
    # Sort by timestamp to ensure we get the first one in time
    keypress_rows = keypress_rows.sort_values(by=['SEQID', 'block_id', 'timestamp'])
    first_keypress = keypress_rows.drop_duplicates(subset=['SEQID', 'block_id'], keep='first')
    
    # Update event_description for the kept rows
    first_keypress['event_description'] = first_keypress['count'].apply(lambda x: f'count={x}')
    
    # Remove helper columns from first_keypress
    first_keypress = first_keypress.drop(columns=['is_keypress', 'event_type_shift', 'is_change', 'block_id', 'count'])
    
    # Get non-keypress rows
    non_keypress_rows = df[~df['is_keypress']].copy()
    non_keypress_rows = non_keypress_rows.drop(columns=['is_keypress', 'event_type_shift', 'is_change', 'block_id'])
    
    # Combine
    df = pd.concat([non_keypress_rows, first_keypress], ignore_index=True)
    
    print(f"Step 6 - After keypress aggregation: {len(df)} rows")
    return df


def step7_event_description_parsing(df, target_booklet, target_item, desc_remove_list=None, remove_details=None, prefix_map=None):
    """
    Step 7: Event Description Parsing and Filtering
    Parses event_description, removes unwanted parts, and cleans prefixes.
    """
    df = df.copy()
    print(f"Step 7 - Before event description parsing: {len(df)} rows")
    
    # Set defaults
    if desc_remove_list is None:
        desc_remove_list = DEFAULT_DESC_REMOVE_LIST
    if remove_details is None:
        remove_details = DEFAULT_REMOVE_DETAILS
    if prefix_map is None:
        prefix_map = DEFAULT_PREFIX_MAP
    
    # Get prefix for current booklet/item
    key = f"{target_booklet}_{target_item}"
    prefix_val = prefix_map.get(key, "")
    prefix_to_remove = f"{prefix_val}_"
    
    def parse_desc(desc):
        if pd.isna(desc):
            return ''
        
        # Convert to lowercase
        desc = str(desc).lower()
        
        # Split by delimiters
        parts = re.split(r'[\|\*\$]', desc)
        
        # Strip and remove empty
        parts = [p.strip() for p in parts if p.strip()]
        
        kept_values = []
        
        for element in parts:
            # Case A: Contains '=' or equals 'end'
            if '=' in element or element == 'end':
                if '=' in element:
                    split_parts = element.split('=', 1)
                    part1 = split_parts[0]
                    part2 = split_parts[1] if len(split_parts) > 1 else ''
                else:
                    # element == 'end'
                    part1 = 'end'
                    part2 = ''
                
                # Check removal list
                if part1 in desc_remove_list:
                    continue
                
                # Remove prefix from part2
                if prefix_to_remove and prefix_to_remove in part2:
                    part2 = part2.replace(prefix_to_remove, '', 1)
                
                # Keep only part2
                if part2:
                    kept_values.append(part2)
            
            # Case B: Does NOT contain '='
            else:
                # Check detail removal list
                if any(detail in element for detail in remove_details):
                    continue
                else:
                    kept_values.append(element)
        
        return '_'.join(kept_values)
    
    df['event_description'] = df['event_description'].apply(parse_desc)
    
    print(f"Step 7 - After event description parsing: {len(df)} rows")
    return df


def step8_export(df, country_codes, output_dir):
    """
    Step 8: Merge Column and File Export
    Creates 'action_event' column and saves to CSV.
    """
    df = df.copy()
    print(f"Step 8 - Before export: {len(df)} rows")
    
    # Create action_event column
    def create_action_event(row):
        event_type = row['event_type']
        desc = row['event_description']
        
        if pd.isna(desc) or desc == '':
            return event_type
        
        # Split by original delimiters (though Step 7 already processed, we follow spec)
        # Spec says: Split event_description by original delimiters, then split by '=' to extract part2
        # Since Step 7 output is joined by '_', splitting by $|* will likely return the whole string
        # Then split by '='
        parts = re.split(r'[\|\*\$]', desc)
        part2_values = []
        for p in parts:
            if '=' in p:
                split_p = p.split('=', 1)
                if len(split_p) > 1:
                    part2_values.append(split_p[1])
                else:
                    part2_values.append(p)
            else:
                part2_values.append(p)
        
        joined_part2 = '_'.join(part2_values)
        
        if joined_part2:
            return f"{event_type}_{joined_part2}"
        else:
            return event_type
    
    df['action_event'] = df.apply(create_action_event, axis=1)
    
    # Select columns for export
    export_cols = ['SEQID', 'event_type', 'event_description', 'action_event', 'timestamp']
    df_export = df[export_cols]
    
    # Determine output filename
    if isinstance(country_codes, list):
        code = country_codes[0]
    else:
        code = country_codes
    
    filename = f"{code}_processed_logdata.csv"
    filepath = os.path.join(output_dir, filename)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    df_export.to_csv(filepath, index=False)
    print(f"Step 8 - Exported to {filepath}")
    
    print(f"Step 8 - After export: {len(df)} rows")
    return df


def preprocess_piaac(country_codes, target_booklet, target_item, exclude_keypress=False,
                     input_dir='.', output_dir='.', remove_list=None, merge_blocks=None,
                     desc_remove_list=None, remove_details=None, prefix_map=None):
    """
    Main function to orchestrate all preprocessing steps.
    """
    # Step 1
    df = step1_load_and_filter(input_dir, country_codes, target_booklet, target_item)
    if df.empty:
        print("No data loaded. Exiting.")
        return df
    
    # Step 2
    df = step2_restart_correction(df)
    
    # Step 3
    df = step3_multiple_logs(df, remove_list, merge_blocks)
    
    # Step 4
    df = step4_duplicate_handling(df, exclude_keypress)
    
    # Step 5
    df = step5_time_reversal(df)
    
    # Step 6
    df = step6_keypress_aggregation(df)
    
    # Step 7
    df = step7_event_description_parsing(df, target_booklet, target_item, desc_remove_list, remove_details, prefix_map)
    
    # Step 8
    df = step8_export(df, country_codes, output_dir)
    
    return df


if __name__ == "__main__":
    # Example usage (uncomment and adjust paths/params to run)
    result_df = preprocess_piaac(
        country_codes=['US'],
        target_booklet='PS1',
        target_item=1,
        exclude_keypress=False,
        input_dir='.',
        output_dir='.'
    )
    pass
