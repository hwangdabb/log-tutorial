# ================================================================
# 생성 모델: deepseek/deepseek-v3.2
# 응답 시간: 84.87s
# 입력 토큰: 6522
# 출력 토큰: 4182
# ================================================================

import pandas as pd
import re
import os
import numpy as np

# ==================== STEP 1: Data Loading and Item Selection ====================
def step1_load_and_select(input_dir, country_codes, target_booklet, target_item):
    """
    Load log data files and filter to target booklet and item.
    """
    dfs = []
    
    for country_code in country_codes:
        file_path = os.path.join(input_dir, f"{country_code}_logdata.txt")
        if not os.path.exists(file_path):
            print(f"Warning: File {file_path} not found, skipping.")
            continue
            
        df = pd.read_csv(file_path, sep='\t')
        print(f"Loaded {country_code}: {len(df)} rows")
        
        # Prepend country code to SEQID for unique identification
        df['SEQID'] = country_code + '_' + df['SEQID'].astype(str)
        dfs.append(df)
    
    if not dfs:
        raise ValueError("No data loaded. Check input_dir and country_codes.")
    
    df_combined = pd.concat(dfs, ignore_index=True)
    print(f"Total rows after loading all countries: {len(df_combined)}")
    
    # Filter to target booklet and item
    df_filtered = df_combined[
        (df_combined['booklet_id'] == target_booklet) & 
        (df_combined['item_id'] == target_item)
    ].copy()
    print(f"Rows after filtering to booklet {target_booklet}, item {target_item}: {len(df_filtered)}")
    
    # Normalize event_type to lowercase
    df_filtered['event_type'] = df_filtered['event_type'].str.lower()
    
    return df_filtered

# ==================== STEP 2: Restart Event Preprocessing ====================
def step2_restart_correction(df):
    """
    Correct timestamps for restart events.
    """
    df_processed = df.copy()
    print(f"Step 2 input rows: {len(df_processed)}")
    
    # Create adjusted timestamp column (initially same as original)
    df_processed['timestamp_adjusted'] = df_processed['timestamp'].copy()
    
    # Process each SEQID group
    for seqid in df_processed['SEQID'].unique():
        seqid_mask = df_processed['SEQID'] == seqid
        seqid_indices = df_processed[seqid_mask].index.tolist()
        
        if len(seqid_indices) < 2:
            continue
            
        # Get restart indices within this SEQID
        restart_mask = df_processed.loc[seqid_indices, 'event_type'] == 'restart'
        restart_indices = df_processed.loc[seqid_indices][restart_mask].index.tolist()
        
        # Process each restart in order
        for restart_idx in restart_indices:
            # Find position within SEQID group
            seqid_pos = seqid_indices.index(restart_idx)
            
            # Skip if first or last row in SEQID
            if seqid_pos == 0 or seqid_pos == len(seqid_indices) - 1:
                continue
            
            # Get previous and next indices
            prev_idx = seqid_indices[seqid_pos - 1]
            next_idx = seqid_indices[seqid_pos + 1]
            
            # Get timestamps
            prev_timestamp = df_processed.loc[prev_idx, 'timestamp_adjusted']
            next_original_timestamp = df_processed.loc[next_idx, 'timestamp']
            
            # Calculate restart timestamp
            restart_timestamp = prev_timestamp + round(next_original_timestamp / 2)
            df_processed.loc[restart_idx, 'timestamp_adjusted'] = restart_timestamp
            
            # Find end boundary
            end_idx = None
            for i in range(seqid_pos + 1, len(seqid_indices)):
                current_idx = seqid_indices[i]
                if df_processed.loc[current_idx, 'event_type'] == 'end':
                    end_idx = current_idx
                    break
            
            if end_idx is None:
                end_idx = seqid_indices[-1]
            
            # Apply offset to rows after restart up to end_idx
            for i in range(seqid_pos + 1, len(seqid_indices)):
                current_idx = seqid_indices[i]
                if current_idx > end_idx:
                    break
                
                original_timestamp = df_processed.loc[current_idx, 'timestamp']
                df_processed.loc[current_idx, 'timestamp_adjusted'] = (
                    restart_timestamp + original_timestamp
                )
    
    # Replace original timestamp with adjusted
    df_processed['timestamp'] = df_processed['timestamp_adjusted']
    df_processed = df_processed.drop(columns=['timestamp_adjusted'])
    
    print(f"Step 2 output rows: {len(df_processed)}")
    return df_processed

# ==================== STEP 3: Multiple Log Entries Handling ====================
def step3_multiple_log_entries(df, remove_list=None, merge_blocks=None):
    """
    Handle multiple log entries: remove ancillary actions and merge blocks.
    """
    df_processed = df.copy()
    print(f"Step 3 input rows: {len(df_processed)}")
    
    # Set default values if None
    if remove_list is None:
        remove_list = ['doaction', 'next_inquiry', 'next_button', 'confirmation_opened',
                      'confirmation_closed', 'environment', 'mc_help_toolbar', 'mail_sent',
                      'mail_deleted', 'mc_help_menuitem', 'sort_menu', 'copy', 'paste',
                      'new_folder', 'mc_sort', 'delete_folder']
    
    if merge_blocks is None:
        merge_blocks = [
            {'sequence': ['folder_viewed', 'mail_drop', 'mail_moved'], 'represent': 'mail_drop'},
            {'sequence': ['folder_viewed', 'mail_moved', 'mail_drop'], 'represent': 'mail_drop'},
            {'sequence': ['button', 'mail_moved'], 'represent': 'button'},
            {'sequence': ['button', 'next_item', 'end'], 'represent': 'button'},
            {'sequence': ['breakoff', 'end'], 'represent': 'breakoff'},
        ]
    
    # Step A: Simple removal
    df_processed = df_processed[~df_processed['event_type'].isin(remove_list)].copy()
    print(f"Rows after simple removal: {len(df_processed)}")
    
    # Step B: Block merging
    # Sort patterns by length descending
    merge_blocks_sorted = sorted(merge_blocks, key=lambda x: len(x['sequence']), reverse=True)
    
    # Process each SEQID group
    rows_to_keep = []
    
    for seqid in df_processed['SEQID'].unique():
        seqid_df = df_processed[df_processed['SEQID'] == seqid].copy()
        seqid_indices = seqid_df.index.tolist()
        seqid_events = seqid_df['event_type'].tolist()
        
        i = 0
        while i < len(seqid_events):
            matched = False
            
            # Try each pattern
            for block in merge_blocks_sorted:
                pattern = block['sequence']
                pattern_len = len(pattern)
                
                # Check if pattern matches starting at position i
                if i + pattern_len <= len(seqid_events):
                    if seqid_events[i:i+pattern_len] == pattern:
                        # Pattern matched - keep representative row
                        rep_event = block['represent']
                        
                        # Find the row with the representative event type
                        rep_idx = None
                        for j in range(i, i + pattern_len):
                            if seqid_events[j] == rep_event:
                                rep_idx = seqid_indices[j]
                                break
                        
                        if rep_idx is not None:
                            rows_to_keep.append(rep_idx)
                        
                        # Skip the consumed rows
                        i += pattern_len
                        matched = True
                        break
            
            if not matched:
                # No pattern matched, keep current row
                rows_to_keep.append(seqid_indices[i])
                i += 1
    
    # Create new DataFrame with kept rows
    df_processed = df_processed.loc[rows_to_keep].copy().reset_index(drop=True)
    print(f"Step 3 output rows: {len(df_processed)}")
    return df_processed

# ==================== STEP 4: Duplicate Action Handling ====================
def step4_duplicate_handling(df, exclude_keypress=True):
    """
    Remove duplicate rows, optionally excluding keypress rows.
    """
    df_processed = df.copy()
    print(f"Step 4 input rows: {len(df_processed)}")
    
    if exclude_keypress:
        # Separate keypress and non-keypress rows
        keypress_mask = df_processed['event_type'] == 'keypress'
        df_keypress = df_processed[keypress_mask].copy()
        df_non_keypress = df_processed[~keypress_mask].copy()
        
        # Remove duplicates from non-keypress rows only
        df_non_keypress = df_non_keypress.drop_duplicates()
        
        # Recombine
        df_processed = pd.concat([df_non_keypress, df_keypress], ignore_index=True)
    else:
        # Remove all duplicates
        df_processed = df_processed.drop_duplicates()
    
    print(f"Step 4 output rows: {len(df_processed)}")
    return df_processed

# ==================== STEP 5: Time Reversal Handling ====================
def step5_time_reversal(df):
    """
    Sort rows within each SEQID by timestamp.
    """
    df_processed = df.copy()
    print(f"Step 5 input rows: {len(df_processed)}")
    
    # Sort within each SEQID group
    df_processed = df_processed.sort_values(
        by=['SEQID', 'timestamp'],
        kind='mergesort'
    ).reset_index(drop=True)
    
    print(f"Step 5 output rows: {len(df_processed)}")
    return df_processed

# ==================== STEP 6: Keypress Aggregation ====================
def step6_keypress_aggregation(df):
    """
    Aggregate consecutive keypress rows into single rows with count.
    """
    df_processed = df.copy()
    print(f"Step 6 input rows: {len(df_processed)}")
    
    # Identify keypress blocks within each SEQID
    df_processed['keypress_block'] = (
        (df_processed['event_type'] != df_processed['event_type'].shift()) |
        (df_processed['SEQID'] != df_processed['SEQID'].shift())
    ).cumsum()
    
    # Process each block
    rows_to_keep = []
    
    for block_id in df_processed['keypress_block'].unique():
        block_df = df_processed[df_processed['keypress_block'] == block_id]
        
        if block_df.iloc[0]['event_type'] == 'keypress':
            # Keypress block - keep first row, update description
            first_row = block_df.iloc[0].copy()
            count = len(block_df)
            first_row['event_description'] = f'count={count}'
            rows_to_keep.append(first_row)
        else:
            # Non-keypress block - keep all rows
            rows_to_keep.extend(block_df.to_dict('records'))
    
    # Create new DataFrame
    df_processed = pd.DataFrame(rows_to_keep)
    
    # Remove temporary column if it exists
    if 'keypress_block' in df_processed.columns:
        df_processed = df_processed.drop(columns=['keypress_block'])
    
    print(f"Step 6 output rows: {len(df_processed)}")
    return df_processed

# ==================== STEP 7: Event Description Parsing ====================
def step7_event_description_parsing(df, target_booklet, target_item, 
                                   desc_remove_list=None, remove_details=None):
    """
    Parse and filter event_description values.
    """
    df_processed = df.copy()
    print(f"Step 7 input rows: {len(df_processed)}")
    
    # Set default values if None
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
    
    # Get prefix for current booklet and item
    key = f"{target_booklet}_{target_item}"
    prefix = prefix_map.get(key, '')
    
    def parse_description(desc):
        if pd.isna(desc):
            return ''
        
        desc_str = str(desc).lower()
        
        # Split by delimiters
        parts = re.split(r'[\|\*\$]', desc_str)
        parts = [p.strip() for p in parts if p.strip()]
        
        filtered_components = []
        
        for part in parts:
            if '=' in part or part == 'end':
                # Case A: Contains '=' or is 'end'
                if part == 'end':
                    # 'end' is in desc_remove_list, so discard
                    continue
                
                var_name, value = part.split('=', 1)
                
                # Check if variable name should be removed
                if var_name in desc_remove_list:
                    continue
                
                # Remove prefix if present
                if prefix and value.startswith(prefix + '_'):
                    value = value.replace(prefix + '_', '', 1)
                
                filtered_components.append(value)
            else:
                # Case B: No '='
                # Check if contains any detail to remove
                if any(detail in part for detail in remove_details):
                    continue
                filtered_components.append(part)
        
        # Join with '_'
        return '_'.join(filtered_components)
    
    # Apply parsing
    df_processed['event_description'] = df_processed['event_description'].apply(parse_description)
    
    print(f"Step 7 output rows: {len(df_processed)}")
    return df_processed

# ==================== STEP 8: Merge Column and Export ====================
def step8_merge_and_export(df, output_dir, country_codes):
    """
    Create action_event column and export to CSV.
    """
    df_processed = df.copy()
    print(f"Step 8 input rows: {len(df_processed)}")
    
    def create_action_event(row):
        event_type = row['event_type']
        event_desc = row['event_description']
        
        if pd.isna(event_desc) or event_desc == '':
            return event_type
        
        # For merging, we use the already parsed event_description
        # which already has components joined with '_'
        return f"{event_type}_{event_desc}"
    
    # Create action_event column
    df_processed['action_event'] = df_processed.apply(create_action_event, axis=1)
    
    # Select columns for export
    export_cols = ['SEQID', 'event_type', 'event_description', 'action_event', 'timestamp']
    df_export = df_processed[export_cols].copy()
    
    # Export CSV for each country
    for country_code in country_codes:
        # Filter rows for this country
        country_mask = df_export['SEQID'].str.startswith(country_code + '_')
        country_df = df_export[country_mask].copy()
        
        if len(country_df) > 0:
            output_path = os.path.join(output_dir, f"{country_code}_processed_logdata.csv")
            country_df.to_csv(output_path, index=False)
            print(f"Exported {len(country_df)} rows to {output_path}")
    
    print(f"Step 8 output rows: {len(df_processed)}")
    return df_processed

# ==================== MAIN FUNCTION ====================
def preprocess_piaac(input_dir, output_dir, country_codes, target_booklet, target_item,
                    exclude_keypress=True, remove_list=None, merge_blocks=None,
                    desc_remove_list=None, remove_details=None):
    """
    Main function orchestrating all preprocessing steps.
    """
    print("=" * 60)
    print("Starting PIAAC Log Data Preprocessing")
    print("=" * 60)
    
    # Step 1: Load and select data
    df = step1_load_and_select(input_dir, country_codes, target_booklet, target_item)
    
    # Step 2: Restart correction
    df = step2_restart_correction(df)
    
    # Step 3: Multiple log entries handling
    df = step3_multiple_log_entries(df, remove_list, merge_blocks)
    
    # Step 4: Duplicate handling
    df = step4_duplicate_handling(df, exclude_keypress)
    
    # Step 5: Time reversal handling
    df = step5_time_reversal(df)
    
    # Step 6: Keypress aggregation
    df = step6_keypress_aggregation(df)
    
    # Step 7: Event description parsing
    df = step7_event_description_parsing(df, target_booklet, target_item,
                                        desc_remove_list, remove_details)
    
    # Step 8: Merge column and export
    df = step8_merge_and_export(df, output_dir, country_codes)
    
    print("=" * 60)
    print("Preprocessing completed successfully!")
    print("=" * 60)
    
    return df

# ==================== EXAMPLE USAGE ====================
if __name__ == "__main__":
    # Example configuration
    input_dir = "."
    output_dir = "."
    country_codes = ["US"]  # List of country codes to process
    target_booklet = "PS1"
    target_item = 1
    exclude_keypress = False
    
    # Run preprocessing
    result_df = preprocess_piaac(
        input_dir=input_dir,
        output_dir=output_dir,
        country_codes=country_codes,
        target_booklet=target_booklet,
        target_item=target_item,
        exclude_keypress=exclude_keypress,
        remove_list=None,  # Use defaults
        merge_blocks=None,  # Use defaults
        desc_remove_list=None,  # Use defaults
        remove_details=None  # Use defaults
    )
    
    print(f"Final processed DataFrame shape: {result_df.shape}")
