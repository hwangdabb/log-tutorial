# ================================================================
# 생성 모델: google/gemini-3.1-flash-lite-preview
# 응답 시간: 10.45s
# 입력 토큰: 7286
# 출력 토큰: 2464
# ================================================================

import pandas as pd
import re
import os
import numpy as np

def step1_load_and_filter(file_paths, target_booklet, target_item):
    dfs = []
    for path in file_paths:
        country = os.path.basename(path).split('_')[0]
        df = pd.read_csv(path, sep='\t')
        df['SEQID'] = country + '_' + df['SEQID'].astype(str)
        dfs.append(df)
    df = pd.concat(dfs, ignore_index=True)
    print(f"Step 1: Initial row count: {len(df)}")
    df = df[(df['booklet_id'] == target_booklet) & (df['item_id'] == target_item)].copy()
    df['event_type'] = df['event_type'].str.lower()
    print(f"Step 1: Filtered row count: {len(df)}")
    return df

def step2_restart_correction(df):
    print(f"Step 2: Before: {len(df)}")
    df = df.copy()
    df['adjusted_timestamp'] = df['timestamp'].astype(float)
    
    for seq in df['SEQID'].unique():
        seq_df = df[df['SEQID'] == seq]
        restart_indices = seq_df[seq_df['event_type'] == 'restart'].index.tolist()
        
        for r_idx in restart_indices:
            if r_idx == seq_df.index[0] or r_idx == seq_df.index[-1]:
                continue
            
            prev_ts = df.loc[r_idx - 1, 'adjusted_timestamp']
            next_orig_ts = df.loc[r_idx + 1, 'timestamp']
            restart_ts = prev_ts + round(next_orig_ts / 2)
            
            end_idx = seq_df.index[-1]
            future_rows = seq_df.loc[r_idx + 1:]
            end_match = future_rows[future_rows['event_type'] == 'end']
            if not end_match.empty:
                end_idx = end_match.index[0]
            
            for i in range(r_idx, end_idx + 1):
                df.loc[i, 'adjusted_timestamp'] = restart_ts + df.loc[i, 'timestamp']
                
    df['timestamp'] = df['adjusted_timestamp']
    df = df.drop(columns=['adjusted_timestamp'])
    print(f"Step 2: After: {len(df)}")
    return df

def step3_multiple_logs(df, remove_list=None, merge_blocks=None):
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
    print(f"Step 3: Before: {len(df)}")
    df = df[~df['event_type'].isin(remove_list)].copy()
    
    merge_blocks.sort(key=lambda x: len(x['sequence']), reverse=True)
    
    final_rows = []
    for seq in df['SEQID'].unique():
        seq_df = df[df['SEQID'] == seq].reset_index(drop=True)
        skip_until = -1
        for i in range(len(seq_df)):
            if i <= skip_until: continue
            matched = False
            for block in merge_blocks:
                seq_len = len(block['sequence'])
                if i + seq_len <= len(seq_df):
                    if seq_df.loc[i:i+seq_len-1, 'event_type'].tolist() == block['sequence']:
                        rep_row = seq_df.iloc[i].copy()
                        rep_row['event_type'] = block['represent']
                        final_rows.append(rep_row)
                        skip_until = i + seq_len - 1
                        matched = True
                        break
            if not matched:
                final_rows.append(seq_df.iloc[i])
    
    df = pd.DataFrame(final_rows)
    print(f"Step 3: After: {len(df)}")
    return df

def step4_duplicate_removal(df, exclude_keypress):
    print(f"Step 4: Before: {len(df)}")
    if exclude_keypress:
        kp = df[df['event_type'] == 'keypress']
        other = df[df['event_type'] != 'keypress'].drop_duplicates()
        df = pd.concat([other, kp]).sort_index()
    else:
        df = df.drop_duplicates()
    print(f"Step 4: After: {len(df)}")
    return df

def step5_time_reversal(df):
    print(f"Step 5: Before: {len(df)}")
    df = df.sort_values(['SEQID', 'timestamp'], kind='mergesort').reset_index(drop=True)
    print(f"Step 5: After: {len(df)}")
    return df

def step6_keypress_aggregation(df):
    print(f"Step 6: Before: {len(df)}")
    df = df.copy()
    df['block'] = (df['event_type'] != df['event_type'].shift()).cumsum()
    
    new_rows = []
    for _, group in df.groupby(['SEQID', 'block']):
        if group['event_type'].iloc[0] == 'keypress':
            row = group.iloc[0].copy()
            row['event_description'] = f"count={len(group)}"
            new_rows.append(row)
        else:
            new_rows.extend([row for _, row in group.iterrows()])
            
    df = pd.DataFrame(new_rows).drop(columns=['block'])
    print(f"Step 6: After: {len(df)}")
    return df

def step7_parsing(df, target_booklet, target_item, desc_remove_list=None, remove_details=None):
    if desc_remove_list is None: desc_remove_list = ['test_time', 'end', 'value']
    if remove_details is None: remove_details = ['nan', ',', '.']
    prefix_map = {'PS1_1': 'u01a', 'PS1_2': 'u01b', 'PS1_3': 'u03a', 'PS1_4': 'u06a', 'PS1_5': 'u06b', 'PS1_6': 'u21', 'PS1_7': 'u04a', 'PS2_1': 'u19a', 'PS2_2': 'u19b', 'PS2_3': 'u07', 'PS2_4': 'u02', 'PS2_5': 'u16', 'PS2_6': 'u11b', 'PS2_7': 'u23'}
    prefix = prefix_map.get(f"{target_booklet}_{target_item}", "")
    
    print(f"Step 7: Before: {len(df)}")
    def parse(desc):
        if pd.isna(desc): return ""
        parts = re.split(r'[\|\*\$]', str(desc).lower())
        res = []
        for p in parts:
            p = p.strip()
            if not p: continue
            if '=' in p:
                k, v = p.split('=', 1)
                if k in desc_remove_list: continue
                if prefix: v = v.replace(f"{prefix}_", "")
                res.append(v)
            elif not any(d in p for d in remove_details):
                res.append(p)
        return "_".join(res)
    
    df = df.copy()
    df['event_description'] = df['event_description'].apply(parse)
    print(f"Step 7: After: {len(df)}")
    return df

def step8_export(df, country_code, output_dir):
    df = df.copy()
    df['action_event'] = df.apply(lambda x: f"{x['event_type']}_{x['event_description']}" if x['event_description'] else x['event_type'], axis=1)
    out_path = os.path.join(output_dir, f"{country_code}_processed_logdata.csv")
    df[['SEQID', 'event_type', 'event_description', 'action_event', 'timestamp']].to_csv(out_path, index=False)
    print(f"Step 8: Exported to {out_path}")
    return df

def preprocess_piaac(file_paths, target_booklet, target_item, country_code, output_dir, exclude_keypress=True):
    df = step1_load_and_filter(file_paths, target_booklet, target_item)
    df = step2_restart_correction(df)
    df = step3_multiple_logs(df)
    df = step4_duplicate_removal(df, exclude_keypress)
    df = step5_time_reversal(df)
    df = step6_keypress_aggregation(df)
    df = step7_parsing(df, target_booklet, target_item)
    df = step8_export(df, country_code, output_dir)
    return df

if __name__ == "__main__":
    file_paths = ["US_logdata.txt"]  # List of input file paths
    target_booklet = "PS1"
    target_item = 1
    country_code = "US"
    output_dir = "."
    
    result_df = preprocess_piaac(file_paths, target_booklet, target_item, country_code, output_dir, exclude_keypress=False)