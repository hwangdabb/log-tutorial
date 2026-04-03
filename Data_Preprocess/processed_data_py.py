# ### Data Preprocessing Code
#
# This script implements the common preprocessing pipeline described in the Data Preprocessing section of the paper.
#
# It covers three main stages: (1) correcting timestamps disrupted by RESTART events, (2) reducing redundant action patterns into single representative events, and (3) merging consecutive KEYPRESS events and combining event types with their descriptions into unified action labels.
#
# The code processes log data from the PS1_1 item (U.S. sample) of the PIAAC Problem Solving in Technology-Rich Environments (PS-TRE) assessment.
#
# An equivalent R implementation (processed_data_r.R) is also provided for reproducibility across languages.

import pandas as pd
import numpy as np
import os
import pickle
import json
import re
import ast
from typing import Dict, List, Tuple, Union
from collections import defaultdict, Counter
from itertools import product

## Function Declarations

def fix_restart_timestamps(df):
    """Fix timestamps for RESTART events by accumulating offsets.

    For each SEQID group, recalculates timestamps after RESTART events
    so that they continue from the previous event's adjusted timestamp.

    Parameters:
        df (DataFrame): Must contain SEQID, event_type, timestamp columns.

    Returns:
        DataFrame: DataFrame with adjusted timestamps.
    """
    df = df.copy()
    df['_original_index'] = range(len(df))

    result_dfs = []

    for _, group in df.groupby('SEQID', sort=False):
        group = group.sort_values('_original_index').reset_index(drop=True)
        adjusted_timestamps = group['timestamp'].astype(float).copy()

        restart_indices = group[group['event_type'] == 'RESTART'].index.tolist()

        for restart_idx in restart_indices:
            if 0 < restart_idx < len(group) - 1:
                prev_timestamp = adjusted_timestamps.iloc[restart_idx - 1]
                next_original_timestamp = group['timestamp'].iloc[restart_idx + 1]

                # RESTART timestamp = previous + (next_original / 2)
                restart_timestamp = prev_timestamp + (next_original_timestamp / 2).round()
                adjusted_timestamps.iloc[restart_idx] = restart_timestamp

                # Find the next END event after RESTART
                end_idx = None
                for i in range(restart_idx + 1, len(group)):
                    if group['event_type'].iloc[i] == 'END':
                        end_idx = i
                        break

                if end_idx is None:
                    end_idx = len(group) - 1

                # Accumulate timestamps from RESTART to END
                offset = restart_timestamp
                for i in range(restart_idx + 1, end_idx + 1):
                    adjusted_timestamps.iloc[i] = offset + group['timestamp'].iloc[i]

        group.loc[:, 'timestamp'] = adjusted_timestamps
        result_dfs.append(group)

    result = pd.concat(result_dfs, ignore_index=True)
    result = result.sort_values('_original_index').drop('_original_index', axis=1).reset_index(drop=True)

    return result


def count_sequences(df, time_window=100):
    """Count event combinations within a time window per SEQID.

    Parameters:
        df (DataFrame): Must contain SEQID, event_type, timestamp columns.
        time_window (int): Maximum time gap for grouping events (default: 100).

    Returns:
        Counter: Counts of each event-type combination tuple.
    """
    all_combinations = []

    for _, group_df in df.groupby('SEQID'):
        group_df = group_df.reset_index(drop=True)

        time_groups = []
        current_group = [0]

        for i in range(1, len(group_df)):
            if group_df.loc[i, 'timestamp'] - group_df.loc[current_group[0], 'timestamp'] <= time_window:
                current_group.append(i)
            else:
                time_groups.append(current_group)
                current_group = [i]
        time_groups.append(current_group)

        for grp in time_groups:
            combo = tuple(group_df.loc[idx, 'event_type'] for idx in grp)
            all_combinations.append(combo)

    return Counter(all_combinations)


def merge_patterns(df, pattern_map):
    """Merge rows matching specific event-type patterns into a single row.

    Parameters:
        df (DataFrame): Must contain SEQID, event_type columns.
        pattern_map (dict): Maps event-type tuples to the event_type to keep.
            e.g., {('A', 'B', 'C'): 'B', ...}

    Returns:
        DataFrame: DataFrame with matched patterns collapsed.
    """
    # Sort patterns by length (longest first) once before iteration
    sorted_patterns = sorted(pattern_map.items(), key=lambda x: -len(x[0]))

    result_rows = []

    for _, group_df in df.groupby('SEQID'):
        group_df = group_df.reset_index(drop=True)
        events = group_df['event_type'].tolist()

        i = 0
        while i < len(events):
            matched = False

            for pattern, keep_type in sorted_patterns:
                pattern_len = len(pattern)
                if i + pattern_len <= len(events):
                    if tuple(events[i:i+pattern_len]) == pattern:
                        for j in range(pattern_len):
                            if events[i+j] == keep_type:
                                result_rows.append(group_df.iloc[i+j])
                                break
                        i += pattern_len
                        matched = True
                        break

            if not matched:
                result_rows.append(group_df.iloc[i])
                i += 1

    return pd.DataFrame(result_rows).reset_index(drop=True)


def replace_pattern(df, pattern, new_event_type, new_event_description):
    """Replace rows matching a pattern with a single row having new event values.

    Parameters:
        df (DataFrame): Must contain SEQID, event_type, event_description columns.
        pattern (tuple): Event-type sequence to match, e.g., ('A', 'B', 'C').
        new_event_type (str): Replacement event_type value.
        new_event_description (str): Replacement event_description value.

    Returns:
        DataFrame: DataFrame with matched patterns replaced.
    """
    result_rows = []
    pattern_len = len(pattern)

    for _, group_df in df.groupby('SEQID'):
        group_df = group_df.reset_index(drop=True)
        events = group_df['event_type'].tolist()

        i = 0
        while i < len(events):
            if i + pattern_len <= len(events):
                if tuple(events[i:i+pattern_len]) == pattern:
                    first_row = group_df.iloc[i].copy()
                    first_row['event_type'] = new_event_type
                    first_row['event_description'] = new_event_description
                    result_rows.append(first_row)
                    i += pattern_len
                    continue

            result_rows.append(group_df.iloc[i])
            i += 1

    return pd.DataFrame(result_rows).reset_index(drop=True)


def preprocess_keypress_data(df):
    """Merge consecutive KEYPRESS events into a single row with count.

    For each SEQID group, collapses consecutive KEYPRESS rows into one row
    with event_description set to 'count=N'.

    Parameters:
        df (DataFrame): Must contain SEQID, event_type, timestamp columns.

    Returns:
        DataFrame: DataFrame with consecutive KEYPRESSes collapsed.
    """
    processed_groups = []

    for _, seqid_data in df.groupby('SEQID', sort=False):
        seqid_data = seqid_data.sort_values('timestamp').reset_index(drop=True)

        result_rows = []
        i = 0

        while i < len(seqid_data):
            current_row = seqid_data.iloc[i].copy()

            if current_row['event_type'] == 'KEYPRESS':
                keypress_count = 1
                j = i + 1

                while j < len(seqid_data) and seqid_data.iloc[j]['event_type'] == 'KEYPRESS':
                    keypress_count += 1
                    j += 1

                current_row['event_description'] = f'count={keypress_count}'
                result_rows.append(current_row)
                i = j
            else:
                result_rows.append(current_row)
                i += 1

        if result_rows:
            processed_groups.append(pd.DataFrame(result_rows))

    result_df = pd.concat(processed_groups, ignore_index=True)

    print(f"Processed data size: {len(result_df)} rows")
    print(f"Removed rows: {len(df) - len(result_df)}")

    return result_df


# Constants for process_event_description
_REMOVE_VARS = {'test_time', 'end', 'value'}
_REMOVE_DETAILS = {'nan', ',', '.'}
_ITEM_PREFIX = 'u01a_'


def process_event_description(description):
    """Parse event_description by splitting on special characters and filtering.

    Args:
        description (str): Raw event_description string.

    Returns:
        str: Filtered and joined description components.
    """
    components = re.split(r'[|*$]', str(description))
    components = [comp.strip() for comp in components if comp.strip()]

    filtered_components = []

    for comp in components:
        if '=' in comp:
            var_name, _, detailed = comp.partition('=')
            var_name = var_name.strip()
            detailed = detailed.strip()

            if var_name not in _REMOVE_VARS:
                detailed = detailed.replace(_ITEM_PREFIX, '')
                filtered_components.append(detailed)
        elif comp == 'end':
            continue
        else:
            if not any(k in comp for k in _REMOVE_DETAILS):
                filtered_components.append(comp)

    return '_'.join(filtered_components)

if __name__ == "__main__":
    # #### PS1_1
    #
    # Select a target item and perform item-specific preprocessing

    problem_num = 'ps1_1'
    file = 'us_' + problem_num + '.txt'
    file_path = './00_Data/' + file
    path_1st = f'./00_Data/us_{problem_num}.pkl'

    data = pd.read_csv(file_path, sep='\t')
    data.drop(columns=['SEQID'], inplace=True)
    data.rename(columns={'SEQID_unify': 'SEQID'}, inplace=True)

    # Fix RESTART event timestamps
    data = fix_restart_timestamps(data)

    # Remove irrelevant event types for PS1_1
    data = data[~data['event_type'].isin([
        'DOACTION', 'NEXT_INQUIRY', 'NEXT_BUTTON', 'CONFIRMATION_OPENED', 'CONFIRMATION_CLOSED',
        'ENVIRONMENT', 'MC_HELP_TOOLBAR', 'MAIL_SENT', 'MAIL_DELETED',
        'MC_HELP_MENUITEM', 'SORT_MENU', 'COPY', 'PASTE',
        'NEW_FOLDER', 'MC_SORT', 'DELETE_FOLDER'
    ])].copy()

    # Merge multi-row action patterns into single rows
    pattern_map = {
        ('FOLDER_VIEWED', 'MAIL_DROP', 'MAIL_MOVED'): 'MAIL_DROP',
        ('FOLDER_VIEWED', 'MAIL_MOVED', 'MAIL_DROP'): 'MAIL_DROP',
        ('BUTTON', 'MAIL_MOVED'): 'BUTTON',
        ('BUTTON', 'NEXT_ITEM', 'END'): 'BUTTON',
        ('BREAKOFF', 'END'): 'BREAKOFF'
    }

    df_merged = merge_patterns(data, pattern_map)
    df = df_merged.drop(columns=['CNTRYID', 'booklet_id', 'item_id', 'event_name'])

    # Remove duplicate rows
    df.drop_duplicates(keep='first', inplace=True)

    # Merge consecutive KEYPRESS events
    fin_data = preprocess_keypress_data(df)
    fin_data.reset_index(drop=True, inplace=True)

    fin_data.head()

    # Merge processed event_type and event_description into a single value

    fin_data.event_type = fin_data.event_type.str.lower()
    fin_data.event_description = fin_data.event_description.str.lower()

    fin_data['event_desc_list'] = fin_data['event_description'].apply(process_event_description).copy()

    # Combine event_type and parsed description into a single action label
    fin_data["action_event"] = np.where(
        fin_data["event_desc_list"] == "",
        fin_data["event_type"],
        fin_data["event_type"] + "_" + fin_data["event_desc_list"]
    )

    result = fin_data[['SEQID', 'event_type', 'event_description', 'action_event', 'timestamp']]

    path_2nd = f'./00_Data/merged_{problem_num}.csv'

    result.to_csv(path_2nd)

