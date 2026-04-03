Write Python code that performs the requested preprocessing operations
on the PIAAC log data provided below.

## Output Format
- Programming language: Python (pandas required)
- Allowed packages: pandas, re, os, numpy. Do NOT use packages outside this list.
- For sequential/iterative logic (e.g., loop-based row scanning,
  index-based access to adjacent rows), prefer standard Python for loops
  or while loops over vectorized pandas operations.
- For simple filtering, grouping, sorting, and I/O, pandas
  methods are encouraged for readability.
- Include comments for each preprocessing step.
- Write the code as a complete, executable script.
- Modularize each preprocessing step as a separate function.

## Execution Format
- Implement each preprocessing step (Steps 1–8) as a separate function.
- Provide a single main function `preprocess_piaac()` that orchestrates
  all step functions in sequence.
- The main function must accept all user-configurable options as parameters
  (e.g., `country_codes`, `target_booklet`, `target_item`, `exclude_keypress`,
  `input_dir`, `output_dir`, etc.).
- **Parameter naming rule**: Function parameter names must NOT be identical
  to DataFrame column names. This prevents ambiguity inside pandas filtering
  where column names and parameter names would collide.
  Use the following mapping:
  | Column name   | Parameter name     |
  |---------------|--------------------|
  | `booklet_id`  | `target_booklet`   |
  | `item_id`     | `target_item`      |
  | `event_type`  | `target_event`     |
  | `SEQID`       | `target_seqid`     |
  Example:
  ```python
  # WRONG — booklet_id refers to both column and parameter → always True
  def step1(df, booklet_id, item_id):
      return df[df['booklet_id'] == booklet_id]

  # CORRECT — parameter name differs from column name
  def step1(df, target_booklet, target_item):
      return df[(df['booklet_id'] == target_booklet) & (df['item_id'] == target_item)]
  ```
- When list-type parameters (`remove_list`, `merge_blocks`, `desc_remove_list`)
  are omitted (`None`), use the default values defined in the Preprocessing Pipeline section.
- The main function returns the final processed DataFrame and exports
  the CSV file (Step 8) as a side effect.

## Constraints
- Do not modify the original DataFrame directly. Use `.copy()` at each step
  to create a new DataFrame.
- Each preprocessing function must accept an input DataFrame and return
  a processed DataFrame.
- Include log messages (`print()`) that report the row count before and after
  each preprocessing step.

## Prohibitions
- Do not arbitrarily delete rows. Only process rows that meet the conditions
  specified in each rule.
- Do not arbitrarily fill or remove missing values (`NaN`).
- Do not use the `inplace=True` parameter anywhere.

## Data Context
### Dataset Information
- Dataset: PIAAC log data
- Description: Process data recording how respondents solve problems
  in an online environment
- Each row represents a single event record.

### Data Structure
```
Column Information:
- CNTRYID: (str) — Country identification code
- SEQID: (int) — Unique person ID
- booklet_id: (str) — Test booklet ID (PS1, PS2)
- item_id: (int) — Individual item ID (1–7)
- event_name: (str) — Location within the interface where the action was performed (taoPIAAC, stimulus)
- event_type: (str) — Major classification of actions performed by users
- timestamp: (float) — Time at which the action was performed (ms)
- event_description: (str) — Specific parameters or targets of the corresponding behavior
```

### Sample Data
```
CNTRYID	SEQID	booklet_id	item_id	event_name	event_type	timestamp	event_description
ZA6712_US.data	10	PS1	1	taoPIAAC	START	0	TEST_TIME=328
ZA6712_US.data	10	PS1	1	stimulus	MAIL_DRAG	32467	id=u01a_item101
ZA6712_US.data	10	PS1	1	stimulus	FOLDER_VIEWED	35562	id=u01a_CanComeFolder
ZA6712_US.data	10	PS1	1	stimulus	MAIL_DROP	35606	target=u01a_CanComeFolder
```

## Preprocessing Pipeline
Perform the following 8 preprocessing steps in the order listed below.
After loading data and selecting the item in Step 1,
Steps 2–7 must be executed strictly in numerical order:
Restart Correction (2) → Multiple Log Entries Handling (3) → Duplicate Removal (4) →
Time Reversal Handling (5) → Keypress Aggregation (6) → Event Description Parsing (7)
Step 8 is executed after Steps 1–7 are complete.

---

#### Step 1: Data Loading and Item Selection
- Input file: `'{{country_code}}_logdata.txt'` format (e.g., `'US_logdata.txt'`)
  Tab-delimited (`'\t'`) text file with the first row as the header (column names).
  Load with `pd.read_csv(..., sep='\t')`.
  Can load a single country file or multiple country files simultaneously.
- Multi-country handling: When loading multiple country files, prepend the country code
  to each country's SEQID for unique identification
  (e.g., US SEQID 10 → `'US_10'`, KR SEQID 43 → `'KR_43'`).
- Item selection: Accept `target_booklet` and `target_item` as function parameters
  and filter to only the data for that item
  (e.g., `target_booklet="PS1"`, `target_item=6`
  → `df[(df['booklet_id'] == target_booklet) & (df['item_id'] == target_item)]`).
- String normalization: After filtering, convert all values in the `'event_type'` column
  to lowercase using `.str.lower()`.
- All subsequent Steps 2–7 are performed only on this filtered data.

#### Step 2: Restart Event Preprocessing
- Condition: When a row with `event_type` value `'restart'` exists within a SEQID.
- Problem: When a restart event occurs, the timestamp resets to 0,
  breaking the cumulative time sequence.
- Processing scope: Each restart's correction applies from the restart row
  up to the next `'end'` event within the same SEQID. If no `'end'` event
  is found after the restart, apply correction to the end of the SEQID group.
- Processing procedure (for each SEQID, iterate over restart rows in order):
  1. Skip condition: If the restart row is the first row or the last row
     of the SEQID group, skip it entirely (do not correct).
  2. Compute the restart row's corrected timestamp:
     - `prev_timestamp`: the already-adjusted timestamp of the immediately
       preceding row (use the adjusted value, not the original, so that
       chained restarts accumulate correctly).
     - `next_original_timestamp`: the **original** (pre-adjustment) timestamp
       of the immediately following row.
     - `restart_timestamp = prev_timestamp + round(next_original_timestamp / 2)`
       (use Python's built-in `round()` for integer rounding).
  3. Find the correction end boundary: Starting from the row after the restart,
     scan forward within the SEQID group for the first row whose `event_type`
     is `'end'`. Record its index as `end_idx`. If no `'end'` row is found,
     set `end_idx` to the last row of the SEQID group.
  4. Apply offset: For every row from (restart_idx + 1) through end_idx,
     set `adjusted_timestamp = restart_timestamp + original_timestamp`.
     Here `original_timestamp` is the value from the raw (pre-adjustment)
     timestamp column for that row.
- Implementation hints:
  - Process each SEQID group with a standard Python for loop over restart indices.
  - Maintain an `adjusted_timestamps` list or numpy array (initialized from the
    original timestamps) so that earlier corrections are visible to later ones.
  - When reading `prev_timestamp`, read from `adjusted_timestamps`
    (already corrected). When reading `next_original_timestamp` and
    the timestamps of rows being offset, read from the **original**
    timestamp column.

#### Step 3: Multiple Log Entries Handling
- Target column: `'event_type'`
- Condition: A single action is recorded across multiple rows (core part + ancillary part).
- Problem: In PIAAC log data, a single action is split into core information and
  ancillary information recorded across multiple rows. This makes it difficult
  to map a representative value for each action in action-level analysis.
- Processing procedure (must be executed in the order below):
  Step A — Simple removal: Remove all rows of ancillary actions that have no analytical
     meaning (e.g., `doaction`) regardless of surrounding context.
     Use `df[~df['event_type'].isin(remove_list)]`.
     Complete simple removal first so that unnecessary rows do not interfere
     with block pattern matching.
  Step B — Block merging: Perform on the resulting DataFrame from Step A.
     When the `event_type` sequence of consecutive rows within the same SEQID
     exactly matches a predefined block pattern with no gaps, replace that
     sequence with a single representative row as specified by the block pattern.
     Matching strategy:
       1. **Sort patterns by length descending** before scanning so that longer
          patterns are attempted first. This prevents a shorter pattern from
          consuming rows that are part of a longer, more specific pattern.
       2. Scan rows left-to-right (ascending row order) within each SEQID.
          At each position, try patterns in the length-sorted order.
          When the first matching block pattern is found, consume those rows
          immediately and continue scanning from the next unconsumed row
          (greedy, non-overlapping).
     When replacing, preserve the metadata of the selected representative action's row as-is.
- Implementation: Allow input of event_types to remove, event blocks to merge,
  and the representative action for each event block.
- Removal list (default):
  ```python
  remove_list = ['doaction', 'next_inquiry', 'next_button', 'confirmation_opened',
                 'confirmation_closed', 'environment', 'mc_help_toolbar', 'mail_sent',
                 'mail_deleted', 'mc_help_menuitem', 'sort_menu', 'copy', 'paste',
                 'new_folder', 'mc_sort', 'delete_folder']
  ```
- Merge list (default):
  ```python
  merge_blocks = [
      {'sequence': ['folder_viewed', 'mail_drop', 'mail_moved'], 'represent': 'mail_drop'},
      {'sequence': ['folder_viewed', 'mail_moved', 'mail_drop'], 'represent': 'mail_drop'},
      {'sequence': ['button', 'mail_moved'], 'represent': 'button'},
      {'sequence': ['button', 'next_item', 'end'], 'represent': 'button'},
      {'sequence': ['breakoff', 'end'], 'represent': 'breakoff'},
  ]
  ```

#### Step 4: Duplicate Action Handling
- Condition: When duplicate rows exist where all column values are identical.
- Processing: Remove completely identical duplicate rows using
  `DataFrame.drop_duplicates()`.
- Exception: When `event_type` is `'keypress'`, simultaneous recordings may occur
  due to rapid input, so provide an option to exclude keypress rows
  from deduplication depending on the analysis purpose.
- Implementation: Make this configurable via an `exclude_keypress=True/False` parameter.
  When `True`, separate keypress rows before dedup, apply `drop_duplicates()` only to
  non-keypress rows, then `pd.concat()` to recombine.

#### Step 5: Time Reversal Handling
- Condition: Within the same SEQID, when timestamps are not in ascending order
  based on row index (i.e., a later-recorded row has a smaller timestamp
  than an earlier-recorded row).
- Processing: Sort all rows within each SEQID group by timestamp in ascending order
  using `DataFrame.sort_values()`.
  The entire row (all columns) is repositioned together, preserving the integrity
  of each row's data. After sorting, reset the row index with `reset_index(drop=True)`.
- Stability: Use a stable sort algorithm (`kind='mergesort'`) so that rows with
  identical timestamps retain their original relative order.

#### Step 6: Keypress Aggregation
- Target: All rows where `event_type` is `'keypress'`.
- Condition: Within the same SEQID, consecutive rows with `event_type` `'keypress'`
  form a keypress block. A single isolated keypress row is also treated
  as a block of size 1.
- Processing procedure:
  1. Identify each block of consecutive keypress rows within the same SEQID.
     Use cumulative grouping
     (e.g., `(df['event_type'] != df['event_type'].shift()).cumsum()`) to detect blocks.
  2. Count the number of rows (N) in each block.
  3. Keep only the first keypress row of each block and overwrite its
     `event_description` with `'count=N'` (use `f'count={N}'`).
  4. Remove all remaining keypress rows in the block.
- Note: Non-consecutive keypress rows (separated by other event_types)
  are treated as independent blocks and processed separately.

#### Step 7: Event Description Parsing and Filtering
- Target column: `'event_description'`
- Split delimiter characters: `'|'`, `'*'`, `'$'`
- Processing procedure:
  1. Convert the `event_description` value to lowercase using `.lower()`.
  2. Split the `event_description` value by the delimiter characters above.
     Use `re.split()` with the regex pattern `r'[|*$]'` (escaped properly:
     `r'[\|\*\$]'`).
  3. Strip leading and trailing whitespace from each split element (`.strip()`).
     Remove any resulting empty strings from the element list.
  4. For each element, branch on whether it contains `'='`:
     **Case A — Element contains `'='`** (or element equals `'end'`):
       a. Split the element by `'='` to get `part1` (variable name) and `part2` (value).
       b. If `part1` is in the removal list (`desc_remove_list`), discard the entire element.
       c. Otherwise, remove the item-related prefix from `part2`.
          The prefix corresponds to the `booklet_id` + `item_id` combination selected in Step 1.
          Remove the prefix followed by `'_'` (e.g., `"u01a_"`) from `part2`
          using `str.replace()` or `re.sub()`.
          If `part2` does not contain the prefix pattern, keep `part2` as-is.
       d. **Keep only `part2`** (the value after `'='`). Discard `part1` and the `'='` sign.
     **Case B — Element does NOT contain `'='`**:
       a. Check whether the element contains any string in the detail removal list
          `remove_details`. Use `any(detail in element for detail in remove_details)`.
       b. If the element matches any entry in `remove_details`, discard it.
       c. Otherwise, keep the element as-is.
  5. Join all remaining kept values (part2 values from Case A, kept elements from Case B)
     into a single string with `'_'` as the separator:
     `'_'.join(filtered_components)`.
  6. If all elements are removed (nothing kept), retain the result as an empty string (`''`).
- Removal list (default):
  ```python
  desc_remove_list = ['test_time', 'end', 'value']
  ```
- Detail removal list (default):
  ```python
  remove_details = ['nan', ',', '.']
  ```
- Prefix mapping:
  ```python
  prefix_map = {
      'PS1_1': 'u01a', 'PS1_2': 'u01b', 'PS1_3': 'u03a',
      'PS1_4': 'u06a', 'PS1_5': 'u06b', 'PS1_6': 'u21',
      'PS1_7': 'u04a', 'PS2_1': 'u19a', 'PS2_2': 'u19b',
      'PS2_3': 'u07',  'PS2_4': 'u02',  'PS2_5': 'u16',
      'PS2_6': 'u11b', 'PS2_7': 'u23'
  }
  ```

#### Step 8: Merge Column and File Export
- Column merging: Split `event_description` by the original delimiters (`'$'`, `'|'`, `'*'`),
  then split each element by `'='` to extract `part2`.
  Join the extracted `part2` values with `'_'` using `'_'.join()`.
  Concatenate `event_type` and the joined `part2` string with `'_'`
  and store in a new column called `'action_event'`.
  If `event_description` is an empty string (`''`), store only `event_type`.
- File export: Save the columns `SEQID`, `event_type`, `event_description`, `action_event`,
  and `timestamp` to a `.csv` file using `DataFrame.to_csv(..., index=False)`.
- Output filename: `'{{country_code}}_processed_logdata.csv'`

## Input/Output Examples

### Step 2: Restart Preprocessing Example
Input:
| SEQID | event_type | timestamp |
|-------|------------|-----------|
| 1     | click      | 8000      |
| 1     | restart    | 0         |
| 1     | click      | 2000      |
| 1     | end        | 3000      |
| 1     | click      | 4000      |

Output:
| SEQID | event_type | timestamp |
|-------|------------|-----------|
| 1     | click      | 8000      |
| 1     | restart    | 9000      |
| 1     | click      | 11000     |
| 1     | end        | 12000     |
| 1     | click      | 4000      |

Explanation:
- restart_timestamp = 8000 + round(2000 / 2) = 9000
- Rows after restart are corrected up to and including the `end` row:
  click → 9000 + 2000 = 11000, end → 9000 + 3000 = 12000
- The row after `end` (click, 4000) is NOT corrected.

### Step 3: Multiple Log Entries — Simple Removal Example
Input:
| SEQID | event_type | timestamp |
|-------|------------|-----------|
| 417   | view       | 2500      |
| 417   | doaction   | 5500      |
| 417   | capture    | 9000      |
| 417   | doaction   | 13000     |

Output:
| SEQID | event_type | timestamp |
|-------|------------|-----------|
| 417   | view       | 2500      |
| 417   | capture    | 9000      |

### Step 3: Multiple Log Entries — Block Merging Example
Input:
| SEQID | event_type    | timestamp |
|-------|---------------|-----------|
| 1202  | click         | 1000      |
| 1202  | folder_viewed | 1800      |
| 1202  | mail_drop     | 6500      |
| 1202  | mail_moved    | 9500      |
| 1202  | cancel        | 12000     |

Output:
| SEQID | event_type | timestamp |
|-------|------------|-----------|
| 1202  | click      | 1000      |
| 1202  | mail_drop  | 6500      |
| 1202  | cancel     | 12000     |

### Step 4: Duplicate Action Handling Example
Input:
| SEQID | event_type | timestamp |
|-------|------------|-----------|
| 309   | click      | 5000      |
| 309   | click      | 5000      |
| 309   | keypress   | 8000      |
| 309   | keypress   | 8000      |

Output (`exclude_keypress=False`):
| SEQID | event_type | timestamp |
|-------|------------|-----------|
| 309   | click      | 5000      |
| 309   | keypress   | 8000      |

Output (`exclude_keypress=True`):
| SEQID | event_type | timestamp |
|-------|------------|-----------|
| 309   | click      | 5000      |
| 309   | keypress   | 8000      |
| 309   | keypress   | 8000      |

### Step 5: Time Reversal Handling Example
Input:
| SEQID | event_type  | event_description | timestamp |
|-------|-------------|-------------------|-----------|
| 713   | mail_drag   | id=item101        | 100       |
| 713   | folder_open | id=folder_a       | 2000      |
| 713   | mail_click  | target=msg5       | 1500      |
| 713   | folder_view | id=folder_b       | 5500      |
| 713   | mail_view   | id=item204        | 5500      |

Output:
| SEQID | event_type  | event_description | timestamp |
|-------|-------------|-------------------|-----------|
| 713   | mail_drag   | id=item101        | 100       |
| 713   | mail_click  | target=msg5       | 1500      |
| 713   | folder_open | id=folder_a       | 2000      |
| 713   | folder_view | id=folder_b       | 5500      |
| 713   | mail_view   | id=item204        | 5500      |

### Step 6: Keypress Aggregation Example
Input:
| SEQID | event_type | event_description | timestamp |
|-------|------------|-------------------|-----------|
| 500   | click      | target=btn1       | 1000      |
| 500   | keypress   | key=a             | 2000      |
| 500   | keypress   | key=b             | 2100      |
| 500   | keypress   | key=c             | 2200      |
| 500   | click      | target=btn2       | 5000      |
| 500   | keypress   | key=x             | 7000      |
| 500   | mail_drag  | id=item101        | 9000      |

Output:
| SEQID | event_type | event_description | timestamp |
|-------|------------|-------------------|-----------|
| 500   | click      | target=btn1       | 1000      |
| 500   | keypress   | count=3           | 2000      |
| 500   | click      | target=btn2       | 5000      |
| 500   | keypress   | count=1           | 7000      |
| 500   | mail_drag  | id=item101        | 9000      |

### Step 7: Event Description Parsing Example
(`target_booklet="PS1"`, `target_item=1`, prefix = `"u01a_"`,
 `desc_remove_list = ['test_time', 'end', 'value']`,
 `remove_details = ['nan', ',', '.']`)

Input:
| SEQID | event_description         |
|-------|---------------------------|
| 1010  | test_time=a$target=bf     |
| 1010  | value=ox                  |
| 1010  | key=dv$index=pir          |
| 1010  | id=u01a_item393           |

Output (values joined with `_`):
| SEQID | event_description |
|-------|-------------------|
| 1010  | bf                |
| 1010  | (empty string)    |
| 1010  | dv_pir            |
| 1010  | item393           |

Explanation:
- Row 1: `test_time=a` → `test_time` is in remove list → discarded.
  `target=bf` → kept, extract `bf`. Result: `"bf"`.
- Row 2: `value=ox` → `value` is in remove list → discarded. Nothing remains.
  Result: `""`.
- Row 3: `key=dv` → kept, extract `dv`. `index=pir` → kept, extract `pir`.
  Result: `"dv_pir"`.
- Row 4: `id=u01a_item393` → kept, extract `u01a_item393`,
  remove prefix `u01a_` → `item393`. Result: `"item393"`.

### Step 8: Merge Column Example
Input:
| SEQID | event_type   | event_description  |
|-------|--------------|--------------------|
| 915   | click_folder | bf                 |
| 915   | menu_button  | (empty string)     |
| 915   | button_file  | dv_pir             |
| 915   | mail_drag    | item393            |

Output:
| SEQID | action_event        |
|-------|---------------------|
| 915   | click_folder_bf     |
| 915   | menu_button         |
| 915   | button_file_dv_pir  |
| 915   | mail_drag_item393   |

## Verification Request
After writing the code, verify that the results match the input/output examples above.
If there are any discrepancies, correct them and submit the final code.

## Pre-Submission Checklist (mandatory)

Before outputting the final code, execute every check below and fix
any issues found. Do not skip any item.

1. **Syntax integrity**: Verify all parentheses, brackets, braces,
   and quotes are properly paired and closed.
2. **Imports**: For every function or module used in the code body,
   confirm it is imported at the top of the script
   (e.g., `import pandas as pd`, `import re`, `import os`, `import numpy as np`).
3. **Function call signatures**: For every function call — built-in,
   library, or user-defined — verify:
   - The number of arguments matches the function definition.
   - All keyword argument names are valid
     (e.g., `df.sort_values(by=...)` not `columns=...`).
4. **Method-object compatibility**: Confirm that each method is valid
   for the object type it is called on
   (e.g., `.str.lower()` on a pandas Series, not a Python scalar).
5. **Variable scope**: Every variable referenced inside a function
   must be either a parameter, locally defined, or an imported
   constant. No undefined or unintended global variables.
6. **Type consistency**: No operations on incompatible types
   (e.g., string + integer without conversion;
   `NaN` comparison with `==` instead of `pd.isna()`).
7. **Column names**: Only use column names specified in the Data
   Structure section. Do not invent new names except `'action_event'`.
8. **Copy rule**: Each step function operates on a `.copy()` of the input,
   not the original.
9. **No `inplace=True`**: Confirm `inplace=True` does not appear anywhere.
10. **Step order and data flow**: Steps are called in exact order
    1→2→3→4→5→6→7→8; each step's output is the next step's input.
11. **Return value**: Every step function returns a DataFrame.
12. **Default values**: When list-type parameters are `None`,
    the function body assigns the default values from the prompt.
13. **Log messages**: Every step function prints row count before
    and after processing.
14. **Parameter naming**: Verify that no function parameter name is identical
    to a DataFrame column name (`booklet_id`, `item_id`, `event_type`, `SEQID`).
    Use the mapping table in the Execution Format section.
15. **Loop-based processing**: For Steps 2 (restart correction) and
    3B (block merging), verify that row-by-row scanning uses explicit
    for/while loops, not vectorized pandas operations that may fail
    on sequential dependencies.
