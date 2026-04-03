# ================================================================
# 생성 모델: anthropic/claude-sonnet-4.6
# 응답 시간: 86.86s
# 입력 토큰: 7696
# 출력 토큰: 7359
# ================================================================

# =============================================================================
# PIAAC Log Data Preprocessing Pipeline
# =============================================================================

library(dplyr)
library(tidyr)
library(stringr)
library(readr)
library(purrr)

# =============================================================================
# Step 1: Data Loading and Item Selection
# =============================================================================

step1_load_and_filter <- function(country_codes, target_booklet, target_item,
                                   input_dir = ".") {
  # Load one or more country files, prepend country code to SEQID for multi-country,
  # filter to target booklet and item, and normalize event_type to lowercase.

  cat(sprintf("[Step 1] Loading data for country/countries: %s\n",
              paste(country_codes, collapse = ", ")))

  all_data <- list()

  for (cc in country_codes) {
    file_path <- file.path(input_dir, paste0(cc, "_logdata.txt"))
    cat(sprintf("[Step 1] Reading file: %s\n", file_path))

    # Read tab-delimited file; read_tsv uses tab by default
    raw_df <- read_tsv(file_path, col_types = cols(
      CNTRYID          = col_character(),
      SEQID            = col_character(),
      booklet_id       = col_character(),
      item_id          = col_double(),
      event_name       = col_character(),
      event_type       = col_character(),
      timestamp        = col_double(),
      event_description = col_character()
    ), show_col_types = FALSE)

    # Prepend country code to SEQID for unique identification across countries
    raw_df <- raw_df %>%
      mutate(SEQID = paste0(cc, "_", SEQID))

    all_data[[cc]] <- raw_df
  }

  # Combine all country data
  combined_df <- bind_rows(all_data)

  row_before <- nrow(combined_df)
  cat(sprintf("[Step 1] Rows before item selection: %d\n", row_before))

  # Filter to target booklet and item
  filtered_df <- combined_df %>%
    filter(booklet_id == target_booklet, item_id == target_item)

  # Normalize event_type to lowercase
  filtered_df <- filtered_df %>%
    mutate(event_type = str_to_lower(event_type))

  row_after <- nrow(filtered_df)
  cat(sprintf("[Step 1] Rows after item selection (booklet=%s, item=%s): %d\n",
              target_booklet, target_item, row_after))

  return(filtered_df)
}

# =============================================================================
# Step 2: Restart Event Preprocessing
# =============================================================================

step2_restart_correction <- function(df_in) {
  # Correct timestamps after restart events within each SEQID.
  # For each restart row (not first/last in group), compute a corrected timestamp
  # and offset all rows from restart+1 through the next 'end' row.

  df_out <- df_in

  row_before <- nrow(df_out)
  cat(sprintf("[Step 2] Rows before restart correction: %d\n", row_before))

  # Get unique SEQIDs
  seqids <- unique(df_out$SEQID)

  # We will rebuild the timestamp column after processing
  # Work on a copy with row indices preserved
  df_out <- df_out %>% mutate(.row_id = row_number())

  # Process each SEQID group
  for (sid in seqids) {
    # Get indices (in df_out) for this SEQID
    grp_idx <- which(df_out$SEQID == sid)

    if (length(grp_idx) < 2) next  # Need at least 2 rows to do anything

    # Extract original timestamps for this group
    orig_ts <- df_out$timestamp[grp_idx]
    event_types_grp <- df_out$event_type[grp_idx]

    # Initialize adjusted timestamps from original
    adj_ts <- orig_ts

    # Find positions of 'restart' within the group (local indices 1-based)
    restart_local <- which(event_types_grp == "restart")

    if (length(restart_local) == 0) next

    # Process each restart in order
    for (ri in restart_local) {
      n_grp <- length(grp_idx)

      # Skip if restart is the first or last row of the group
      if (ri == 1 || ri == n_grp) next

      # prev_timestamp: already-adjusted timestamp of immediately preceding row
      prev_ts <- adj_ts[ri - 1]

      # next_original_timestamp: original timestamp of immediately following row
      next_orig_ts <- orig_ts[ri + 1]

      # Compute restart corrected timestamp
      restart_ts <- prev_ts + round(next_orig_ts / 2)

      # Update the restart row's adjusted timestamp
      adj_ts[ri] <- restart_ts

      # Find end boundary: first 'end' event after restart within group
      end_idx_local <- NA
      for (j in (ri + 1):n_grp) {
        if (event_types_grp[j] == "end") {
          end_idx_local <- j
          break
        }
      }
      if (is.na(end_idx_local)) {
        end_idx_local <- n_grp
      }

      # Apply offset to rows from (ri+1) through end_idx_local
      for (j in (ri + 1):end_idx_local) {
        adj_ts[j] <- restart_ts + orig_ts[j]
      }
    }

    # Write adjusted timestamps back to df_out
    df_out$timestamp[grp_idx] <- adj_ts
  }

  # Remove helper column
  df_out <- df_out %>% select(-.row_id)

  row_after <- nrow(df_out)
  cat(sprintf("[Step 2] Rows after restart correction: %d\n", row_after))

  return(df_out)
}

# =============================================================================
# Step 3: Multiple Log Entries Handling
# =============================================================================

step3_multiple_log_handling <- function(df_in,
                                         remove_list = NULL,
                                         merge_blocks = NULL) {
  # Step A: Remove ancillary rows by event_type.
  # Step B: Merge consecutive block patterns within each SEQID.

  df_out <- df_in

  # Default removal list
  if (is.null(remove_list)) {
    remove_list <- c("doaction", "next_inquiry", "next_button",
                     "confirmation_opened", "confirmation_closed",
                     "environment", "mc_help_toolbar", "mail_sent",
                     "mail_deleted", "mc_help_menuitem", "sort_menu",
                     "copy", "paste", "new_folder", "mc_sort", "delete_folder")
  }

  # Default merge blocks
  if (is.null(merge_blocks)) {
    merge_blocks <- list(
      list(sequence = c("folder_viewed", "mail_drop", "mail_moved"),  represent = "mail_drop"),
      list(sequence = c("folder_viewed", "mail_moved", "mail_drop"),  represent = "mail_drop"),
      list(sequence = c("button", "mail_moved"),                       represent = "button"),
      list(sequence = c("button", "next_item", "end"),                 represent = "button"),
      list(sequence = c("breakoff", "end"),                            represent = "breakoff")
    )
  }

  row_before <- nrow(df_out)
  cat(sprintf("[Step 3] Rows before multiple log handling: %d\n", row_before))

  # --- Step A: Simple removal ---
  df_out <- df_out %>%
    filter(!event_type %in% remove_list)

  cat(sprintf("[Step 3] Rows after Step A (simple removal): %d\n", nrow(df_out)))

  # --- Step B: Block merging ---
  # Sort patterns by length descending so longer patterns are tried first
  merge_blocks_sorted <- merge_blocks[order(
    sapply(merge_blocks, function(b) length(b$sequence)),
    decreasing = TRUE
  )]

  # Process each SEQID group
  seqids <- unique(df_out$SEQID)

  result_list <- list()

  for (sid in seqids) {
    grp_df <- df_out %>% filter(SEQID == sid)
    n <- nrow(grp_df)

    if (n == 0) next

    # We'll build a logical vector of rows to keep, and track representative rows
    keep <- rep(TRUE, n)
    # For rows that are "representative" of a merged block, we keep them as-is.
    # For rows that are consumed (non-representative), we mark keep = FALSE.

    i <- 1
    while (i <= n) {
      matched <- FALSE

      # Try each pattern (already sorted by length desc)
      for (blk in merge_blocks_sorted) {
        seq_len <- length(blk$sequence)
        represent <- blk$represent

        # Check if pattern fits starting at position i
        if (i + seq_len - 1 > n) next

        # Check if all rows in the window are still "active" (not already consumed)
        window_indices <- i:(i + seq_len - 1)
        if (!all(keep[window_indices])) next

        # Check event_type match
        window_types <- grp_df$event_type[window_indices]
        if (!identical(window_types, blk$sequence)) next

        # Match found: find the representative row index within the window
        rep_local <- which(window_types == represent)
        if (length(rep_local) == 0) {
          # If representative not found in window, use first row
          rep_local <- 1
        } else {
          rep_local <- rep_local[1]
        }
        rep_idx <- window_indices[rep_local]

        # Mark all window rows as not kept, then re-mark representative
        keep[window_indices] <- FALSE
        keep[rep_idx] <- TRUE

        # Advance i past the consumed block
        i <- i + seq_len
        matched <- TRUE
        break
      }

      if (!matched) {
        i <- i + 1
      }
    }

    result_list[[as.character(sid)]] <- grp_df[keep, ]
  }

  df_out <- bind_rows(result_list)

  row_after <- nrow(df_out)
  cat(sprintf("[Step 3] Rows after Step B (block merging): %d\n", row_after))

  return(df_out)
}

# =============================================================================
# Step 4: Duplicate Action Handling
# =============================================================================

step4_duplicate_removal <- function(df_in, exclude_keypress = TRUE) {
  # Remove completely identical duplicate rows.
  # If exclude_keypress = TRUE, keypress rows are excluded from deduplication
  # (they may have legitimate duplicates due to rapid input).

  df_out <- df_in

  row_before <- nrow(df_out)
  cat(sprintf("[Step 4] Rows before duplicate removal: %d\n", row_before))

  if (exclude_keypress) {
    # Separate keypress and non-keypress rows
    kp_rows    <- df_out %>% filter(event_type == "keypress")
    non_kp_rows <- df_out %>% filter(event_type != "keypress")

    # Apply distinct only to non-keypress rows
    non_kp_dedup <- non_kp_rows %>% distinct()

    # Recombine, preserving original order as best as possible
    # We use bind_rows and then re-sort by original row order if needed.
    # Since we need to preserve relative order, we add a row index before splitting.
    df_out_indexed <- df_in %>% mutate(.orig_order = row_number())

    kp_indexed    <- df_out_indexed %>% filter(event_type == "keypress")
    non_kp_indexed <- df_out_indexed %>% filter(event_type != "keypress") %>% distinct()

    df_out <- bind_rows(kp_indexed, non_kp_indexed) %>%
      arrange(.orig_order) %>%
      select(-.orig_order)

  } else {
    df_out <- df_out %>% distinct()
  }

  row_after <- nrow(df_out)
  cat(sprintf("[Step 4] Rows after duplicate removal (exclude_keypress=%s): %d\n",
              exclude_keypress, row_after))

  return(df_out)
}

# =============================================================================
# Step 5: Time Reversal Handling
# =============================================================================

step5_time_reversal <- function(df_in) {
  # Sort rows within each SEQID by timestamp ascending.
  # Rows with identical timestamps retain their original relative order (stable sort).

  df_out <- df_in

  row_before <- nrow(df_out)
  cat(sprintf("[Step 5] Rows before time reversal handling: %d\n", row_before))

  # Add original order index for stable sort
  df_out <- df_out %>%
    mutate(.orig_order = row_number()) %>%
    arrange(SEQID, timestamp, .orig_order) %>%
    select(-.orig_order)

  row_after <- nrow(df_out)
  cat(sprintf("[Step 5] Rows after time reversal handling: %d\n", row_after))

  return(df_out)
}

# =============================================================================
# Step 6: Keypress Aggregation
# =============================================================================

step6_keypress_aggregation <- function(df_in) {
  # Within each SEQID, aggregate consecutive keypress rows into a single row.
  # The first row of each keypress block is kept; its event_description is
  # replaced with 'count=N' where N is the block size.

  df_out <- df_in

  row_before <- nrow(df_out)
  cat(sprintf("[Step 6] Rows before keypress aggregation: %d\n", row_before))

  seqids <- unique(df_out$SEQID)

  result_list <- list()

  for (sid in seqids) {
    grp_df <- df_out %>% filter(SEQID == sid)
    n <- nrow(grp_df)

    if (n == 0) next

    et <- grp_df$event_type

    # Identify consecutive keypress blocks using run-length encoding
    rle_result <- rle(et)
    rle_lengths <- rle_result$lengths
    rle_values  <- rle_result$values

    # Build a block ID for each row
    block_id <- rep(seq_along(rle_lengths), times = rle_lengths)

    # Determine which rows to keep and what their event_description should be
    keep <- rep(TRUE, n)
    new_desc <- grp_df$event_description  # copy

    row_ptr <- 1
    for (b in seq_along(rle_lengths)) {
      blen <- rle_lengths[b]
      bval <- rle_values[b]
      block_rows <- row_ptr:(row_ptr + blen - 1)

      if (bval == "keypress") {
        # Keep only the first row of the block
        if (blen > 1) {
          keep[block_rows[-1]] <- FALSE
        }
        # Update event_description of the first row
        new_desc[block_rows[1]] <- paste0("count=", blen)
      }

      row_ptr <- row_ptr + blen
    }

    grp_df$event_description <- new_desc
    result_list[[as.character(sid)]] <- grp_df[keep, ]
  }

  df_out <- bind_rows(result_list)

  row_after <- nrow(df_out)
  cat(sprintf("[Step 6] Rows after keypress aggregation: %d\n", row_after))

  return(df_out)
}

# =============================================================================
# Step 7: Event Description Parsing and Filtering
# =============================================================================

step7_parse_event_description <- function(df_in,
                                           target_booklet,
                                           target_item,
                                           desc_remove_list = NULL,
                                           remove_details = NULL) {
  # Parse and filter event_description values:
  # 1. Lowercase
  # 2. Split by |, *, $
  # 3. Trim whitespace, remove empty strings
  # 4. For elements with '=': extract part2, remove if part1 in desc_remove_list,
  #    strip item prefix from part2
  # 5. For elements without '=': remove if matches remove_details
  # 6. Join remaining values with '_'

  df_out <- df_in

  # Default removal lists
  if (is.null(desc_remove_list)) {
    desc_remove_list <- c("test_time", "end", "value")
  }
  if (is.null(remove_details)) {
    remove_details <- c("nan", ",", ".")
  }

  # Prefix map
  prefix_map <- c(
    "PS1_1" = "u01a", "PS1_2" = "u01b", "PS1_3" = "u03a",
    "PS1_4" = "u06a", "PS1_5" = "u06b", "PS1_6" = "u21",
    "PS1_7" = "u04a", "PS2_1" = "u19a", "PS2_2" = "u19b",
    "PS2_3" = "u07",  "PS2_4" = "u02",  "PS2_5" = "u16",
    "PS2_6" = "u11b", "PS2_7" = "u23"
  )

  # Determine prefix for this booklet/item combination
  key <- paste0(target_booklet, "_", target_item)
  item_prefix <- if (key %in% names(prefix_map)) {
    paste0(prefix_map[[key]], "_")
  } else {
    ""
  }

  row_before <- nrow(df_out)
  cat(sprintf("[Step 7] Rows before event description parsing: %d\n", row_before))
  cat(sprintf("[Step 7] Using prefix: '%s'\n", item_prefix))

  # Helper function to parse a single event_description string
  parse_desc <- function(desc_val) {
    if (is.na(desc_val)) return("")

    # 1. Lowercase
    desc_lower <- str_to_lower(desc_val)

    # 2. Split by |, *, $
    parts <- str_split(desc_lower, "[\\|\\*\\$]")[[1]]

    # 3. Trim whitespace and remove empty strings
    parts <- str_trim(parts)
    parts <- parts[nchar(parts) > 0]

    if (length(parts) == 0) return("")

    # 4 & 5. Process each part
    kept_values <- character(0)

    for (p in parts) {
      if (str_detect(p, "=")) {
        # Case A: contains '='
        eq_pos <- str_locate(p, "=")[1, "start"]
        part1 <- substr(p, 1, eq_pos - 1)
        part2 <- substr(p, eq_pos + 1, nchar(p))

        # If part1 is in desc_remove_list, discard
        if (part1 %in% desc_remove_list) next

        # Remove item prefix from part2
        if (nchar(item_prefix) > 0 && str_detect(part2, fixed(item_prefix))) {
          part2 <- str_replace_all(part2, fixed(item_prefix), "")
        }

        # Keep part2
        kept_values <- c(kept_values, part2)

      } else {
        # Case B: no '='
        # Check if element matches any entry in remove_details
        discard <- any(sapply(remove_details, function(rd) {
          grepl(rd, p, fixed = TRUE)
        }))

        if (!discard) {
          kept_values <- c(kept_values, p)
        }
      }
    }

    # 5. Join with '_'
    if (length(kept_values) == 0) {
      return("")
    } else {
      return(paste(kept_values, collapse = "_"))
    }
  }

  # Apply parsing to each row
  df_out <- df_out %>%
    mutate(event_description = sapply(event_description, parse_desc))

  row_after <- nrow(df_out)
  cat(sprintf("[Step 7] Rows after event description parsing: %d\n", row_after))

  return(df_out)
}

# =============================================================================
# Step 8: Merge Column and File Export
# =============================================================================

step8_merge_and_export <- function(df_in,
                                    country_codes,
                                    output_dir = ".") {
  # Create action_event column by combining event_type and parsed event_description.
  # Export selected columns to CSV.

  df_out <- df_in

  row_before <- nrow(df_out)
  cat(sprintf("[Step 8] Rows before merge and export: %d\n", row_before))

  # Helper: parse event_description for action_event construction
  # Split by $, |, * then split each by = to get part2, join with _
  build_action_event <- function(ev_type, ev_desc) {
    if (is.na(ev_desc) || ev_desc == "") {
      return(ev_type)
    }

    # Split by original delimiters
    parts <- str_split(ev_desc, "[\\|\\*\\$]")[[1]]
    parts <- str_trim(parts)
    parts <- parts[nchar(parts) > 0]

    if (length(parts) == 0) {
      return(ev_type)
    }

    # For each part, split by '=' and take part2 if exists, else keep as-is
    extracted <- sapply(parts, function(p) {
      if (str_detect(p, "=")) {
        eq_pos <- str_locate(p, "=")[1, "start"]
        substr(p, eq_pos + 1, nchar(p))
      } else {
        p
      }
    }, USE.NAMES = FALSE)

    extracted <- extracted[nchar(extracted) > 0]

    if (length(extracted) == 0) {
      return(ev_type)
    }

    joined <- paste(extracted, collapse = "_")
    return(paste0(ev_type, "_", joined))
  }

  # Apply to each row
  df_out <- df_out %>%
    mutate(action_event = mapply(build_action_event, event_type, event_description,
                                  SIMPLIFY = TRUE, USE.NAMES = FALSE))

  # Select output columns
  df_export <- df_out %>%
    select(SEQID, event_type, event_description, action_event, timestamp)

  # Determine output filename
  # If multiple countries, join them; otherwise use single code
  cc_str <- paste(country_codes, collapse = "_")
  out_filename <- paste0(cc_str, "_processed_logdata.csv")
  out_path <- file.path(output_dir, out_filename)

  # Export to CSV
  write_csv(df_export, out_path)

  row_after <- nrow(df_export)
  cat(sprintf("[Step 8] Rows exported: %d\n", row_after))
  cat(sprintf("[Step 8] Output saved to: %s\n", out_path))

  return(df_out)
}

# =============================================================================
# Main Preprocessing Function
# =============================================================================

preprocess_piaac <- function(
    country_codes,
    target_booklet,
    target_item,
    exclude_keypress = TRUE,
    input_dir        = ".",
    output_dir       = ".",
    remove_list      = NULL,
    merge_blocks     = NULL,
    desc_remove_list = NULL,
    remove_details   = NULL
) {
  # Orchestrates all 8 preprocessing steps in sequence.
  #
  # Parameters:
  #   country_codes    : character vector of country codes (e.g., c("US", "KR"))
  #   target_booklet   : booklet ID to filter (e.g., "PS1")
  #   target_item      : item ID to filter (e.g., 6)
  #   exclude_keypress : logical; if TRUE, keypress rows excluded from dedup (Step 4)
  #   input_dir        : directory containing input log files
  #   output_dir       : directory for output CSV file
  #   remove_list      : character vector of event_types to remove in Step 3A (NULL = default)
  #   merge_blocks     : list of block patterns for Step 3B (NULL = default)
  #   desc_remove_list : character vector of part1 keys to remove in Step 7 (NULL = default)
  #   remove_details   : character vector of substrings to remove in Step 7 Case B (NULL = default)

  cat("=============================================================\n")
  cat("  PIAAC Log Data Preprocessing Pipeline\n")
  cat(sprintf("  Countries: %s | Booklet: %s | Item: %s\n",
              paste(country_codes, collapse = ", "), target_booklet, target_item))
  cat("=============================================================\n")

  # Step 1: Load and filter
  cat("\n--- Step 1: Data Loading and Item Selection ---\n")
  df1 <- step1_load_and_filter(
    country_codes  = country_codes,
    target_booklet = target_booklet,
    target_item    = target_item,
    input_dir      = input_dir
  )

  # Step 2: Restart correction
  cat("\n--- Step 2: Restart Event Preprocessing ---\n")
  df2 <- step2_restart_correction(df_in = df1)

  # Step 3: Multiple log entries handling
  cat("\n--- Step 3: Multiple Log Entries Handling ---\n")
  df3 <- step3_multiple_log_handling(
    df_in        = df2,
    remove_list  = remove_list,
    merge_blocks = merge_blocks
  )

  # Step 4: Duplicate removal
  cat("\n--- Step 4: Duplicate Action Handling ---\n")
  df4 <- step4_duplicate_removal(
    df_in            = df3,
    exclude_keypress = exclude_keypress
  )

  # Step 5: Time reversal handling
  cat("\n--- Step 5: Time Reversal Handling ---\n")
  df5 <- step5_time_reversal(df_in = df4)

  # Step 6: Keypress aggregation
  cat("\n--- Step 6: Keypress Aggregation ---\n")
  df6 <- step6_keypress_aggregation(df_in = df5)

  # Step 7: Event description parsing
  cat("\n--- Step 7: Event Description Parsing and Filtering ---\n")
  df7 <- step7_parse_event_description(
    df_in            = df6,
    target_booklet   = target_booklet,
    target_item      = target_item,
    desc_remove_list = desc_remove_list,
    remove_details   = remove_details
  )

  # Step 8: Merge column and export
  cat("\n--- Step 8: Merge Column and File Export ---\n")
  df8 <- step8_merge_and_export(
    df_in         = df7,
    country_codes = country_codes,
    output_dir    = output_dir
  )

  cat("\n=============================================================\n")
  cat("  Preprocessing complete.\n")
  cat(sprintf("  Final row count: %d\n", nrow(df8)))
  cat("=============================================================\n")

  return(df8)
}

# =============================================================================
# Example Usage (uncomment to run)
# =============================================================================

result <- preprocess_piaac(
  country_codes    = c("US"),
  target_booklet   = "PS1",
  target_item      = 1,
  exclude_keypress = FALSE,
  input_dir        = ".",
  output_dir       = "."
)

# Multi-country example:
# result <- preprocess_piaac(
#   country_codes    = c("US", "KR"),
#   target_booklet   = "PS1",
#   target_item      = 6,
#   exclude_keypress = TRUE,
#   input_dir        = "./data",
#   output_dir       = "./output"
# )
