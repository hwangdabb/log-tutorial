# ================================================================
# 생성 모델: openai/gpt-5.4
# 응답 시간: 48.1s
# 입력 토큰: 6686
# 출력 토큰: 4659
# ================================================================

# Load required packages
library(dplyr)
library(tidyr)
library(stringr)
library(readr)
library(purrr)

# Step 1: Data Loading and Item Selection
step1_load_and_select <- function(df_in = NULL,
                                  country_codes,
                                  target_booklet,
                                  target_item,
                                  input_dir = ".") {
  before_n <- if (is.null(df_in)) 0 else nrow(df_in)
  message("Step 1 - before: ", before_n, " rows")

  # Create a local working copy
  df_out <- df_in

  # Load one or more country log files
  loaded_list <- lapply(country_codes, function(cc) {
    file_path <- file.path(input_dir, paste0(cc, "_logdata.txt"))

    # Read tab-delimited log data
    tmp_df <- readr::read_tsv(file_path, col_types = cols())

    # Create unique multi-country SEQID by prepending country code
    if (length(country_codes) > 1) {
      tmp_df <- tmp_df %>%
        mutate(SEQID = str_c(cc, "_", SEQID))
    } else {
      tmp_df <- tmp_df %>%
        mutate(SEQID = as.character(SEQID))
    }

    tmp_df
  })

  # Combine all loaded country files
  df_out <- bind_rows(loaded_list)

  # Filter to the selected booklet and item, then normalize event_type to lowercase
  df_out <- df_out %>%
    filter(booklet_id == target_booklet, item_id == target_item) %>%
    mutate(
      event_type = str_to_lower(event_type),
      SEQID = as.character(SEQID)
    )

  after_n <- nrow(df_out)
  message("Step 1 - after: ", after_n, " rows")
  df_out
}

# Step 2: Restart Event Preprocessing
step2_restart_correction <- function(df_in) {
  before_n <- nrow(df_in)
  message("Step 2 - before: ", before_n, " rows")

  # Create a local working copy
  df_out <- df_in

  # Preserve original order for within-SEQID scanning
  df_out <- df_out %>%
    mutate(.row_id_internal = seq_len(n()))

  seq_groups <- split(df_out, df_out$SEQID)
  corrected_groups <- vector("list", length(seq_groups))
  group_names <- names(seq_groups)

  # Process each SEQID independently using base R loops
  for (g in seq_along(seq_groups)) {
    grp <- seq_groups[[g]]
    grp <- grp[order(grp$.row_id_internal), , drop = FALSE]

    original_timestamps <- grp$timestamp
    adjusted_timestamps <- original_timestamps
    evt <- grp$event_type
    n_grp <- nrow(grp)

    restart_idx <- which(evt == "restart")

    if (length(restart_idx) > 0) {
      for (r in restart_idx) {
        # Skip if restart is first or last row of the SEQID group
        if (r == 1 || r == n_grp) {
          next
        }

        # Compute corrected restart timestamp
        prev_timestamp <- adjusted_timestamps[r - 1]
        next_original_timestamp <- original_timestamps[r + 1]
        restart_timestamp <- prev_timestamp + round(next_original_timestamp / 2)

        # Update restart row itself
        adjusted_timestamps[r] <- restart_timestamp

        # Find next 'end' after restart; if none, use last row
        end_idx <- n_grp
        if (r + 1 <= n_grp) {
          found_end <- FALSE
          for (j in (r + 1):n_grp) {
            if (evt[j] == "end") {
              end_idx <- j
              found_end <- TRUE
              break
            }
          }
          if (!found_end) {
            end_idx <- n_grp
          }
        }

        # Apply offset from row after restart through end_idx using original timestamps
        if (r + 1 <= end_idx) {
          for (j in (r + 1):end_idx) {
            adjusted_timestamps[j] <- restart_timestamp + original_timestamps[j]
          }
        }
      }
    }

    grp$timestamp <- adjusted_timestamps
    corrected_groups[[g]] <- grp
  }

  df_out <- bind_rows(corrected_groups) %>%
    arrange(.row_id_internal) %>%
    select(-.row_id_internal)

  after_n <- nrow(df_out)
  message("Step 2 - after: ", after_n, " rows")
  df_out
}

# Step 3: Multiple Log Entries Handling
step3_multiple_log_entries <- function(df_in,
                                       remove_list = NULL,
                                       merge_blocks = NULL) {
  before_n <- nrow(df_in)
  message("Step 3 - before: ", before_n, " rows")

  # Create a local working copy
  df_out <- df_in

  # Apply default ancillary removal list if omitted
  if (is.null(remove_list)) {
    remove_list <- c(
      "doaction", "next_inquiry", "next_button", "confirmation_opened",
      "confirmation_closed", "environment", "mc_help_toolbar", "mail_sent",
      "mail_deleted", "mc_help_menuitem", "sort_menu", "copy", "paste",
      "new_folder", "mc_sort", "delete_folder"
    )
  }

  # Apply default merge block patterns if omitted
  if (is.null(merge_blocks)) {
    merge_blocks <- list(
      list(sequence = c("folder_viewed", "mail_drop", "mail_moved"), represent = "mail_drop"),
      list(sequence = c("folder_viewed", "mail_moved", "mail_drop"), represent = "mail_drop"),
      list(sequence = c("button", "mail_moved"), represent = "button"),
      list(sequence = c("button", "next_item", "end"), represent = "button"),
      list(sequence = c("breakoff", "end"), represent = "breakoff")
    )
  }

  # Preserve current order for left-to-right block scanning
  df_out <- df_out %>%
    mutate(.row_id_internal = seq_len(n()))

  # Step 3A: Remove ancillary rows with no analytical meaning
  df_out <- df_out %>%
    filter(!event_type %in% remove_list)

  # Sort merge patterns by descending sequence length
  pattern_lengths <- sapply(merge_blocks, function(x) length(x$sequence))
  merge_blocks <- merge_blocks[order(pattern_lengths, decreasing = TRUE)]

  seq_groups <- split(df_out, df_out$SEQID)
  merged_groups <- vector("list", length(seq_groups))

  # Step 3B: Greedy non-overlapping block merge within each SEQID
  for (g in seq_along(seq_groups)) {
    grp <- seq_groups[[g]]
    grp <- grp[order(grp$.row_id_internal), , drop = FALSE]

    n_grp <- nrow(grp)
    keep_rows <- list()
    i <- 1

    while (i <= n_grp) {
      matched <- FALSE

      for (p in seq_along(merge_blocks)) {
        seq_pattern <- merge_blocks[[p]]$sequence
        represent_evt <- merge_blocks[[p]]$represent
        pat_len <- length(seq_pattern)

        if ((i + pat_len - 1) <= n_grp) {
          current_seq <- grp$event_type[i:(i + pat_len - 1)]

          if (identical(as.character(current_seq), as.character(seq_pattern))) {
            rel_idx <- which(seq_pattern == represent_evt)[1]
            rep_idx <- i + rel_idx - 1
            keep_rows[[length(keep_rows) + 1]] <- grp[rep_idx, , drop = FALSE]
            i <- i + pat_len
            matched <- TRUE
            break
          }
        }
      }

      if (!matched) {
        keep_rows[[length(keep_rows) + 1]] <- grp[i, , drop = FALSE]
        i <- i + 1
      }
    }

    merged_groups[[g]] <- bind_rows(keep_rows)
  }

  df_out <- bind_rows(merged_groups) %>%
    arrange(.row_id_internal) %>%
    select(-.row_id_internal)

  after_n <- nrow(df_out)
  message("Step 3 - after: ", after_n, " rows")
  df_out
}

# Step 4: Duplicate Action Handling
step4_remove_duplicates <- function(df_in,
                                    exclude_keypress = TRUE) {
  before_n <- nrow(df_in)
  message("Step 4 - before: ", before_n, " rows")

  # Create a local working copy
  df_out <- df_in

  # Preserve order before deduplication/recombination
  df_out <- df_out %>%
    mutate(.row_id_internal = seq_len(n()))

  if (exclude_keypress) {
    # Separate keypress rows from non-keypress rows
    non_keypress_df <- df_out %>%
      filter(event_type != "keypress") %>%
      distinct()

    keypress_df <- df_out %>%
      filter(event_type == "keypress")

    # Recombine and restore original order
    df_out <- bind_rows(non_keypress_df, keypress_df) %>%
      arrange(.row_id_internal)
  } else {
    # Deduplicate all rows including keypress
    df_out <- df_out %>%
      distinct() %>%
      arrange(.row_id_internal)
  }

  df_out <- df_out %>%
    select(-.row_id_internal)

  after_n <- nrow(df_out)
  message("Step 4 - after: ", after_n, " rows")
  df_out
}

# Step 5: Time Reversal Handling
step5_fix_time_reversal <- function(df_in) {
  before_n <- nrow(df_in)
  message("Step 5 - before: ", before_n, " rows")

  # Create a local working copy
  df_out <- df_in

  # Preserve original row order to maintain stability on equal timestamps
  df_out <- df_out %>%
    mutate(.row_id_internal = seq_len(n()))

  # Sort rows within SEQID by ascending timestamp
  df_out <- df_out %>%
    arrange(SEQID, timestamp, .row_id_internal) %>%
    select(-.row_id_internal)

  after_n <- nrow(df_out)
  message("Step 5 - after: ", after_n, " rows")
  df_out
}

# Step 6: Keypress Aggregation
step6_aggregate_keypress <- function(df_in) {
  before_n <- nrow(df_in)
  message("Step 6 - before: ", before_n, " rows")

  # Create a local working copy
  df_out <- df_in

  # Preserve current order for sequential keypress block detection
  df_out <- df_out %>%
    mutate(.row_id_internal = seq_len(n()))

  seq_groups <- split(df_out, df_out$SEQID)
  aggregated_groups <- vector("list", length(seq_groups))

  # Aggregate consecutive keypress blocks within each SEQID
  for (g in seq_along(seq_groups)) {
    grp <- seq_groups[[g]]
    grp <- grp[order(grp$.row_id_internal), , drop = FALSE]

    n_grp <- nrow(grp)
    out_rows <- list()
    i <- 1

    while (i <= n_grp) {
      if (grp$event_type[i] == "keypress") {
        block_start <- i
        block_end <- i

        while (block_end + 1 <= n_grp && grp$event_type[block_end + 1] == "keypress") {
          block_end <- block_end + 1
        }

        block_n <- block_end - block_start + 1
        keep_row <- grp[block_start, , drop = FALSE]
        keep_row$event_description <- paste0("count=", block_n)
        out_rows[[length(out_rows) + 1]] <- keep_row

        i <- block_end + 1
      } else {
        out_rows[[length(out_rows) + 1]] <- grp[i, , drop = FALSE]
        i <- i + 1
      }
    }

    aggregated_groups[[g]] <- bind_rows(out_rows)
  }

  df_out <- bind_rows(aggregated_groups) %>%
    arrange(.row_id_internal) %>%
    select(-.row_id_internal)

  after_n <- nrow(df_out)
  message("Step 6 - after: ", after_n, " rows")
  df_out
}

# Step 7: Event Description Parsing and Filtering
step7_parse_event_description <- function(df_in,
                                          target_booklet,
                                          target_item,
                                          desc_remove_list = NULL,
                                          remove_details = NULL) {
  before_n <- nrow(df_in)
  message("Step 7 - before: ", before_n, " rows")

  # Create a local working copy
  df_out <- df_in

  # Apply default description removal list if omitted
  if (is.null(desc_remove_list)) {
    desc_remove_list <- c("test_time", "end", "value")
  }

  # Apply default detail removal list if omitted
  if (is.null(remove_details)) {
    remove_details <- c("nan", ",", ".")
  }

  # Prefix mapping for item-specific prefix removal
  prefix_map <- c(
    "PS1_1" = "u01a", "PS1_2" = "u01b", "PS1_3" = "u03a",
    "PS1_4" = "u06a", "PS1_5" = "u06b", "PS1_6" = "u21",
    "PS1_7" = "u04a", "PS2_1" = "u19a", "PS2_2" = "u19b",
    "PS2_3" = "u07",  "PS2_4" = "u02",  "PS2_5" = "u16",
    "PS2_6" = "u11b", "PS2_7" = "u23"
  )

  map_key <- paste0(target_booklet, "_", target_item)
  item_prefix <- unname(prefix_map[map_key])

  if (length(item_prefix) == 0 || is.na(item_prefix)) {
    item_prefix <- ""
  }

  # Parse and clean each event_description value row-wise
  parsed_desc <- vapply(seq_len(nrow(df_out)), function(idx) {
    desc_val <- df_out$event_description[idx]

    if (is.na(desc_val)) {
      return(NA_character_)
    }

    # Convert to lowercase
    desc_val <- str_to_lower(desc_val)

    # Split by original delimiters, trim whitespace, remove empties
    parts <- stringr::str_split(desc_val, "[\\|\\*\\$]")[[1]]
    parts <- stringr::str_trim(parts)
    parts <- parts[parts != ""]

    if (length(parts) == 0) {
      return("")
    }

    kept_parts <- character(0)

    for (elem in parts) {
      # Case A: element contains '=' or element equals 'end'
      if (str_detect(elem, fixed("=")) || identical(elem, "end")) {
        if (identical(elem, "end")) {
          part1 <- "end"
          part2 <- ""
        } else {
          split_eq <- strsplit(elem, "=", fixed = TRUE)[[1]]
          part1 <- split_eq[1]
          if (length(split_eq) >= 2) {
            part2 <- paste(split_eq[-1], collapse = "=")
          } else {
            part2 <- ""
          }
        }

        # Discard if variable name is in removal list
        if (part1 %in% desc_remove_list) {
          next
        }

        # Remove item-specific prefix from part2 if present
        if (!identical(item_prefix, "")) {
          part2 <- sub(paste0("^", item_prefix, "_"), "", part2)
        }

        kept_parts <- c(kept_parts, part2)
      } else {
        # Case B: remove if element contains any detail removal string
        remove_flag <- FALSE
        for (rm_detail in remove_details) {
          if (grepl(rm_detail, elem, fixed = TRUE)) {
            remove_flag <- TRUE
            break
          }
        }

        if (!remove_flag) {
          kept_parts <- c(kept_parts, elem)
        }
      }
    }

    # Join retained components with underscore; keep empty string if none remain
    if (length(kept_parts) == 0) {
      ""
    } else {
      paste(kept_parts, collapse = "_")
    }
  }, FUN.VALUE = character(1))

  df_out <- df_out %>%
    mutate(event_description = parsed_desc)

  after_n <- nrow(df_out)
  message("Step 7 - after: ", after_n, " rows")
  df_out
}

# Step 8: Merge Column and File Export
step8_merge_and_export <- function(df_in,
                                   country_codes,
                                   output_dir = ".") {
  before_n <- nrow(df_in)
  message("Step 8 - before: ", before_n, " rows")

  # Create a local working copy
  df_out <- df_in

  # Build action_event from event_type and extracted event_description components
  action_event_vec <- vapply(seq_len(nrow(df_out)), function(idx) {
    evt <- df_out$event_type[idx]
    desc_val <- df_out$event_description[idx]

    if (is.na(desc_val) || identical(desc_val, "")) {
      return(evt)
    }

    # Split by original delimiters, then extract part after '=' if present; otherwise keep element
    parts <- stringr::str_split(desc_val, "[\\|\\*\\$]")[[1]]
    parts <- stringr::str_trim(parts)
    parts <- parts[parts != ""]

    extracted <- character(0)

    if (length(parts) > 0) {
      for (elem in parts) {
        if (grepl("=", elem, fixed = TRUE)) {
          split_eq <- strsplit(elem, "=", fixed = TRUE)[[1]]
          if (length(split_eq) >= 2) {
            extracted <- c(extracted, paste(split_eq[-1], collapse = "="))
          } else {
            extracted <- c(extracted, "")
          }
        } else {
          extracted <- c(extracted, elem)
        }
      }
    }

    extracted <- extracted[!is.na(extracted)]

    if (length(extracted) == 0 || identical(paste(extracted, collapse = "_"), "")) {
      evt
    } else {
      paste0(evt, "_", paste(extracted, collapse = "_"))
    }
  }, FUN.VALUE = character(1))

  df_out <- df_out %>%
    mutate(action_event = action_event_vec)

  # Export processed file with required columns
  export_df <- df_out %>%
    select(SEQID, event_type, event_description, action_event, timestamp)

  if (length(country_codes) == 1) {
    out_name <- paste0(country_codes, "_processed_logdata.csv")
  } else {
    out_name <- paste0(paste(country_codes, collapse = "_"), "_processed_logdata.csv")
  }

  out_path <- file.path(output_dir, out_name)
  readr::write_csv(export_df, out_path)

  after_n <- nrow(df_out)
  message("Step 8 - after: ", after_n, " rows")
  df_out
}

# Main orchestration function
preprocess_piaac <- function(country_codes,
                             target_booklet,
                             target_item,
                             exclude_keypress = TRUE,
                             input_dir = ".",
                             output_dir = ".",
                             remove_list = NULL,
                             merge_blocks = NULL,
                             desc_remove_list = NULL,
                             remove_details = NULL) {
  # Step 1: Load data and filter selected item
  df_step1 <- step1_load_and_select(
    df_in = NULL,
    country_codes = country_codes,
    target_booklet = target_booklet,
    target_item = target_item,
    input_dir = input_dir
  )

  # Step 2: Correct restart-based timestamp resets
  df_step2 <- step2_restart_correction(df_step1)

  # Step 3: Handle multiple log entry patterns
  df_step3 <- step3_multiple_log_entries(
    df_in = df_step2,
    remove_list = remove_list,
    merge_blocks = merge_blocks
  )

  # Step 4: Remove complete duplicates
  df_step4 <- step4_remove_duplicates(
    df_in = df_step3,
    exclude_keypress = exclude_keypress
  )

  # Step 5: Fix time reversal by sorting timestamps within SEQID
  df_step5 <- step5_fix_time_reversal(df_step4)

  # Step 6: Aggregate consecutive keypress rows
  df_step6 <- step6_aggregate_keypress(df_step5)

  # Step 7: Parse and clean event descriptions
  df_step7 <- step7_parse_event_description(
    df_in = df_step6,
    target_booklet = target_booklet,
    target_item = target_item,
    desc_remove_list = desc_remove_list,
    remove_details = remove_details
  )

  # Step 8: Create action_event and export CSV
  df_step8 <- step8_merge_and_export(
    df_in = df_step7,
    country_codes = country_codes,
    output_dir = output_dir
  )

  # Return final processed tibble
  df_step8
}

result <- preprocess_piaac(
  country_codes    = c("US"),
  target_booklet   = "PS1",
  target_item      = 1,
  exclude_keypress = FALSE,
  input_dir        = ".",
  output_dir       = "."
)