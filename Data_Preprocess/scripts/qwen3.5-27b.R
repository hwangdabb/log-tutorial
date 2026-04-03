# ================================================================
# 생성 모델: qwen/qwen3.5-27b
# 응답 시간: 146.2s
# 입력 토큰: 7340
# 출력 토큰: 10367
# ================================================================

library(tidyverse)

# Step 1: Data Loading and Item Selection
step1_load_and_select <- function(input_dir, country_codes, target_booklet, target_item) {
  df_out <- tibble()
  
  for (cc in country_codes) {
    file_path <- file.path(input_dir, paste0(cc, "_logdata.txt"))
    if (!file.exists(file_path)) {
      message(paste("File not found:", file_path))
      next
    }
    
    df_temp <- readr::read_tsv(file_path, col_types = readr::cols(.default = readr::col_character()))
    
    # Prepend country code to SEQID if multiple countries
    if (length(country_codes) > 1) {
      df_temp <- df_temp %>%
        mutate(SEQID = paste0(cc, "_", SEQID))
    }
    
    # Filter by booklet and item
    df_temp <- df_temp %>%
      filter(booklet_id == target_booklet, item_id == as.character(target_item))
    
    # Normalize event_type to lowercase
    df_temp <- df_temp %>%
      mutate(event_type = stringr::str_to_lower(event_type))
    
    df_out <- bind_rows(df_out, df_temp)
  }
  
  message(paste("Step 1: Loaded and selected data. Rows:", nrow(df_out)))
  return(df_out)
}

# Step 2: Restart Event Preprocessing
step2_restart_correction <- function(df_in) {
  df_out <- df_in
  message(paste("Step 2: Starting restart correction. Rows before:", nrow(df_out)))
  
  # Group by SEQID and process
  df_list <- df_out %>%
    group_split(SEQID)
  
  processed_list <- lapply(df_list, function(group_df) {
    n <- nrow(group_df)
    if (n == 0) return(group_df)
    
    # Initialize adjusted timestamps with original
    adj_ts <- as.numeric(group_df$timestamp)
    orig_ts <- as.numeric(group_df$timestamp)
    event_types <- group_df$event_type
    
    # Find restart indices
    restart_indices <- which(event_types == "restart")
    
    for (idx in restart_indices) {
      # Skip if first or last row of group
      if (idx == 1 || idx == n) next
      
      # Get prev adjusted timestamp
      prev_ts <- adj_ts[idx - 1]
      
      # Get next original timestamp
      next_orig_ts <- orig_ts[idx + 1]
      
      # Calculate restart timestamp
      restart_ts <- prev_ts + round(next_orig_ts / 2)
      
      # Update restart row adjusted timestamp
      adj_ts[idx] <- restart_ts
      
      # Find end boundary
      end_idx <- n
      for (k in (idx + 1):n) {
        if (event_types[k] == "end") {
          end_idx <- k
          break
        }
      }
      
      # Apply offset to rows from idx+1 to end_idx
      for (j in (idx + 1):end_idx) {
        adj_ts[j] <- restart_ts + orig_ts[j]
      }
    }
    
    group_df$timestamp <- adj_ts
    return(group_df)
  })
  
  df_out <- bind_rows(processed_list)
  message(paste("Step 2: Restart correction complete. Rows after:", nrow(df_out)))
  return(df_out)
}

# Step 3: Multiple Log Entries Handling
step3_multiple_logs <- function(df_in, remove_list = NULL, merge_blocks = NULL) {
  df_out <- df_in
  message(paste("Step 3: Starting multiple log handling. Rows before:", nrow(df_out)))
  
  # Defaults
  if (is.null(remove_list)) {
    remove_list <- c("doaction", "next_inquiry", "next_button", "confirmation_opened",
                     "confirmation_closed", "environment", "mc_help_toolbar", "mail_sent",
                     "mail_deleted", "mc_help_menuitem", "sort_menu", "copy", "paste",
                     "new_folder", "mc_sort", "delete_folder")
  }
  if (is.null(merge_blocks)) {
    merge_blocks <- list(
      list(sequence = c("folder_viewed", "mail_drop", "mail_moved"), represent = "mail_drop"),
      list(sequence = c("folder_viewed", "mail_moved", "mail_drop"), represent = "mail_drop"),
      list(sequence = c("button", "mail_moved"), represent = "button"),
      list(sequence = c("button", "next_item", "end"), represent = "button"),
      list(sequence = c("breakoff", "end"), represent = "breakoff")
    )
  }
  
  # Step A: Simple removal
  df_out <- df_out %>%
    filter(!event_type %in% remove_list)
  
  # Step B: Block merging
  # Sort patterns by length descending
  merge_blocks <- merge_blocks[order(sapply(merge_blocks, function(x) length(x$sequence)), decreasing = TRUE)]
  
  df_list <- df_out %>%
    group_split(SEQID)
  
  processed_list <- lapply(df_list, function(group_df) {
    n <- nrow(group_df)
    if (n == 0) return(group_df)
    
    keep_indices <- integer(0)
    i <- 1
    
    while (i <= n) {
      matched <- FALSE
      for (pattern in merge_blocks) {
        seq_len <- length(pattern$sequence)
        if (i + seq_len - 1 <= n) {
          # Check if sequence matches
          current_seq <- group_df$event_type[i:(i + seq_len - 1)]
          if (all(current_seq == pattern$sequence)) {
            # Find representative row index within the pattern
            rep_idx <- which(pattern$sequence == pattern$represent)[1]
            actual_row_idx <- i + rep_idx - 1
            
            keep_indices <- c(keep_indices, actual_row_idx)
            i <- i + seq_len
            matched <- TRUE
            break
          }
        }
      }
      
      if (!matched) {
        keep_indices <- c(keep_indices, i)
        i <- i + 1
      }
    }
    
    return(group_df[keep_indices, ])
  })
  
  df_out <- bind_rows(processed_list)
  message(paste("Step 3: Multiple log handling complete. Rows after:", nrow(df_out)))
  return(df_out)
}

# Step 4: Duplicate Action Handling
step4_duplicate_handling <- function(df_in, exclude_keypress = FALSE) {
  df_out <- df_in
  message(paste("Step 4: Starting duplicate handling. Rows before:", nrow(df_out)))
  
  if (exclude_keypress) {
    keypress_df <- df_out %>% filter(event_type == "keypress")
    other_df <- df_out %>% filter(event_type != "keypress")
    
    other_df <- other_df %>% distinct()
    
    df_out <- bind_rows(other_df, keypress_df)
  } else {
    df_out <- df_out %>% distinct()
  }
  
  message(paste("Step 4: Duplicate handling complete. Rows after:", nrow(df_out)))
  return(df_out)
}

# Step 5: Time Reversal Handling
step5_time_reversal <- function(df_in) {
  df_out <- df_in
  message(paste("Step 5: Starting time reversal handling. Rows before:", nrow(df_out)))
  
  df_out <- df_out %>%
    arrange(SEQID, timestamp)
  
  message(paste("Step 5: Time reversal handling complete. Rows after:", nrow(df_out)))
  return(df_out)
}

# Step 6: Keypress Aggregation
step6_keypress_aggregation <- function(df_in) {
  df_out <- df_in
  message(paste("Step 6: Starting keypress aggregation. Rows before:", nrow(df_out)))
  
  df_list <- df_out %>%
    group_split(SEQID)
  
  processed_list <- lapply(df_list, function(group_df) {
    n <- nrow(group_df)
    if (n == 0) return(group_df)
    
    event_types <- group_df$event_type
    keep_indices <- integer(0)
    i <- 1
    
    while (i <= n) {
      if (event_types[i] == "keypress") {
        # Count consecutive keypresses
        count <- 1
        j <- i + 1
        while (j <= n && event_types[j] == "keypress") {
          count <- count + 1
          j <- j + 1
        }
        
        # Keep first row, update description
        keep_indices <- c(keep_indices, i)
        group_df$event_description[i] <- paste0("count=", count)
        
        # Skip remaining keypresses in block
        i <- j
      } else {
        keep_indices <- c(keep_indices, i)
        i <- i + 1
      }
    }
    
    return(group_df[keep_indices, ])
  })
  
  df_out <- bind_rows(processed_list)
  message(paste("Step 6: Keypress aggregation complete. Rows after:", nrow(df_out)))
  return(df_out)
}

# Step 7: Event Description Parsing and Filtering
step7_desc_parsing <- function(df_in, target_booklet, target_item, desc_remove_list = NULL, remove_details = NULL) {
  df_out <- df_in
  message(paste("Step 7: Starting event description parsing. Rows before:", nrow(df_out)))
  
  # Defaults
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
  
  prefix_key <- paste0(target_booklet, "_", target_item)
  prefix <- prefix_map[prefix_key]
  if (is.na(prefix)) prefix <- ""
  
  # Process each row
  df_out$event_description <- sapply(df_out$event_description, function(desc) {
    if (is.na(desc) || desc == "") return("")
    
    # Lowercase
    desc <- stringr::str_to_lower(desc)
    
    # Split by delimiters
    parts <- stringr::str_split(desc, "[\\|\\*\\$]")[[1]]
    
    # Trim and remove empty
    parts <- trimws(parts)
    parts <- parts[parts != ""]
    
    kept_values <- character(0)
    
    for (part in parts) {
      if (grepl("=", part, fixed = TRUE)) {
        # Case A: Contains '='
        eq_parts <- stringr::str_split(part, "=")[[1]]
        if (length(eq_parts) >= 2) {
          part1 <- eq_parts[1]
          part2 <- eq_parts[2]
          
          # Check removal list
          if (part1 %in% desc_remove_list) next
          
          # Remove prefix from part2
          if (nchar(prefix) > 0) {
            pattern <- paste0("^", prefix, "_")
            part2 <- stringr::str_replace(part2, pattern, "")
          }
          
          kept_values <- c(kept_values, part2)
        }
      } else {
        # Case B: No '='
        # Check remove_details
        remove <- FALSE
        for (detail in remove_details) {
          if (grepl(detail, part, fixed = TRUE)) {
            remove <- TRUE
            break
          }
        }
        if (!remove) {
          kept_values <- c(kept_values, part)
        }
      }
    }
    
    return(paste(kept_values, collapse = "_"))
  })
  
  message(paste("Step 7: Event description parsing complete. Rows after:", nrow(df_out)))
  return(df_out)
}

# Step 8: Merge Column and File Export
step8_merge_and_export <- function(df_in, output_dir, country_codes) {
  df_out <- df_in
  message(paste("Step 8: Starting merge and export. Rows before:", nrow(df_out)))
  
  # Create action_event
  df_out <- df_out %>%
    mutate(
      action_event = ifelse(
        event_description == "",
        event_type,
        paste0(event_type, "_", event_description)
      )
    )
  
  # Select columns for export
  export_cols <- c("SEQID", "event_type", "event_description", "action_event", "timestamp")
  df_export <- df_out %>% select(all_of(export_cols))
  
  ## Export combined file elif single country.
  if (length(country_codes) == 1) {
    data_to_write <- df_export
    file_name <- paste0(country_codes, "_processed_logdata.csv")
    file_path <- file.path(output_dir, file_name)
    readr::write_csv(data_to_write, file_path)
  }
  # Export files per country code
  # Extract country code from SEQID (assuming format CODE_SEQID)
  unique_countries <- unique(stringr::str_extract(df_export$SEQID, "^[A-Z]+"))
  
  for (cc in unique_countries) {
    # Filter data for this country
    # Note: If SEQID was not modified (single country), it might not have prefix.
    # We check if SEQID starts with cc
    filter_pattern <- paste0("^", cc, "_")
    if (all(grepl(filter_pattern, df_export$SEQID))) {
      # All rows match this country (single country case)
      data_to_write <- df_export
    } else {
      # Multi-country case
      data_to_write <- df_export %>% filter(stringr::str_starts(SEQID, cc))
    }
    
    if (nrow(data_to_write) > 0) {
      file_name <- paste0(cc, "_processed_logdata.csv")
      file_path <- file.path(output_dir, file_name)
      readr::write_csv(data_to_write, file_path)
      message(paste("Exported:", file_path))
    }
  }
  
  message(paste("Step 8: Merge and export complete. Rows after:", nrow(df_out)))
  return(df_out)
}

# Main Function
preprocess_piaac <- function(
  input_dir,
  output_dir,
  country_codes,
  target_booklet,
  target_item,
  exclude_keypress = FALSE,
  remove_list = NULL,
  merge_blocks = NULL,
  desc_remove_list = NULL,
  remove_details = NULL
) {
  # Step 1
  df <- step1_load_and_select(input_dir, country_codes, target_booklet, target_item)
  
  # Step 2
  df <- step2_restart_correction(df)
  
  # Step 3
  df <- step3_multiple_logs(df, remove_list, merge_blocks)
  
  # Step 4
  df <- step4_duplicate_handling(df, exclude_keypress)
  
  # Step 5
  df <- step5_time_reversal(df)
  
  # Step 6
  df <- step6_keypress_aggregation(df)
  
  # Step 7
  df <- step7_desc_parsing(df, target_booklet, target_item, desc_remove_list, remove_details)
  
  # Step 8
  df <- step8_merge_and_export(df, output_dir, country_codes)
  
  return(df)
}

result <- preprocess_piaac(
  input_dir        = ".",
  output_dir       = ".",
  country_codes    = c("US"),
  target_booklet   = "PS1",
  target_item      = 1,
  exclude_keypress = FALSE
)