# ================================================================
# 생성 모델: deepseek/deepseek-v3.2
# 응답 시간: 152.73s
# 입력 토큰: 6804
# 출력 토큰: 3869
# ================================================================

# Load required packages
library(dplyr)
library(tidyr)
library(stringr)
library(readr)
library(purrr)

# Step 1: Data Loading and Item Selection
step1_load_select <- function(input_dir, country_codes, target_booklet, target_item) {
  # Load multiple country files
  df_list <- list()
  
  for (code in country_codes) {
    file_path <- file.path(input_dir, paste0(code, "_logdata.txt"))
    if (!file.exists(file_path)) {
      message("File not found: ", file_path)
      next
    }
    
    df_country <- read_tsv(file_path, col_types = cols(
      CNTRYID = col_character(),
      SEQID = col_integer(),
      booklet_id = col_character(),
      item_id = col_integer(),
      event_name = col_character(),
      event_type = col_character(),
      timestamp = col_double(),
      event_description = col_character()
    ))
    
    # Prepend country code to SEQID for unique identification
    df_country <- df_country %>%
      mutate(SEQID = paste0(code, "_", SEQID))
    
    df_list[[code]] <- df_country
  }
  
  if (length(df_list) == 0) {
    stop("No data loaded. Check file paths.")
  }
  
  # Combine all country data
  df_combined <- bind_rows(df_list)
  
  message("Initial row count: ", nrow(df_combined))
  
  # Filter by target booklet and item
  df_filtered <- df_combined %>%
    filter(booklet_id == target_booklet, item_id == target_item)
  
  message("After Step 1 (filtering): ", nrow(df_filtered))
  
  # Convert event_type to lowercase
  df_filtered <- df_filtered %>%
    mutate(event_type = str_to_lower(event_type))
  
  return(df_filtered)
}

# Step 2: Restart Event Preprocessing
step2_restart_correction <- function(df_in) {
  df_out <- df_in
  original_timestamps <- df_out$timestamp
  
  # Process each SEQID group
  for (seqid_val in unique(df_out$SEQID)) {
    seqid_mask <- df_out$SEQID == seqid_val
    seqid_indices <- which(seqid_mask)
    
    if (length(seqid_indices) < 2) next
    
    # Find restart rows
    restart_indices <- seqid_indices[df_out$event_type[seqid_mask] == "restart"]
    
    # Process each restart in order
    for (restart_idx in restart_indices) {
      # Skip if restart is first or last row in SEQID group
      if (restart_idx == min(seqid_indices) || restart_idx == max(seqid_indices)) {
        next
      }
      
      # Get previous adjusted timestamp
      prev_timestamp <- df_out$timestamp[restart_idx - 1]
      
      # Get next original timestamp
      next_original_timestamp <- original_timestamps[restart_idx + 1]
      
      # Calculate restart timestamp
      restart_timestamp <- prev_timestamp + round(next_original_timestamp / 2)
      
      # Update restart row
      df_out$timestamp[restart_idx] <- restart_timestamp
      
      # Find end boundary
      end_idx <- restart_idx
      for (i in (restart_idx + 1):max(seqid_indices)) {
        if (df_out$event_type[i] == "end") {
          end_idx <- i
          break
        }
      }
      if (end_idx == restart_idx) {
        end_idx <- max(seqid_indices)
      }
      
      # Apply offset to rows after restart up to end boundary
      for (i in (restart_idx + 1):end_idx) {
        df_out$timestamp[i] <- restart_timestamp + original_timestamps[i]
      }
    }
  }
  
  message("After Step 2 (restart correction): ", nrow(df_out))
  return(df_out)
}

# Step 3: Multiple Log Entries Handling
step3_multiple_entries <- function(df_in, remove_list = NULL, merge_blocks = NULL) {
  df_out <- df_in
  
  # Default removal list
  if (is.null(remove_list)) {
    remove_list <- c("doaction", "next_inquiry", "next_button", "confirmation_opened",
                     "confirmation_closed", "environment", "mc_help_toolbar", "mail_sent",
                     "mail_deleted", "mc_help_menuitem", "sort_menu", "copy", "paste",
                     "new_folder", "mc_sort", "delete_folder")
  }
  
  # Default merge blocks
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
  merge_blocks <- merge_blocks[order(-sapply(merge_blocks, function(x) length(x$sequence)))]
  
  # Process each SEQID group
  result_rows <- list()
  
  for (seqid_val in unique(df_out$SEQID)) {
    seqid_data <- df_out %>% filter(SEQID == seqid_val)
    n_rows <- nrow(seqid_data)
    i <- 1
    
    while (i <= n_rows) {
      matched <- FALSE
      
      # Try each pattern
      for (block in merge_blocks) {
        pattern_len <- length(block$sequence)
        if (i + pattern_len - 1 > n_rows) next
        
        # Check if sequence matches
        current_seq <- seqid_data$event_type[i:(i + pattern_len - 1)]
        if (all(current_seq == block$sequence)) {
          # Find representative row
          rep_idx <- which(seqid_data$event_type[i:(i + pattern_len - 1)] == block$represent)[1]
          if (is.na(rep_idx)) rep_idx <- 1
          
          # Keep representative row
          result_rows[[length(result_rows) + 1]] <- seqid_data[i + rep_idx - 1, ]
          
          # Move past the matched block
          i <- i + pattern_len
          matched <- TRUE
          break
        }
      }
      
      if (!matched) {
        # Keep current row
        result_rows[[length(result_rows) + 1]] <- seqid_data[i, ]
        i <- i + 1
      }
    }
  }
  
  df_out <- bind_rows(result_rows)
  message("After Step 3 (multiple entries): ", nrow(df_out))
  return(df_out)
}

# Step 4: Duplicate Action Handling
step4_duplicate_removal <- function(df_in, exclude_keypress = FALSE) {
  df_out <- df_in
  
  if (exclude_keypress) {
    # Separate keypress rows
    keypress_rows <- df_out %>% filter(event_type == "keypress")
    non_keypress_rows <- df_out %>% filter(event_type != "keypress")
    
    # Remove duplicates from non-keypress rows
    non_keypress_rows <- non_keypress_rows %>% distinct()
    
    # Recombine
    df_out <- bind_rows(non_keypress_rows, keypress_rows) %>%
      arrange(SEQID, timestamp)
  } else {
    df_out <- df_out %>% distinct()
  }
  
  message("After Step 4 (duplicate removal): ", nrow(df_out))
  return(df_out)
}

# Step 5: Time Reversal Handling
step5_time_reversal <- function(df_in) {
  df_out <- df_in %>%
    arrange(SEQID, timestamp)
  
  message("After Step 5 (time reversal): ", nrow(df_out))
  return(df_out)
}

# Step 6: Keypress Aggregation
step6_keypress_aggregation <- function(df_in) {
  df_out <- df_in
  
  # Process each SEQID group
  result_rows <- list()
  
  for (seqid_val in unique(df_out$SEQID)) {
    seqid_data <- df_out %>% filter(SEQID == seqid_val)
    n_rows <- nrow(seqid_data)
    
    i <- 1
    while (i <= n_rows) {
      if (seqid_data$event_type[i] == "keypress") {
        # Find consecutive keypress block
        block_start <- i
        block_end <- i
        
        while (block_end + 1 <= n_rows && 
               seqid_data$event_type[block_end + 1] == "keypress") {
          block_end <- block_end + 1
        }
        
        block_size <- block_end - block_start + 1
        
        # Keep first row with updated description
        first_row <- seqid_data[block_start, ]
        first_row$event_description <- paste0("count=", block_size)
        result_rows[[length(result_rows) + 1]] <- first_row
        
        # Skip to after the block
        i <- block_end + 1
      } else {
        # Keep non-keypress row
        result_rows[[length(result_rows) + 1]] <- seqid_data[i, ]
        i <- i + 1
      }
    }
  }
  
  df_out <- bind_rows(result_rows)
  message("After Step 6 (keypress aggregation): ", nrow(df_out))
  return(df_out)
}

# Step 7: Event Description Parsing and Filtering
step7_description_parsing <- function(df_in, target_booklet, target_item, 
                                      desc_remove_list = NULL, remove_details = NULL) {
  df_out <- df_in
  
  # Default removal lists
  if (is.null(desc_remove_list)) {
    desc_remove_list <- c("test_time", "end", "value")
  }
  
  if (is.null(remove_details)) {
    remove_details <- c("nan", ",", ".")
  }
  
  # Prefix mapping
  prefix_map <- c(
    "PS1_1" = "u01a", "PS1_2" = "u01b", "PS1_3" = "u03a",
    "PS1_4" = "u06a", "PS1_5" = "u06b", "PS1_6" = "u21",
    "PS1_7" = "u04a", "PS2_1" = "u19a", "PS2_2" = "u19b",
    "PS2_3" = "u07",  "PS2_4" = "u02",  "PS2_5" = "u16",
    "PS2_6" = "u11b", "PS2_7" = "u23"
  )
  
  # Get prefix for current booklet-item combination
  key <- paste(target_booklet, target_item, sep = "_")
  prefix <- ifelse(key %in% names(prefix_map), paste0(prefix_map[key], "_"), "")
  
  # Process each row
  process_description <- function(desc) {
    if (is.na(desc) || desc == "") return("")
    
    # Convert to lowercase
    desc_lower <- str_to_lower(desc)
    
    # Split by delimiters
    elements <- str_split(desc_lower, "[\\|\\*\\$]")[[1]]
    elements <- str_trim(elements)
    elements <- elements[elements != ""]
    
    if (length(elements) == 0) return("")
    
    kept_values <- character(0)
    
    for (elem in elements) {
      if (str_detect(elem, "=") || elem == "end") {
        # Case A: Contains '=' or is 'end'
        if (elem == "end") {
          # 'end' is in remove list, discard
          next
        }
        
        parts <- str_split(elem, "=", n = 2)[[1]]
        if (length(parts) != 2) next
        
        part1 <- parts[1]
        part2 <- parts[2]
        
        # Check if part1 is in removal list
        if (part1 %in% desc_remove_list) {
          next
        }
        
        # Remove prefix from part2 if present
        if (prefix != "" && str_detect(part2, paste0("^", prefix))) {
          part2 <- str_remove(part2, fixed(prefix))
        }
        
        kept_values <- c(kept_values, part2)
      } else {
        # Case B: Does not contain '='
        # Check if element contains any remove_details
        should_remove <- any(sapply(remove_details, function(detail) {
          grepl(detail, elem, fixed = TRUE)
        }))
        
        if (!should_remove) {
          kept_values <- c(kept_values, elem)
        }
      }
    }
    
    if (length(kept_values) == 0) {
      return("")
    } else {
      return(paste(kept_values, collapse = "_"))
    }
  }
  
  # Apply processing to all rows
  df_out <- df_out %>%
    mutate(event_description = sapply(event_description, process_description))
  
  message("After Step 7 (description parsing): ", nrow(df_out))
  return(df_out)
}

# Step 8: Merge Column and File Export
step8_merge_export <- function(df_in, output_dir, country_codes) {
  df_out <- df_in
  
  # Create action_event column
  create_action_event <- function(event_type, event_description) {
    if (is.na(event_description) || event_description == "") {
      return(event_type)
    } else {
      return(paste0(event_type, "_", event_description))
    }
  }
  
  df_out <- df_out %>%
    mutate(action_event = mapply(create_action_event, event_type, event_description))
  
  # Select required columns
  df_export <- df_out %>%
    select(SEQID, event_type, event_description, action_event, timestamp)
  
  # Export CSV for each country
  for (code in country_codes) {
    # Filter rows for this country
    df_country <- df_export %>%
      filter(str_starts(SEQID, paste0(code, "_")))
    
    if (nrow(df_country) > 0) {
      output_file <- file.path(output_dir, paste0(code, "_processed_logdata.csv"))
      write_csv(df_country, output_file)
      message("Exported: ", output_file)
    }
  }
  
  message("After Step 8 (merge and export): ", nrow(df_out))
  return(df_out)
}

# Main function
preprocess_piaac <- function(input_dir, output_dir, country_codes,
                             target_booklet, target_item,
                             exclude_keypress = FALSE,
                             remove_list = NULL,
                             merge_blocks = NULL,
                             desc_remove_list = NULL,
                             remove_details = NULL) {
  
  # Step 1: Load and select data
  message("\n=== Step 1: Loading and selecting data ===")
  df <- step1_load_select(input_dir, country_codes, target_booklet, target_item)
  
  # Step 2: Restart correction
  message("\n=== Step 2: Restart correction ===")
  df <- step2_restart_correction(df)
  
  # Step 3: Multiple log entries handling
  message("\n=== Step 3: Multiple log entries handling ===")
  df <- step3_multiple_entries(df, remove_list, merge_blocks)
  
  # Step 4: Duplicate removal
  message("\n=== Step 4: Duplicate removal ===")
  df <- step4_duplicate_removal(df, exclude_keypress)
  
  # Step 5: Time reversal handling
  message("\n=== Step 5: Time reversal handling ===")
  df <- step5_time_reversal(df)
  
  # Step 6: Keypress aggregation
  message("\n=== Step 6: Keypress aggregation ===")
  df <- step6_keypress_aggregation(df)
  
  # Step 7: Event description parsing
  message("\n=== Step 7: Event description parsing ===")
  df <- step7_description_parsing(df, target_booklet, target_item, 
                                  desc_remove_list, remove_details)
  
  # Step 8: Merge and export
  message("\n=== Step 8: Merge and export ===")
  df <- step8_merge_export(df, output_dir, country_codes)
  
  message("\n=== Preprocessing complete ===")
  message("Final row count: ", nrow(df))
  
  return(df)
}

# Example usage (uncomment and adjust paths/params to run)
result <- preprocess_piaac(
  input_dir = ".",
  output_dir = ".",
  country_codes = c("US"),
  target_booklet = "PS1",
  target_item = 1,
  exclude_keypress = FALSE
)
