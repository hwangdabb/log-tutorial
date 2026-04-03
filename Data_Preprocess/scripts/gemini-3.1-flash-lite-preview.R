# ================================================================
# 생성 모델: google/gemini-3.1-flash-lite-preview
# 응답 시간: 14.25s
# 입력 토큰: 7548
# 출력 토큰: 2497
# ================================================================

library(tidyverse)

# Step 1: Data Loading and Item Selection
step1 <- function(df, target_booklet, target_item) {
  df_out <- df %>%
    filter(booklet_id == target_booklet, item_id == target_item) %>%
    mutate(event_type = str_to_lower(event_type))
  message(paste("Step 1: Filtered to", nrow(df_out), "rows."))
  return(df_out)
}

# Step 2: Restart Event Preprocessing
step2 <- function(df) {
  df_out <- df %>% group_by(SEQID) %>% group_split()
  
  processed_list <- map(df_out, function(sub_df) {
    if (!"restart" %in% sub_df$event_type) return(sub_df)
    
    adj_ts <- sub_df$timestamp
    restart_indices <- which(sub_df$event_type == "restart")
    
    for (idx in restart_indices) {
      if (idx == 1 || idx == nrow(sub_df)) next
      
      prev_ts <- adj_ts[idx - 1]
      next_orig_ts <- sub_df$timestamp[idx + 1]
      restart_val <- prev_ts + round(next_orig_ts / 2)
      
      end_idx <- which(sub_df$event_type == "end" & seq_along(sub_df$event_type) > idx)
      end_idx <- if (length(end_idx) > 0) end_idx[1] else nrow(sub_df)
      
      adj_ts[(idx + 1):end_idx] <- restart_val + sub_df$timestamp[(idx + 1):end_idx]
      adj_ts[idx] <- restart_val
    }
    sub_df$timestamp <- adj_ts
    return(sub_df)
  })
  
  df_out <- bind_rows(processed_list)
  message(paste("Step 2: Processed restarts. Row count:", nrow(df_out)))
  return(df_out)
}

# Step 3: Multiple Log Entries Handling
step3 <- function(df, remove_list, merge_blocks) {
  df_out <- df %>% filter(!event_type %in% remove_list)
  
  # Block Merging
  merge_blocks <- merge_blocks[order(sapply(merge_blocks, function(x) length(x$sequence)), decreasing = TRUE)]
  
  df_out <- df_out %>% group_by(SEQID) %>% group_split() %>% map_dfr(function(sub_df) {
    i <- 1
    res <- list()
    while (i <= nrow(sub_df)) {
      matched <- FALSE
      for (block in merge_blocks) {
        seq_len <- length(block$sequence)
        if (i + seq_len - 1 <= nrow(sub_df)) {
          if (all(sub_df$event_type[i:(i + seq_len - 1)] == block$sequence)) {
            new_row <- sub_df[i, ]
            new_row$event_type <- block$represent
            res[[length(res) + 1]] <- new_row
            i <- i + seq_len
            matched <- TRUE
            break
          }
        }
      }
      if (!matched) {
        res[[length(res) + 1]] <- sub_df[i, ]
        i <- i + 1
      }
    }
    bind_rows(res)
  })
  message(paste("Step 3: Handled log entries. Row count:", nrow(df_out)))
  return(df_out)
}

# Step 4: Duplicate Action Handling
step4 <- function(df, exclude_keypress) {
  if (exclude_keypress) {
    df_kp <- df %>% filter(event_type == "keypress")
    df_other <- df %>% filter(event_type != "keypress") %>% distinct()
    df_out <- bind_rows(df_other, df_kp)
  } else {
    df_out <- df %>% distinct()
  }
  message(paste("Step 4: Deduplicated. Row count:", nrow(df_out)))
  return(df_out)
}

# Step 5: Time Reversal Handling
step5 <- function(df) {
  df_out <- df %>% arrange(SEQID, timestamp)
  message(paste("Step 5: Sorted by time. Row count:", nrow(df_out)))
  return(df_out)
}

# Step 6: Keypress Aggregation
step6 <- function(df) {
  df_out <- df %>% group_by(SEQID) %>% group_split() %>% map_dfr(function(sub_df) {
    sub_df$grp <- cumsum(sub_df$event_type != lag(sub_df$event_type, default = ""))
    sub_df %>% group_by(grp) %>% mutate(
      is_kp = (event_type == "keypress"),
      n = n(),
      event_description = if_else(is_kp, paste0("count=", n), event_description)
    ) %>% filter(!is_kp | row_number() == 1) %>% ungroup() %>% select(-grp, -is_kp, -n)
  })
  message(paste("Step 6: Aggregated keypresses. Row count:", nrow(df_out)))
  return(df_out)
}

# Step 7: Event Description Parsing
step7 <- function(df, target_booklet, target_item, desc_remove_list, remove_details) {
  prefix <- paste0(target_booklet, "_", target_item)
  prefix_map <- c("PS1_1" = "u01a", "PS1_2" = "u01b", "PS1_3" = "u03a", "PS1_4" = "u06a", "PS1_5" = "u06b", "PS1_6" = "u21", "PS1_7" = "u04a", "PS2_1" = "u19a", "PS2_2" = "u19b", "PS2_3" = "u07", "PS2_4" = "u02", "PS2_5" = "u16", "PS2_6" = "u11b", "PS2_7" = "u23")
  p_val <- prefix_map[prefix]
  
  df_out <- df %>% mutate(event_description = map_chr(event_description, function(desc) {
    if (is.na(desc) || desc == "") return("")
    parts <- unlist(str_split(str_to_lower(desc), "[\\|\\*\\$]"))
    parts <- parts[parts != ""]
    
    kept <- map(parts, function(p) {
      if (str_detect(p, "=")) {
        kv <- str_split(p, "=")[[1]]
        k <- kv[1]; v <- kv[2]
        if (k %in% desc_remove_list) return(NULL)
        if (!is.na(p_val)) v <- str_replace(v, paste0(p_val, "_"), "")
        return(v)
      } else {
        if (any(sapply(remove_details, function(d) str_detect(p, fixed(d))))) return(NULL)
        return(p)
      }
    })
    paste(unlist(kept), collapse = "_")
  }))
  message(paste("Step 7: Parsed descriptions. Row count:", nrow(df_out)))
  return(df_out)
}

# Step 8: Merge Column and Export
step8 <- function(df, country_code, output_dir) {
  df_out <- df %>% mutate(action_event = if_else(event_description == "", event_type, paste0(event_type, "_", event_description)))
  write_csv(df_out %>% select(SEQID, event_type, event_description, action_event, timestamp), file.path(output_dir, paste0(country_code, "_processed_logdata.csv")))
  message("Step 8: Exported CSV.")
  return(df_out)
}

preprocess_piaac <- function(country_codes, target_booklet, target_item, input_dir = ".", output_dir = ".", 
                             exclude_keypress = TRUE, remove_list = NULL, merge_blocks = NULL) {
  if (is.null(remove_list)) remove_list <- c("doaction", "next_inquiry", "next_button", "confirmation_opened", "confirmation_closed", "environment", "mc_help_toolbar", "mail_sent", "mail_deleted", "mc_help_menuitem", "sort_menu", "copy", "paste", "new_folder", "mc_sort", "delete_folder")
  if (is.null(merge_blocks)) merge_blocks <- list(list(sequence = c("folder_viewed", "mail_drop", "mail_moved"), represent = "mail_drop"), list(sequence = c("folder_viewed", "mail_moved", "mail_drop"), represent = "mail_drop"), list(sequence = c("button", "mail_moved"), represent = "button"), list(sequence = c("button", "next_item", "end"), represent = "button"), list(sequence = c("breakoff", "end"), represent = "breakoff"))
  
  raw_data <- map_dfr(country_codes, function(cc) {
    read_tsv(file.path(input_dir, paste0(cc, "_logdata.txt")), show_col_types = FALSE) %>% mutate(SEQID = paste0(cc, "_", SEQID))
  })
  
  df <- step1(raw_data, target_booklet, target_item)
  df <- step2(df)
  df <- step3(df, remove_list, merge_blocks)
  df <- step4(df, exclude_keypress)
  df <- step5(df)
  df <- step6(df)
  df <- step7(df, target_booklet, target_item, c("test_time", "end", "value"), c("nan", ",", "."))
  df <- step8(df, country_codes[1], output_dir)
  
  return(df)
}

# Example usage (uncomment and adjust paths/params to run)
result <- preprocess_piaac(
  country_codes    = c("US"),
  target_booklet   = "PS1",
  target_item      = 1,
  exclude_keypress = FALSE,
  input_dir        = ".",
  output_dir       = "."
)