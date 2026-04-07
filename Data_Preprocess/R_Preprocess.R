## ============================================================================
## Base Data Preprocessing
## This script implements the common preprocessing pipeline described in the Data Preprocessing section of the paper.
## It covers three main stages:
## (1) correcting timestamps disrupted by RESTART events, 
## (2) reducing redundant action patterns into single representative events
## (3) merging consecutive KEYPRESS events and combining event types with their descriptions into unified action labels.
## 
## The code processes log data from the PS1_1 item (U.S. sample) of the PIAAC Problem Solving in Technology-Rich Environments (PS-TRE) assessment.
##
## An equivalent Python implementation (processed_data_r.ipynb) is also provided for reproducibility across languages.
## ============================================================================

library(dplyr)
library(tidyr)
library(stringr)
library(readr)

## ============================================================================
## Function Declarations
## ============================================================================

#' Fix timestamps for RESTART events by accumulating offsets.
#'
#' For each SEQID group, recalculates timestamps after RESTART events
#' so that they continue from the previous event's adjusted timestamp.
#'
#' @param df DataFrame. Must contain SEQID, event_type, timestamp columns.
#' @return DataFrame with adjusted timestamps.
fix_restart_timestamps <- function(df) {
  df <- df %>% mutate(.original_index = row_number())

  result_list <- list()
  seqids <- unique(df$SEQID)

  for (sid in seqids) {
    group <- df %>%
      filter(SEQID == sid) %>%
      arrange(.original_index)

    adjusted_timestamps <- as.numeric(group$timestamp)
    restart_indices <- which(group$event_type == "RESTART")

    for (restart_idx in restart_indices) {
      if (restart_idx > 1 && restart_idx < nrow(group)) {
        prev_timestamp <- adjusted_timestamps[restart_idx - 1]
        next_original_timestamp <- group$timestamp[restart_idx + 1]

        # RESTART timestamp = previous + round(next_original / 2)
        restart_timestamp <- prev_timestamp + round(next_original_timestamp / 2)
        adjusted_timestamps[restart_idx] <- restart_timestamp

        # Find the next END event after RESTART
        end_idx <- NA
        if (restart_idx + 1 <= nrow(group)) {
          for (i in (restart_idx + 1):nrow(group)) {
            if (group$event_type[i] == "END") {
              end_idx <- i
              break
            }
          }
        }

        if (is.na(end_idx)) {
          end_idx <- nrow(group)
        }

        # Accumulate timestamps from RESTART to END
        offset <- restart_timestamp
        if (restart_idx + 1 <= end_idx) {
          for (i in (restart_idx + 1):end_idx) {
            adjusted_timestamps[i] <- offset + group$timestamp[i]
          }
        }
      }
    }

    group$timestamp <- adjusted_timestamps
    result_list[[length(result_list) + 1]] <- group
  }

  result <- bind_rows(result_list)
  result <- result %>%
    arrange(.original_index) %>%
    select(-.original_index)

  return(result)
}


#' Count event combinations within a time window per SEQID.
#'
#' @param df DataFrame. Must contain SEQID, event_type, timestamp columns.
#' @param time_window Maximum time gap for grouping events (default: 100).
#' @return Named vector of counts for each event-type combination.
count_sequences <- function(df, time_window = 100) {
  all_combinations <- list()

  for (sid in unique(df$SEQID)) {
    group_df <- df %>%
      filter(SEQID == sid) %>%
      arrange(row_number())

    if (nrow(group_df) == 0) next

    time_groups <- list()
    current_group <- c(1)

    if (nrow(group_df) > 1) {
      for (i in 2:nrow(group_df)) {
        if (group_df$timestamp[i] - group_df$timestamp[current_group[1]] <= time_window) {
          current_group <- c(current_group, i)
        } else {
          time_groups[[length(time_groups) + 1]] <- current_group
          current_group <- c(i)
        }
      }
    }
    time_groups[[length(time_groups) + 1]] <- current_group

    for (grp in time_groups) {
      combo <- paste(group_df$event_type[grp], collapse = "|")
      all_combinations[[length(all_combinations) + 1]] <- combo
    }
  }

  combo_vec <- unlist(all_combinations)
  return(sort(table(combo_vec), decreasing = TRUE))
}


#' Merge rows matching specific event-type patterns into a single row.
#'
#' @param df DataFrame. Must contain SEQID, event_type columns.
#' @param pattern_map List of lists, each with 'pattern' (character vector)
#'   and 'keep' (event_type to retain). e.g.,
#'   list(list(pattern = c("A", "B", "C"), keep = "B"), ...)
#' @return DataFrame with matched patterns collapsed.
merge_patterns <- function(df, pattern_map) {
  # Sort patterns by length (longest first) once before iteration
  pattern_map <- pattern_map[order(-sapply(pattern_map, function(x) length(x$pattern)))]

  result_rows <- list()

  for (sid in unique(df$SEQID)) {
    group_df <- df %>% filter(SEQID == sid)
    events <- group_df$event_type

    i <- 1
    while (i <= length(events)) {
      matched <- FALSE

      for (pm in pattern_map) {
        pattern <- pm$pattern
        keep_type <- pm$keep
        pattern_len <- length(pattern)

        if (i + pattern_len - 1 <= length(events)) {
          if (all(events[i:(i + pattern_len - 1)] == pattern)) {
            for (j in 0:(pattern_len - 1)) {
              if (events[i + j] == keep_type) {
                result_rows[[length(result_rows) + 1]] <- group_df[i + j, ]
                break
              }
            }
            i <- i + pattern_len
            matched <- TRUE
            break
          }
        }
      }

      if (!matched) {
        result_rows[[length(result_rows) + 1]] <- group_df[i, ]
        i <- i + 1
      }
    }
  }

  result <- bind_rows(result_rows)
  rownames(result) <- NULL
  return(result)
}


#' Replace rows matching a pattern with a single row having new event values.
#'
#' @param df DataFrame. Must contain SEQID, event_type, event_description columns.
#' @param pattern Character vector of event-type sequence, e.g., c("A", "B", "C").
#' @param new_event_type Replacement event_type value.
#' @param new_event_description Replacement event_description value.
#' @return DataFrame with matched patterns replaced.
replace_pattern <- function(df, pattern, new_event_type, new_event_description) {
  result_rows <- list()
  pattern_len <- length(pattern)

  for (sid in unique(df$SEQID)) {
    group_df <- df %>% filter(SEQID == sid)
    events <- group_df$event_type

    i <- 1
    while (i <= length(events)) {
      if (i + pattern_len - 1 <= length(events)) {
        if (all(events[i:(i + pattern_len - 1)] == pattern)) {
          first_row <- group_df[i, ]
          first_row$event_type <- new_event_type
          first_row$event_description <- new_event_description
          result_rows[[length(result_rows) + 1]] <- first_row
          i <- i + pattern_len
          next
        }
      }
      result_rows[[length(result_rows) + 1]] <- group_df[i, ]
      i <- i + 1
    }
  }

  result <- bind_rows(result_rows)
  rownames(result) <- NULL
  return(result)
}


#' Merge consecutive KEYPRESS events into a single row with count.
#'
#' For each SEQID group, collapses consecutive KEYPRESS rows into one row
#' with event_description set to 'count=N'.
#'
#' @param df DataFrame. Must contain SEQID, event_type, timestamp columns.
#' @return DataFrame with consecutive KEYPRESSes collapsed.
preprocess_keypress_data <- function(df) {
  processed_groups <- list()

  for (sid in unique(df$SEQID)) {
    seqid_data <- df %>%
      filter(SEQID == sid) %>%
      arrange(timestamp)

    if (nrow(seqid_data) == 0) next

    result_rows <- list()
    i <- 1

    while (i <= nrow(seqid_data)) {
      current_row <- seqid_data[i, ]

      if (current_row$event_type == "KEYPRESS") {
        keypress_count <- 1
        j <- i + 1

        while (j <= nrow(seqid_data) && seqid_data$event_type[j] == "KEYPRESS") {
          keypress_count <- keypress_count + 1
          j <- j + 1
        }

        current_row$event_description <- paste0("count=", keypress_count)
        result_rows[[length(result_rows) + 1]] <- current_row
        i <- j
      } else {
        result_rows[[length(result_rows) + 1]] <- current_row
        i <- i + 1
      }
    }

    if (length(result_rows) > 0) {
      processed_groups[[length(processed_groups) + 1]] <- bind_rows(result_rows)
    }
  }

  result_df <- bind_rows(processed_groups)

  cat(sprintf("Processed data size: %d rows\n", nrow(result_df)))
  cat(sprintf("Removed rows: %d\n", nrow(df) - nrow(result_df)))

  return(result_df)
}


# Constants for process_event_description
.REMOVE_VARS <- c("test_time", "end", "value")
.REMOVE_DETAILS <- c("nan", ",", ".")
.ITEM_PREFIX <- "u01a_"


#' Parse event_description by splitting on special characters and filtering.
#'
#' @param description Raw event_description string.
#' @return Filtered and joined description components.
process_event_description <- function(description) {
  components <- unlist(str_split(as.character(description), "[|*$]"))
  components <- trimws(components)
  components <- components[components != ""]

  filtered_components <- c()

  for (comp in components) {
    if (grepl("=", comp)) {
      parts <- str_split_fixed(comp, "=", n = 2)
      var_name <- trimws(parts[1, 1])
      detailed <- trimws(parts[1, 2])

      if (!(var_name %in% .REMOVE_VARS)) {
        detailed <- str_replace_all(detailed, fixed(.ITEM_PREFIX), "")
        filtered_components <- c(filtered_components, detailed)
      }
    } else if (comp == "end") {
      next
    } else {
      if (!any(sapply(.REMOVE_DETAILS, function(k) grepl(k, comp, fixed = TRUE)))) {
        filtered_components <- c(filtered_components, comp)
      }
    }
  }

  return(paste(filtered_components, collapse = "_"))
}


## ============================================================================
## PS1_1
## Select a target item and perform item-specific preprocessing
## ============================================================================

problem_num <- "ps1_1"
file <- paste0("us_", problem_num, ".txt")
file_path <- file.path("path/to/your/data", file)

data <- read_tsv(file_path, show_col_types = FALSE)

data <- data %>%
  select(-SEQID) %>%
  rename(SEQID = SEQID_unify)

# Fix RESTART event timestamps
data <- fix_restart_timestamps(data)

# Remove irrelevant event types for PS1_1
remove_events <- c(
  "DOACTION", "NEXT_INQUIRY", "NEXT_BUTTON", "CONFIRMATION_OPENED",
  "CONFIRMATION_CLOSED", "ENVIRONMENT", "MC_HELP_TOOLBAR", "MAIL_SENT",
  "MAIL_DELETED", "MC_HELP_MENUITEM", "SORT_MENU", "COPY", "PASTE",
  "NEW_FOLDER", "MC_SORT", "DELETE_FOLDER"
)
data <- data %>% filter(!(event_type %in% remove_events))

# Merge multi-row action patterns into single rows
pattern_map <- list(
  list(pattern = c("FOLDER_VIEWED", "MAIL_DROP", "MAIL_MOVED"), keep = "MAIL_DROP"),
  list(pattern = c("FOLDER_VIEWED", "MAIL_MOVED", "MAIL_DROP"), keep = "MAIL_DROP"),
  list(pattern = c("BUTTON", "MAIL_MOVED"),                     keep = "BUTTON"),
  list(pattern = c("BUTTON", "NEXT_ITEM", "END"),               keep = "BUTTON"),
  list(pattern = c("BREAKOFF", "END"),                           keep = "BREAKOFF")
)

df_merged <- merge_patterns(data, pattern_map)
df <- df_merged %>% select(-CNTRYID, -booklet_id, -item_id, -event_name)

# Remove duplicate rows
df <- df %>% distinct(.keep_all = TRUE)

# Merge consecutive KEYPRESS events
fin_data <- preprocess_keypress_data(df)

head(fin_data)


## ============================================================================
## Merge processed event_type and event_description into a single value
## ============================================================================

fin_data <- fin_data %>%
  mutate(
    event_type = tolower(event_type),
    event_description = tolower(event_description)
  )

fin_data <- fin_data %>%
  mutate(event_desc_list = sapply(event_description, process_event_description, USE.NAMES = FALSE))

# Combine event_type and parsed description into a single action label
fin_data <- fin_data %>%
  mutate(action_event = ifelse(
    event_desc_list == "",
    event_type,
    paste0(event_type, "_", event_desc_list)
  ))

result <- fin_data %>% select(SEQID, event_type, event_description, action_event, timestamp)

path_output <- file.path("path/to/your/output", sprintf("merged_%s.csv", problem_num))
write_csv(result, path_output)
