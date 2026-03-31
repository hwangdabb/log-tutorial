rm(list = ls())

# ============================================================
# Additional Preprocessing: Action Recoding
# Recode granular merged_event labels into 15 behaviorally
# meaningful categories, and append merged_event to ps1_usa.csv.
# Run this script once before running S6_HMM.R or S6_SIP.R.
# ============================================================

# ── User Configuration ───────────────────────────────────────
path <- "path/to/your/data/"   # directory containing ps1_usa.csv
# ─────────────────────────────────────────────────────────────

library(tidyverse)

# ── 1. Load Data ───────────────────────────────────────────────
ps1_data <- read.csv(paste0(path, "ps1_usa.csv"), header = TRUE, row.names = 1)

# ── 2. Action Recoding ─────────────────────────────────────────
# Collapse fine-grained action_event strings into 15 categories (-> merged_event).
ps1_data <- ps1_data %>%
  mutate(
    merged_event = case_when(
      action_event == "start"                             ~ "start",
      str_detect(action_event, "^mail_viewed")            ~ "mail_viewed",
      str_detect(action_event, "^mail_drag")              ~ "mail_drag",
      str_detect(action_event, "^mail_drop")              ~ "mail_drop",
      str_detect(action_event, "^folder_viewed")          ~ "folder_viewed",
      str_detect(action_event, "^folder_unfolded")        ~ "folder_unfolded",
      str_detect(action_event, "^folder_folded")          ~ "folder_folded",
      str_detect(action_event, "^keypress")               ~ "keypress",
      str_detect(action_event, "^textbox_onfocus")        ~ "textbox_onfocus",
      str_detect(action_event, "^textbox_killfocus")      ~ "textbox_killfocus",
      str_detect(action_event, "^toolbar")                ~ "toolbar",
      action_event == "button_endtask_txt3"               ~ "button_end_confirm",
      action_event == "button_endtask_txt4"               ~ "button_end_cancel",
      action_event == "button_nextinquiry_button"         ~ "button_next",
      str_detect(action_event, "^button")                 ~ "button_other",
      TRUE                                                ~ action_event
    ),
    action_int = as.integer(factor(merged_event))
  )

# ── 3. Check ───────────────────────────────────────────────────
freq_table <- ps1_data %>%
  count(merged_event, sort = TRUE) %>%
  mutate(prop = round(n / sum(n), 3))
print(freq_table)

cat("Number of action categories:", n_distinct(ps1_data$merged_event), "\n")
cat("Number of respondents:      ", n_distinct(ps1_data$SEQID), "\n")

# ── 4. Save ────────────────────────────────────────────────────
write.csv(ps1_data, paste0(path, "ps1_usa.csv"))
cat("Saved:", paste0(path, "ps1_usa.csv"), "\n")
