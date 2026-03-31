rm(list = ls())

# ============================================================
# HMM Preprocessing: Action Recoding
# Run this script before S6_HMM.R and S6_SIP.R.
# Output: preprocessing/ps1_data2.csv
# ============================================================

# ── User Configuration ───────────────────────────────────────
path <- "path/to/your/data/"   # directory containing merged_ps1_1.csv
# ─────────────────────────────────────────────────────────────

# ── 0. Load Packages ───────────────────────────────────────────
library(tidyverse)

# ── 1. Load Data ───────────────────────────────────────────────
ps1_data <- read.csv(paste0(path, "preprocessing/merged_ps1_1.csv"), header = TRUE, row.names = 1)

# ── 2. Action Recoding ─────────────────────────────────────────
item_data <- ps1_data %>%
  mutate(
    action_recoded = case_when(
      # 1. start
      merged_event == "start"                             ~ "start",
      # 2. mail_viewed
      str_detect(merged_event, "^mail_viewed")            ~ "mail_viewed",
      # 3. mail_drag
      str_detect(merged_event, "^mail_drag")              ~ "mail_drag",
      # 4. mail_drop
      str_detect(merged_event, "^mail_drop")              ~ "mail_drop",
      # 5. folder_viewed
      str_detect(merged_event, "^folder_viewed")          ~ "folder_viewed",
      # 6. folder_unfolded
      str_detect(merged_event, "^folder_unfolded")        ~ "folder_unfolded",
      # 7. folder_folded
      str_detect(merged_event, "^folder_folded")          ~ "folder_folded",
      # 8. keypress
      str_detect(merged_event, "^keypress")               ~ "keypress",
      # 9. textbox_onfocus
      str_detect(merged_event, "^textbox_onfocus")        ~ "textbox_onfocus",
      # 10. textbox_killfocus
      str_detect(merged_event, "^textbox_killfocus")      ~ "textbox_killfocus",
      # 11. toolbar
      str_detect(merged_event, "^toolbar")                ~ "toolbar",
      # 12. button - end confirm
      merged_event == "button_endtask_txt3"               ~ "button_end_confirm",
      # 13. button - end cancel
      merged_event == "button_endtask_txt4"               ~ "button_end_cancel",
      # 14. button - next inquiry
      merged_event == "button_nextinquiry_button"         ~ "button_next",
      # 15. button - all other button events
      str_detect(merged_event, "^button")                 ~ "button_other",
      # 16. all other events kept as-is
      TRUE                                                ~ merged_event
    ),
    action_int = as.integer(factor(action_recoded))
  )

# ── 4. Check ───────────────────────────────────────────────────
freq_table <- item_data %>%
  count(action_recoded, sort = TRUE) %>%
  mutate(prop = round(n / sum(n), 3))
print(freq_table)

cat("Number of action categories:", n_distinct(item_data$action_recoded), "\n")
cat("Number of respondents:      ", n_distinct(item_data$SEQID), "\n")

# ── 5. Save ────────────────────────────────────────────────────
write.csv(item_data, paste0(path, "preprocessing/ps1_data2.csv"))
cat("Saved:", paste0(path, "preprocessing/ps1_data2.csv"), "\n")
