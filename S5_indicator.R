rm(list=ls())

# ============================================================
# Basic Process Indicators:
# Time on Task (ToT), Time to First Action (TFA), Number of Actions (NA)
# ============================================================

# ── User Configuration ───────────────────────────────────────────────────────────────
path <- "path/to/your/data/"   # set to directory containing ps1_usa.csv, prgusap1.csv
# ─────────────────────────────────────────────────────────────────────────────────────

library(dplyr)
library(ggplot2)
library(dplyr)

# ── 0. Load data ─────────────────────────────────────────────────────────────────────
ps1_data       <- read.csv(paste0(path, "ps1_usa.csv"), header = TRUE, row.names = 1)
ps1_score_data <- read.csv(paste0(path, "prgusap1.csv"), row.names = 1)

# ── 1. Exclude system events ─────────────────────────────────────────────────────────
# Exclude the "start" event, which is a system-generated item presentation marker.
# All remaining events are treated as task-relevant actions.
ps1_actions <- ps1_data %>% filter(event_type != "start")

# ── 2. Compute indicators per respondent (SEQID) ─────────────────────────────────────
process_indicators <- ps1_actions %>%
  group_by(SEQID) %>%
  summarise(

    # -- Time on Task (ToT) ------------------------------------
    # Total duration from item onset (t=0) to the last recorded action, 
    # used as a proxy for submission time.
    # Unit: milliseconds (divide by 1000 for seconds)
    ToT = max(timestamp) / 1000,
    
    # -- Time to First Action (TFA) ----------------------------
    # Latency between item presentation (t=0) and the first meaningful action.
    # Reflects initial comprehension speed and planning time before engaging with the task.
    # Unit: milliseconds (divide by 1000 for seconds)
    TFA = min(timestamp) / 1000,
    
    # -- Number of Actions (NA) --------------------------------
    # Total count of meaningful actions performed by the respondent.
    # Captures overall behavioral activity level.
    NoA = n(),
    
    .groups = "drop"
  )

# ── 3. Preview and summary ────────────────────────────────────────
head(process_indicators, n = 2)
summary(process_indicators[, c("ToT", "TFA", "NoA")])
