rm(list = ls())

# ============================================================
# Hidden Markov Model: Latent State Inference and GGM Network Analysis
# ============================================================

# ── User Configuration ────────────────────────────────────────────
path <- "path/to/your/data/"   # directory containing ps1_usa.csv, prgusap1.csv
# ─────────────────────────────────────────────────────────────────

library(depmixS4)
library(tidyverse)
library(qgraph)
library(bootnet)

# ── Step 0. Load Data ──────────────────────────────────────────────
# Requires merged_event column — run preprocessing.R first if not present.
ps1_data       <- read.csv(paste0(path, "ps1_usa.csv"), header = TRUE, row.names = 1)
ps1_score_data <- read.csv(paste0(path, "prgusap1.csv"), row.names = 1)

# ── Step 1. Outcome variable ───────────────────────────────────────
score <- ps1_score_data %>%
  mutate(SEQID   = paste0("US_", SEQID),
         score   = as.integer(U01a000S),
         correct = as.integer(score == 3)) %>%
  filter(!is.na(score)) %>%
  select(SEQID, score, correct)

# ── Step 2. Prepare sequences ──────────────────────────────────────
item_data <- ps1_data %>%
  left_join(score, by = "SEQID") %>%
  filter(!is.na(correct)) %>%
  mutate(action_int = as.integer(factor(merged_event)))

correct_data   <- item_data %>% filter(correct == 1)
incorrect_data <- item_data %>% filter(correct == 0)
correct_action_levels   <- levels(factor(correct_data$merged_event))
incorrect_action_levels <- levels(factor(incorrect_data$merged_event))

cat("Correct N:  ", n_distinct(correct_data$SEQID), "\n")
cat("Incorrect N:", n_distinct(incorrect_data$SEQID), "\n")

# ── Step 3. Model Selection (Q = 1--9) ───────────────────────────────
fit_hmm <- function(data, Q, seed = 42) {
  seq_lengths <- data %>%
    group_by(SEQID) %>% summarise(n = n(), .groups = "drop") %>% pull(n)
  mod <- depmix(action_int ~ 1, data = data, nstates = Q,
                ntimes = seq_lengths, family = multinomial())
  set.seed(seed)
  fit_mod <- fit(mod, verbose = FALSE)
  return(fit_mod)
}

select_Q <- function(data, Q_range = 1:9, label = "") {
  map_dfr(Q_range, function(Q) {
    cat(label, "Q =", Q, "\n")
    fit <- tryCatch(fit_hmm(data, Q), error = function(e) NULL)
    if (is.null(fit)) return(NULL)
    data.frame(Q = Q, AIC = AIC(fit), BIC = BIC(fit))
  })
}

ic_correct   <- select_Q(correct_data,   label = "Correct")
ic_incorrect <- select_Q(incorrect_data, label = "Incorrect")

ic_correct %>%
  pivot_longer(c(AIC, BIC), names_to = "criterion", values_to = "value") %>%
  ggplot(aes(x = Q, y = value, color = criterion)) +
  geom_line() + geom_point() + scale_x_continuous(breaks = 1:9) + theme_bw()

ic_incorrect %>%
  pivot_longer(c(AIC, BIC), names_to = "criterion", values_to = "value") %>%
  ggplot(aes(x = Q, y = value, color = criterion)) +
  geom_line() + geom_point() + scale_x_continuous(breaks = 1:9) + theme_bw()

# ── Step 4. Fit Final Model ──────────────────────────────────────────
Q <- 9

hmm_correct   <- fit_hmm(correct_data,   Q)
hmm_incorrect <- fit_hmm(incorrect_data, Q)

# ── Step 5. Emission and Transition Matrices ─────────────────────────
get_emission_probs <- function(fit, action_levels, Q) {
  em <- t(sapply(1:Q, function(q) {
    coefs     <- fit@response[[q]][[1]]@parameters$coefficients
    exp_coefs <- exp(coefs)
    exp_coefs / sum(exp_coefs)   # softmax normalization
  }))
  rownames(em) <- paste0("S", 1:Q)
  colnames(em) <- action_levels
  return(em)
}

get_transition <- function(fit, Q) {
  tr <- t(sapply(1:Q, function(q) fit@transition[[q]]@parameters$coefficients))
  rownames(tr) <- colnames(tr) <- paste0("S", 1:Q)
  return(tr)
}

em_correct   <- get_emission_probs(hmm_correct,   correct_action_levels,   Q)
em_incorrect <- get_emission_probs(hmm_incorrect, incorrect_action_levels, Q)

# Top 3 actions per state (for state labeling)
top3_table <- function(em, group_label) {
  data.frame(
    Group = group_label,
    State = paste0("S", 1:nrow(em)),
    Top3  = apply(em, 1, function(row) {
      idx <- order(row, decreasing = TRUE)[1:3]
      paste0(colnames(em)[idx], "(", round(row[idx], 3), ")", collapse = ", ")
    })
  )
}

correct_table   <- top3_table(em_correct,   "Correct")
incorrect_table <- top3_table(em_incorrect, "Incorrect")

trans_correct   <- get_transition(hmm_correct,   Q)
trans_incorrect <- get_transition(hmm_incorrect, Q) 

# ── Step 6. Viterbi State Sequences and Group Comparison ─────────────
freq_compare <- bind_rows(
  data.frame(state = as.character(posterior(hmm_correct)$state),
             SEQID = correct_data$SEQID,   group = "Correct"),
  data.frame(state = as.character(posterior(hmm_incorrect)$state),
             SEQID = incorrect_data$SEQID, group = "Incorrect")
) %>%
  count(group, SEQID, state) %>%                   # count state visits per respondent
  group_by(group, state) %>%
  summarise(avg_freq = mean(n), .groups = "drop")  # average visit frequency per group 

print(freq_compare)

# ── Step 7. GGM on Emission Matrices ─────────────────────────────────
ggm_correct   <- estimateNetwork(t(em_correct), default = "EBICglasso", threshold = TRUE)
ggm_incorrect <- estimateNetwork(t(em_incorrect), default = "EBICglasso", threshold = TRUE)

# ── Step 8. Visualize GGM Networks ───────────────────────────────────
par(mfrow = c(1, 2))
qgraph(ggm_correct$graph, title = "Correct", labels = paste0("S", 1:Q))
qgraph(ggm_incorrect$graph, title = "Incorrect", labels = paste0("S", 1:Q))
