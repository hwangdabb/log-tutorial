rm(list = ls())

# ============================================================
# Hidden Markov Model: Latent State Inference and GGM Network Analysis
# Note: run S6_HMM_PREPROCESSING.R first to generate ps1_data2.csv
# ============================================================

# ── User Configuration ────────────────────────────────────────────
path <- "path/to/your/data/"   # directory containing ps1_data2.csv, prgusap1.csv
# ─────────────────────────────────────────────────────────────────

library(depmixS4)
library(tidyverse)
library(qgraph)
library(bootnet)

# ── Step 0. Load Data ──────────────────────────────────────────────
ps1_data       <- read.csv(paste0(path, "preprocessing/ps1_data2.csv"), header = TRUE, row.names = 1)
ps1_score_data <- read.csv(paste0(path, "prgusap1.csv"), row.names = 1)

# ── Step 1. Outcome variable ───────────────────────────────────────
score <- ps1_score_data %>%
  mutate(SEQID   = paste0("US_", SEQID),
         score   = as.integer(U01a000S),
         correct = as.integer(score == 3)) %>%
  filter(!is.na(score)) %>%
  select(SEQID, score, correct)

# ── Step 2. Prepare sequences ──────────────────────────────────────
# action_recoded: action categories recoded from merged_event (see preprocessing)
item_data <- ps1_data %>%
  left_join(score, by = "SEQID") %>%
  filter(!is.na(correct)) %>%
  mutate(action_int = as.integer(factor(action_recoded)))

correct_data   <- item_data %>% filter(correct == 1)
incorrect_data <- item_data %>% filter(correct == 0)
correct_action_levels   <- levels(factor(correct_data$action_recoded))
incorrect_action_levels <- levels(factor(incorrect_data$action_recoded))
                                  
cat("Correct N:  ", n_distinct(correct_data$SEQID), "\n")
cat("Incorrect N:", n_distinct(incorrect_data$SEQID), "\n")

# ── Step 3. Model Selection (K = 1--9) ───────────────────────────────
fit_hmm <- function(data, K, seed = 42) {
  seq_lengths <- data %>%
    group_by(SEQID) %>% summarise(n = n(), .groups = "drop") %>% pull(n)
    mod <- depmix(action_int ~ 1, data = data, nstates = K,
                  ntimes = seq_lengths, family = multinomial())
  set.seed(seed)
  fit_mod <- fit(mod, verbose = FALSE)
  return(fit_mod)
}

select_K <- function(data, K_range = 1:9, label = "") {
  map_dfr(K_range, function(K) {
    cat(label, "K =", K, "\n")
    fit <- tryCatch(fit_hmm(data, K), error = function(e) NULL)
    if (is.null(fit)) return(NULL)
    data.frame(K = K, AIC = AIC(fit), BIC = BIC(fit))
  })
}

ic_correct   <- select_K(correct_data,   label = "Correct")
ic_incorrect <- select_K(incorrect_data, label = "Incorrect")

ic_correct %>%
  pivot_longer(c(AIC, BIC), names_to = "criterion", values_to = "value") %>%
  ggplot(aes(x = K, y = value, color = criterion)) +
  geom_line() + geom_point() + scale_x_continuous(breaks = 1:9) + theme_bw()

ic_incorrect %>%
  pivot_longer(c(AIC, BIC), names_to = "criterion", values_to = "value") %>%
  ggplot(aes(x = K, y = value, color = criterion)) +
  geom_line() + geom_point() + scale_x_continuous(breaks = 1:9) + theme_bw()

# ── Step 4. Fit Final Model ──────────────────────────────────────────
K <- 9

hmm_correct   <- fit_hmm(correct_data,   K)
hmm_incorrect <- fit_hmm(incorrect_data, K)

# ── Step 5. Emission and Transition Matrices ─────────────────────────
get_emission_probs <- function(fit, action_levels, K) {
  em <- t(sapply(1:K, function(k) {
    coefs     <- fit@response[[k]][[1]]@parameters$coefficients
    exp_coefs <- exp(coefs)
    exp_coefs / sum(exp_coefs)   # softmax normalization
  }))
  rownames(em) <- paste0("S", 1:K)
  colnames(em) <- action_levels
  return(em)
}

get_transition <- function(fit, K) {
  tr <- t(sapply(1:K, function(k) fit@transition[[k]]@parameters$coefficients))
  rownames(tr) <- colnames(tr) <- paste0("S", 1:K)
  return(tr)
}

em_correct   <- get_emission_probs(hmm_correct,   correct_action_levels,   K)
em_incorrect <- get_emission_probs(hmm_incorrect, incorrect_action_levels, K)

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

trans_correct   <- get_transition(hmm_correct,   K)
trans_incorrect <- get_transition(hmm_incorrect, K) 

# ── Step 6. Viterbi state Sequences and Group Comparison ─────────────
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
qgraph(ggm_correct$graph, title = "Correct", labels = paste0("S", 1:K))
qgraph(ggm_incorrect$graph, title = "Incorrect", labels = paste0("S", 1:K))






# ── Step 9. Additional Visualizations ────────────────────────────────

# IC criterion plot (faceted by group)
ic_correct$group   <- "Correct"
ic_incorrect$group <- "Incorrect"
ic_all             <- bind_rows(ic_correct, ic_incorrect)

p_ic <- ic_all %>%
  pivot_longer(cols = c(AIC, BIC), names_to = "criterion", values_to = "value") %>%
  ggplot(aes(x = K, y = value, color = criterion, linetype = criterion)) +
  geom_line(linewidth = 0.8) + geom_point(size = 2) +
  facet_wrap(~ group) +
  scale_x_continuous(breaks = 1:9) +
  labs(x = "Number of States (K)", y = "Information Criterion",
       color = NULL, linetype = NULL) +
  theme_bw()
print(p_ic)

# Emission and transition heatmaps
plot_emission <- function(em, title = "") {
  em_df <- as.data.frame(em) %>%
    rownames_to_column("state") %>%
    pivot_longer(-state, names_to = "action", values_to = "prob") %>%
    filter(prob > 0.01)
  
  ggplot(em_df, aes(x = state, y = action, size = prob, color = prob)) +
    geom_point() +
    scale_size_continuous(range = c(1, 10), limits = c(0, 1)) +
    scale_color_gradient(low = "lightblue", high = "red", limits = c(0, 1)) +
    labs(title = title, x = "State", y = "Action") +
    theme_bw() +
    theme(axis.text.y = element_text(size = 7),
          axis.text.x = element_text(size = 9),
          plot.title  = element_text(hjust = 0.5),
          legend.position = "none")
}

plot_transition <- function(tr, title = "") {
  as.data.frame(tr) %>%
    rownames_to_column("from") %>%
    pivot_longer(-from, names_to = "to", values_to = "prob") %>%
    mutate(
      from = factor(from, levels = paste0("S", 1:nrow(tr))),
      to   = factor(to,   levels = paste0("S", 1:nrow(tr)))
    ) %>%
    ggplot(aes(x = to, y = from, fill = prob)) +
    geom_tile(color = "white", linewidth = 0.5) +
    geom_text(aes(label = ifelse(prob > 0.05, round(prob, 2), "")),
              size = 3, color = "white", fontface = "bold") +
    scale_fill_gradient(low = "lightblue", high = "red", 
                        limits = c(0, 1)) +
    labs(title = title, x = "To", y = "From") +
    theme_bw() +
    theme(plot.title   = element_text(hjust = 0.5),
          panel.grid   = element_blank(),
          axis.text    = element_text(size = 9),
          legend.position = "none")
}

legend_plot <- ggplot(data.frame(x = 0, y = 0, prob = c(0, 1)),
                      aes(x, y, color = prob, size = prob)) +
  geom_point() +
  scale_color_gradient(low = "lightblue", high = "red", 
                       limits = c(0, 1), name = "Probability") +
  scale_size_continuous(range = c(1, 10), 
                        limits = c(0, 1), name = "Probability") +
  theme_void() +
  theme(legend.position = "right",
        legend.title    = element_text(size = 10),
        legend.text     = element_text(size = 9)) +
  guides(color = guide_colorbar(title = "Probability"),
         size  = guide_legend(title  = "Probability"))

get_legend <- function(p) {
  g <- ggplotGrob(p)
  leg <- g$grobs[[which(sapply(g$grobs, function(x) x$name) == "guide-box")]]
  return(leg)
}

shared_legend <- get_legend(legend_plot)

# individual plots
p1 <- plot_emission(em_correct,       "")
p2 <- plot_emission(em_incorrect,     "")
p3 <- plot_transition(trans_correct,   "")
p4 <- plot_transition(trans_incorrect, "")
collect_plot <- (p1 | p3) / (p2 | p4)
print(collect_plot + plot_layout(guides = "collect") & theme(legend.position = "right"))

# State frequency comparison by behavioral label
# state -> behavioral label mapping
correct_map   <- c("1"="Mail/Folder View", "2"="Submission",
                   "3"="Mail Drag",        "4"="Button Next",
                   "5"="Start",            "6"="Mail View",
                   "7"="Exploration",      "8"="Mail Drop",  
                   "9"="Mail View")

incorrect_map <- c("1"="Mail/Folder View", "2"="Button Next",
                   "3"="Mail View",        "4"="Mail Drop",
                   "5"="Mail View",        "6"="Start",
                   "7"="Mail Drag",        "8"="Exploration",
                   "9"="Submission")

freq_labeled <- bind_rows(
  data.frame(state = as.character(posterior(hmm_correct)$state),
             SEQID = correct_data$SEQID, group = "Correct"),
  data.frame(state = as.character(posterior(hmm_incorrect)$state),
             SEQID = incorrect_data$SEQID, group = "Incorrect")
) %>%
  mutate(label = ifelse(group == "Correct",
                        correct_map[state],
                        incorrect_map[state])) %>%
  count(group, SEQID, label) %>%
  group_by(group, label) %>%
  summarise(avg_freq = mean(n), .groups = "drop")

plot3 <- ggplot(freq_labeled, aes(x = label, y = avg_freq, fill = group)) +
  geom_bar(stat = "identity", position = "dodge") +
  labs(x = "State", y = "Average Frequency", fill = NULL) +
  scale_fill_manual(values = c("Correct" = "#00BFC4", "Incorrect" = "#F8766D")) +
  scale_x_discrete(guide = guide_axis(angle = 45)) +
  theme_bw()
print(plot3)

# GGM network visualization
par(mfrow = c(1, 2))
qgraph(ggm_correct$graph,
       layout     = "spring",
       labels     = paste0("S", 1:K),
       color      = "#00BFC4")
qgraph(ggm_incorrect$graph,
       layout     = "spring",
       labels     = paste0("S", 1:K),
       color      = "#F8766D")
