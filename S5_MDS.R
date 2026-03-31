rm(list = ls())

# ============================================================
# Multidimensional Scaling (MDS): Feature Extraction & Validation
# Based on Tang et al. (2020); interpretation following Zhang et al. (2024)
# ============================================================

# ── User Configuration ────────────────────────────────────────────
path <- "path/to/your/data/"   # directory containing ps1_usa.csv, prgusap1.csv
# ─────────────────────────────────────────────────────────────────

library(dplyr)
library(tidyr)
library(ggplot2)
library(glmnet)
library(pls)

# ── Step 0. Load Data ──────────────────────────────────────────────
ps1_data       <- read.csv(paste0(path, "ps1_usa.csv"), header = TRUE, row.names = 1)
ps1_score_data <- read.csv(paste0(path, "prgusap1.csv"), row.names = 1)

# ── Step 1. Construct Action Sequences per Respondent ──────────────
# Exclude start events, then collapse each respondent's actions
# into a time-ordered list indexed by SEQID

sequences <- ps1_data %>%
  filter(event_type != "start") %>%
  arrange(SEQID, timestamp) %>%
  group_by(SEQID) %>%
  summarise(seq = list(merged_event), .groups = "drop")

seq_list <- sequences$seq   # list of n action sequences

# ── Step 2. Compute OSS Dissimilarity Matrix ────────────────────────

# OSS (Optimal Subsequence Similarity) dissimilarity between two sequences:
# f = normalized positional discrepancy for shared actions
# g = count of actions unique to one sequence
# dissimilarity = (f + g) / (L1 + L2)

oss_dissimilarity <- function(s1, s2) {
  s1 <- unlist(s1); L1 <- length(s1)
  s2 <- unlist(s2); L2 <- length(s2)
  
  C  <- intersect(unique(s1), unique(s2))   # actions shared by both sequences
  U1 <- setdiff(unique(s1), unique(s2))     # actions only in s1
  U2 <- setdiff(unique(s2), unique(s1))     # actions only in s2
  
  # f: sum of positional differences for shared actions (normalized)
  f_val <- 0
  if (length(C) > 0) {
    for (a in C) {
      s1_a  <- which(s1 == a)
      s2_a  <- which(s2 == a)
      K12_a <- min(length(s1_a), length(s2_a))
      f_val <- f_val + sum(abs(s1_a[1:K12_a] - s2_a[1:K12_a]))
    }
    f_val <- f_val / max(L1, L2)
  }
  
  # g: total frequency of actions that appear in only one sequence
  g_val <- sum(s1 %in% U1) + sum(s2 %in% U2)
  
  return((f_val + g_val) / (L1 + L2))
}

# Compute pairwise dissimilarities for all respondents -> n x n matrix
compute_dissimilarity_matrix <- function(seqs) {
  n <- length(seqs)
  D <- matrix(0, n, n)
  for (i in 1:(n - 1)) {
    for (ip in (i + 1):n) {
      D[i, ip] <- D[ip, i] <- oss_dissimilarity(seqs[[i]], seqs[[ip]])
    }
  }
  return(D)
}

D <- compute_dissimilarity_matrix(seq_list)

# ── 4. Outcome variable ──────────────────────────────────────
# Dichotomize U01a: score > 0 = correct (He et al., 2019)
score <- ps1_score_data %>%
  mutate(SEQID   = paste0("US_", SEQID),
         score   = as.integer(U01a000S),
         correct = as.integer(score == 3)) %>%
  filter(!is.na(score)) %>%
  select(SEQID, score, correct)

# ── Step 3. Extract K Principal MDS Features ────────────────────────
# Following Tang et al. (2020): apply cmdscale to obtain MDS coordinates,
# then rotate via PCA to extract principal components.
# K is ideally chosen by cross-validation; here fixed at K = 50.

K       <- 50
mds_fit <- cmdscale(as.dist(D), k = K)                    # dissimilarity -> Euclidean coordinates
pca_fit <- prcomp(mds_fit, center = TRUE, scale. = FALSE)  # PCA rotation

pca_feature <- as.data.frame(pca_fit$x[, 1:K]) %>%
  mutate(SEQID = sequences$SEQID) %>%
  relocate(SEQID)

plot_df <- pca_feature %>%
  select(SEQID, PC1, PC2) %>%
  left_join(score, by = "SEQID") %>%
  filter(!is.na(correct)) %>%
  mutate(correct = factor(correct, levels = c(0, 1),
                          labels = c("Incorrect", "Correct")))

ggplot(plot_df, aes(x = PC1, y = PC2, color = correct)) +
  geom_point() +
  scale_color_manual(values = c("Incorrect" = "#F8766D",
                                "Correct"   = "#00BFC4")) +
  labs(x = "PF1", y = "PF2", color = "Correctness") +
  theme_bw()

# ── Step 4. Feature Interpretation ──────────────────────────────────
# PC1: Attentiveness
# Respondents with high PC1 tend to have longer sequences (more actions),
# suggesting more engaged or careful problem-solving behavior.
len_dist <- sapply(seq_list, length)
log_len  <- log(len_dist)
cat(sprintf("Correlation of PC1 and log sequence length: %.3f\n", abs(cor(pca_feature$PC1, log_len))))

high_pc <- len_dist[pca_feature$PC1 >= quantile(pca_feature$PC1, 0.9)]
low_pc  <- len_dist[pca_feature$PC1 <= quantile(pca_feature$PC1, 0.1)]
cat(sprintf("PC1 top 10%%    mean seq length: %.1f (SD = %.1f)\n", mean(high_pc), sd(high_pc)))
cat(sprintf("PC1 bottom 10%% mean seq length: %.1f (SD = %.1f)\n", mean(low_pc),  sd(low_pc)))

# PC2: Intensity of mail and folder viewing
# Compare the most frequent actions between respondents in the top vs. bottom
# decile of PC2 to understand what behavioral dimension PC2 captures.
high_idx <- which(pca_feature$PC2 >= quantile(pca_feature$PC2, 0.9)); high_seq <- unlist(seq_list[high_idx])
low_idx  <- which(pca_feature$PC2 <= quantile(pca_feature$PC2, 0.1)); low_seq  <- unlist(seq_list[low_idx])

pc2_actions <- data.frame(
  Rank        = 1:5,
  High_PC2    = names(sort(table(high_seq),      decreasing = TRUE)[1:5]),
  High_Freq   = as.integer(sort(table(high_seq), decreasing = TRUE)[1:5]),
  Low_PC2     = names(sort(table(low_seq),       decreasing = TRUE)[1:5]),
  Low_Freq    = as.integer(sort(table(low_seq),  decreasing = TRUE)[1:5])
)

pc2_actions

# ── Step 5. Outcome and Background Variables ────────────────────────
# - correct:  binary item score (full credit = score 3 on U01a)
# - numeracy/literacy: mean of 10 plausible values (PVs)
# - age, yrs_edu, ict_home, ict_work: background covariates

score <- ps1_score_data %>%
  mutate(
    SEQID    = paste0("US_", SEQID),
    correct  = as.integer(U01a000S == 3),
    numeracy = rowMeans(select(., starts_with("PVNUM")), na.rm = TRUE),
    literacy = rowMeans(select(., starts_with("PVLIT")), na.rm = TRUE),
    age      = suppressWarnings(as.numeric(AGEG5LFS)),
    yrs_edu  = suppressWarnings(as.numeric(YRSQUAL)),
    ict_home = suppressWarnings(as.numeric(ICTHOME)),
    ict_work = suppressWarnings(as.numeric(ICTWORK))
  ) %>%
  filter(!is.na(correct), !is.na(numeracy), !is.na(age),
         !is.na(yrs_edu), !is.na(ict_home), !is.na(ict_work)) %>%
  select(SEQID, correct, numeracy, literacy, age, yrs_edu, ict_home, ict_work)

# ── Step 6. Construct Validity of MDS Features ───
# Create binary indicators (presence/absence) for the top 50 most frequent actions.
# Predict each binary action indicator and item correctness from PC1-PC50
# High prediction accuracy indicates that MDS features preserve behavioral information.

key_actions  <- names(sort(table(unlist(seq_list)), decreasing = TRUE)[1:50])
derived_vars <- sapply(key_actions, function(a) {
  as.integer(sapply(seq_list, function(x) a %in% unlist(x)))
})

model_df <- cbind(pca_feature, derived_vars) %>% inner_join(score, by = "SEQID")

pc_cols     <- paste0("PC", 1:K)
target_vars <- c(key_actions, "correct")
accuracies  <- setNames(numeric(length(target_vars)), target_vars)

set.seed(42)
for (var in target_vars) {
  df  <- model_df %>% select(all_of(pc_cols), y = all_of(var)) %>% filter(!is.na(y))
  acc <- numeric(10)
  
  for (r in 1:10) {
    train_idx <- sample(nrow(df), floor(0.8 * nrow(df)))
    train_df  <- df[ train_idx, ]
    test_df   <- df[-train_idx, ]
    
    fit     <- glm(y ~ ., data = train_df, family = binomial, control = glm.control(maxit = 100))
    pred    <- as.integer(predict(fit, test_df, type = "response") > 0.5)
    acc[r]  <- mean(pred == test_df$y, na.rm = TRUE)
  }
  
  accuracies[var] <- mean(acc)
}

cat(sprintf("Mean accuracy: %.3f\n", mean(accuracies)))
cat(sprintf("Prop > 0.90:   %.3f\n", mean(accuracies > 0.9)))
hist(accuracies, breaks = seq(0.9, 1, by = 0.01), xlab = "Prediction Accuracy", main = "")

# ── Step 7. External Variable Prediction  ───────
# Baseline: predict external variable from item correctness only
# Process:  predict external variable using MDS features
# Evaluation metric: Out-of-Sample R^2
# Higher Process OSR^2 relative to Baseline indicates incremental predictive
# validity of process features beyond what item scores alone capture.

ext_vars     <- c("numeracy", "literacy", "age", "yrs_edu", "ict_home", "ict_work")
osr_results  <- data.frame(variable = ext_vars, baseline_OSR = NA, process_OSR = NA)
formula_proc <- as.formula(paste("~ correct + correct:(", paste(pc_cols, collapse = "+"), ") +",
                                 paste(pc_cols, collapse = "+")))

set.seed(42)
for (v in seq_along(ext_vars)) {
  var <- ext_vars[v]
  
  osr_base <- numeric(10)
  osr_proc <- numeric(10)
  
  for (r in 1:10) {
    train_idx <- sample(nrow(model_df), floor(0.8 * nrow(model_df)))
    train_df  <- model_df[ train_idx, ]
    test_df   <- model_df[-train_idx, ]
    y_train   <- train_df[[var]]
    y_test    <- test_df[[var]]
    
    fit_base       <- lm(y_train ~ correct, data = train_df)
    pred_base      <- predict(fit_base, newdata = test_df)
    osr_base[r]    <- cor(pred_base, y_test)
    
    X_train        <- model.matrix(formula_proc, train_df)[, -1]
    X_test         <- model.matrix(formula_proc, test_df)[, -1]
    cv_fit         <- cv.glmnet(X_train, y_train, alpha = 0)
    fit_proc       <- glmnet(X_train, y_train, alpha = 0, lambda = cv_fit$lambda.min)
    pred_proc      <- as.vector(predict(fit_proc, s = cv_fit$lambda.min, newx = X_test))
    osr_proc[r]    <- cor(pred_proc, y_test)
  }
  
  osr_results$baseline_OSR[v] <- mean(osr_base)
  osr_results$process_OSR[v]  <- mean(osr_proc)
}

print(osr_results)

# Visualization: grouped bar chart comparing Baseline vs. Process OSR^2
osr_results_gg <- osr_results %>%
  pivot_longer(cols = c("baseline_OSR", "process_OSR"), 
               names_to = "Model", values_to = "OSR") %>%
  mutate(
    Model = factor(Model, levels = c("baseline_OSR", "process_OSR"),
                          labels = c("Response", "MDS")),
    Variable = factor(variable, levels = ext_vars, 
                      labels = c("Numeracy", "Literacy", "Age", "Education (Years)", 
                                 "ICT (Home)", "ICT (Work)"))
  )

ggplot(osr_results_gg, aes(x = Variable, y = OSR, fill = Model)) +
  geom_bar(stat = "identity", position = position_dodge(width = 0.7), width = 0.6) +
  geom_text(aes(label = round(OSR, 3)), position = position_dodge(width = 0.7), vjust = -0.4) +
  labs(y = expression(paste("O.S.R"))) + theme_bw()

# ── Step 8. PLS Analysis: Interpreting MDS Features (Zhang et al., 2024)
# Partial Least Squares (PLS) extracts latent components that maximize
# covariance between the MDS features (PC1-50) and age.

# Fit PLS and select optimal number of components via CV (minimize RMSEP)
pls_fit <- plsr(age ~ ., data = model_df %>% select(age, all_of(pc_cols)), scale = TRUE, validation = "CV")
n_comp  <- which.min(RMSEP(pls_fit)$val[1, 1, ]) 

# Append PLS component scores to model_df
pls_scores <- as.data.frame(scores(pls_fit)[, 1:n_comp])
colnames(pls_scores) <- paste0("PLS", 1:n_comp)
model_df   <- cbind(model_df, pls_scores)

# Generate trigrams for each respondent's sequence
# Trigrams enable more interpretable linking of PLS components to behavioral patterns.
get_trigrams <- function(seq) {
  s <- unlist(seq)
  if (length(s) < 3) return(character(0))
  paste(s[1:(length(s)-2)], s[2:(length(s)-1)], s[3:length(s)], sep = "->")
}
trigram_list <- lapply(seq_list[match(model_df$SEQID, sequences$SEQID)], get_trigrams)

# Convert top 100 most frequent trigrams into binary presence indicators
top_trigrams <- names(sort(table(unlist(trigram_list)), decreasing = TRUE)[1:100])
trigram_mat  <- sapply(top_trigrams, function(b) {
  as.integer(sapply(trigram_list, function(x) b %in% x))
})
trigram_df   <- as.data.frame(trigram_mat)

# Build Table 4 (replicating Zhang et al., 2024 structure):
# rho_Y       = correlation between PLS component and age
# Pattern     = trigram with the highest absolute correlation with the component
# rho_pattern = correlation between PLS component and the representative trigram
pls_table <- data.frame()

for (k in 1:n_comp) {
  comp  <- paste0("PLS", k)
  rho_y <- cor(model_df[[comp]], model_df$age, use = "complete.obs")
  rho_score <- cor(model_df[[comp]], model_df$correct, use = "complete.obs")
  
  trigram_cors <- sapply(top_trigrams, function(b) {
    cor(model_df[[comp]], trigram_df[[b]], use = "complete.obs")
  })
  top_idx <- which.max(abs(trigram_cors))
  
  pls_table <- rbind(pls_table, data.frame(
    Variable    = ifelse(k == 1, "Age", ""),
    PLS         = k,
    rho_Y       = round(rho_y, 2),
    Pattern     = top_trigrams[top_idx],
    rho_pattern = round(trigram_cors[top_idx], 2),
    rho_score   = round(rho_score, 2)
  ))
}

print("=== Table: PLS Pattern Interpretation ===")
print(pls_table)

