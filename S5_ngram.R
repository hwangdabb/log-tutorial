rm(list=ls())

# ============================================================
# N-gram Analysis: Feature Extraction and Clustering
# ============================================================

# ── User Configuration ───────────────────────────────────────
path <- "path/to/your/data/"   # directory containing merged_ps1_1.csv, prgusap1.csv
# ─────────────────────────────────────────────────────────────

library(dplyr)
library(tidyr)
library(stringr)
library(ggplot2)

# ── 0. Load data ─────────────────────────────────────────────
ps1_data       <- read.csv(paste0(path, "preprocessing/merged_ps1_1.csv"), header = TRUE, row.names = 1)
ps1_score_data <- read.csv(paste0(path, "prgusap1.csv"), row.names = 1)

# ── 1. Construct action sequences per respondent ─────────────
sequences <- ps1_data %>%
  filter(event_type != "start") %>%
  arrange(SEQID, timestamp) %>%
  group_by(SEQID) %>%
  summarise(seq = list(merged_event), .groups = "drop")

# ── 2. Extract n-grams ───────────────────────────────────────
# Boundary tokens START/END preserve initial and terminal behaviors
extract_ngrams <- function(seq, n) {
  seq_padded <- c("START", seq, "END")
  T <- length(seq_padded)
  if (T < n) return(character(0))
  sapply(1:(T - n + 1), function(i) {
    paste(seq_padded[i:(i + n - 1)], collapse = "-")
  })
}

get_ngram_df <- function(sequences, n) {
  sequences %>%
    rowwise() %>%
    mutate(ngrams = list(extract_ngrams(seq, n))) %>%
    unnest(ngrams) %>%
    rename(pattern = ngrams) %>%
    select(SEQID, pattern)
}

unigrams <- get_ngram_df(sequences, n = 1)
bigrams  <- get_ngram_df(sequences, n = 2)
trigrams <- get_ngram_df(sequences, n = 3)

# ── 3. Compute TF-ISF weights and filter patterns ────────────
# ISF_v  = log(N / sf_v)
# TF-ISF = (1 + log(tf_v,ij)) * ISF_v
# Patterns are filtered following He et al. (2019):
# (i)  sf < 5: insufficient support
# (ii) ISF = 0: used by all respondents (no discriminative value)
compute_tfisf <- function(ngram_df, min_sf = 5) {
  N  <- n_distinct(ngram_df$SEQID)
  tf <- ngram_df %>% count(SEQID, pattern, name = "tf")
  sf <- tf %>% count(pattern, name = "sf")
  tf %>%
    left_join(sf, by = "pattern") %>%
    mutate(ISF   = log(N / sf),
           TFISF = (1 + log(tf)) * ISF) %>%
    filter(sf >= min_sf,   # (i)
           ISF > 0)        # (ii)
}

tfisf_uni <- compute_tfisf(unigrams)
tfisf_bi  <- compute_tfisf(bigrams)
tfisf_tri <- compute_tfisf(trigrams)

# ── 4. Outcome variable ──────────────────────────────────────
# Dichotomize U01a: score > 0 = correct (He et al., 2019)
score <- ps1_score_data %>%
  mutate(SEQID   = paste0("US_", SEQID),
         score   = as.integer(U01a000S),
         correct = as.integer(score == 3)) %>%
  filter(!is.na(score)) %>%
  select(SEQID, score, correct)

# ── 5. Chi-square screening ──────────────────────────────────
# Identify patterns most strongly associated with correctness
chisq_screen <- function(tfisf_df, score_df) {
  presence <- tfisf_df %>%
    distinct(SEQID, pattern) %>%
    mutate(present = 1)
  
  expand.grid(SEQID   = unique(score_df$SEQID),
              pattern = unique(presence$pattern),
              stringsAsFactors = FALSE) %>%
    left_join(presence, by = c("SEQID", "pattern")) %>%
    mutate(present = replace_na(present, 0)) %>%
    left_join(score_df, by = "SEQID") %>%
    group_by(pattern) %>%
    summarise(
      prop_correct   = mean(present[correct == 1]),
      prop_incorrect = mean(present[correct == 0]),
      chisq = tryCatch(
        chisq.test(table(present, correct))$statistic,
        error = function(e) NA_real_),
      .groups = "drop") %>%
    filter(!is.na(chisq)) %>%
    mutate(dominant = ifelse(prop_correct > prop_incorrect,
                             "Correct", "Incorrect")) %>%
    arrange(desc(chisq))
}

result_uni <- chisq_screen(tfisf_uni, score)
result_bi  <- chisq_screen(tfisf_bi,  score)
result_tri <- chisq_screen(tfisf_tri, score)

# Top 5 per group per n-gram order
format_top5 <- function(result, ngram_label) {
  bind_rows(
    result %>% filter(dominant == "Correct")   %>% head(5),
    result %>% filter(dominant == "Incorrect") %>% head(5)
  ) %>%
    select(dominant, pattern, chisq) %>%
    mutate(ngram = ngram_label)
}

top_table <- bind_rows(
  format_top5(result_uni, "Unigrams"),
  format_top5(result_bi,  "Bigrams"),
  format_top5(result_tri, "Trigrams")
) %>%
  select(dominant, ngram, pattern, chisq) %>%
  arrange(dominant, ngram)

print(top_table, n = 30)

# ── 6. K-means clustering ────────────────────────────────────
# Features: unigrams (TF-ISF) 
feature_matrix <- tfisf_uni %>%
  select(SEQID, pattern, TFISF) %>%
  pivot_wider(names_from  = pattern,
              values_from = TFISF,
              values_fill = 0)

seqids <- feature_matrix$SEQID
X      <- feature_matrix %>% select(-SEQID) %>% as.matrix()

# k = 3 selected based on elbow and silhouette methods
set.seed(42)
km <- kmeans(X, centers = 3, nstart = 100)

# ── 7. Cluster results ───────────────────────────────────────
cluster_result <- tibble(SEQID = seqids, cluster = km$cluster) %>%
  left_join(score, by = "SEQID") %>%
  left_join(ps1_score_data %>%
              mutate(SEQID = paste0("US_", SEQID)) %>%
              select(SEQID, PV1 = PVPSL1),
            by = "SEQID") %>%
  mutate(cluster = factor(cluster, labels = c("Cluster 1", "Cluster 2", "Cluster 3")))

# ── 8. Visualization and ANOVA ───────────────────────────────
# -- PV1 by cluster --------------------------------
ggplot(cluster_result, aes(x = cluster, y = PV1, fill = cluster)) +
  geom_boxplot() +
  labs(x = "", y = "PS-TRE Proficiency (PV1)") + theme_bw()

# -- PV1 by correctness nested in cluster ----------
cluster_result2 <- cluster_result %>%
  filter(!is.na(correct), !is.na(PV1)) %>%
  mutate(Correctness = factor(correct, levels = c(0, 1), labels = c("Incorrect", "Correct")))

ggplot(cluster_result2, aes(x = cluster, y = PV1, fill = Correctness)) +
  geom_boxplot() +
  labs(x = "", y = "PS-TRE Proficiency (PV1)") + theme_bw()

# -- Two-way ANOVA: cluster × correctness ----------
twoway_aov <- aov(PV1 ~ cluster * correct, data = cluster_result2)
summary(twoway_aov)
TukeyHSD(twoway_aov)

# -- Figure 3: Interaction plot --------------------------------
interaction_summary <- cluster_result2 %>%
  group_by(cluster, correct) %>%
  summarise(
    mean_PV1 = mean(PV1, na.rm = TRUE), se_PV1 = sd(PV1, na.rm = TRUE) / sqrt(n()),
    .groups  = "drop"
  )

ggplot(interaction_summary, aes(x = cluster, y = mean_PV1,
                                color = correct, group = correct)) +
  geom_line() + geom_point() +
  geom_errorbar(aes(ymin = mean_PV1 - se_PV1, ymax = mean_PV1 + se_PV1), width = 0.15) +
  labs(x = "", y = "Mean PS-TRE Proficiency (PV1)", color = "Correctness") + theme_bw()
