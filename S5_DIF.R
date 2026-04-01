rm(list = ls())

# ============================================================
# DIF Analysis with Process Data
# Following Qi (2023) and Chen et al. (2025)
# Applied to Party Invitations -- U01a (United States sample)
# ============================================================

# ── User Configuration ───────────────────────────────────────
path <- "path/to/your/data/"   # directory containing ps1_usa.csv, prgusap1.csv
# ─────────────────────────────────────────────────────────────

library(dplyr)
library(tidyr)
library(ggplot2)
library(patchwork)

# ── Step 0. Load data ──────────────────────────────────────────────
ps1_data       <- read.csv(paste0(path, "ps1_usa.csv"), header = TRUE, row.names = 1)
ps1_score_data <- read.csv(paste0(path, "prgusap1.csv"), row.names = 1)

# -- Step 1. Construct Action Sequences -----------------------------------------
sequences <- ps1_data %>%
  filter(event_type != "start") %>%
  arrange(SEQID, timestamp) %>%
  group_by(SEQID) %>%
  summarise(seq = list(action_event), .groups = "drop")

seq_list <- sequences$seq

# -- Step 2. OSS Dissimilarity Matrix -------------------------------------------
oss_dissimilarity <- function(s1, s2) {
  s1 <- unlist(s1); L1 <- length(s1)
  s2 <- unlist(s2); L2 <- length(s2)
  C  <- intersect(unique(s1), unique(s2))
  U1 <- setdiff(unique(s1), unique(s2))
  U2 <- setdiff(unique(s2), unique(s1))
  f_val <- 0
  if (length(C) > 0) {
    for (a in C) {
      s1_a  <- which(s1 == a); s2_a <- which(s2 == a)
      K12_a <- min(length(s1_a), length(s2_a))
      f_val <- f_val + sum(abs(s1_a[1:K12_a] - s2_a[1:K12_a]))
    }
    f_val <- f_val / max(L1, L2)
  }
  g_val <- sum(s1 %in% U1) + sum(s2 %in% U2)
  return((f_val + g_val) / (L1 + L2))
}

compute_dissimilarity_matrix <- function(seqs) {
  n <- length(seqs); D <- matrix(0, n, n)
  for (i in 1:(n - 1))
    for (ip in (i + 1):n)
      D[i, ip] <- D[ip, i] <- oss_dissimilarity(seqs[[i]], seqs[[ip]])
  return(D)
}

D <- compute_dissimilarity_matrix(seq_list)

# -- Step 3. MDS Features -------------------------------------------------------
K       <- 100
mds_fit <- cmdscale(as.dist(D), k = K)
pca_fit <- prcomp(mds_fit, center = TRUE, scale. = FALSE)

pca_feature <- as.data.frame(pca_fit$x[, 1:K]) %>%
  mutate(SEQID = sequences$SEQID) %>%
  relocate(SEQID)

# -- Step 4. Outcome and Group Variables ----------------------------------------
score <- ps1_score_data %>%
  mutate(
    SEQID     = paste0("US_", SEQID),
    correct   = as.integer(U01a000S == 3),
    age_group = as.integer(suppressWarnings(as.numeric(AGEG5LFS)) >= 8),
    theta_pv  = PVPSL1
  ) %>%
  filter(!is.na(correct), !is.na(age_group), !is.na(PVPSL1)) %>%
  select(SEQID, correct, age_group, theta_pv)

model_df <- pca_feature %>%
  inner_join(score, by = "SEQID") %>%
  mutate(score_stratum = ntile(theta_pv, 10))

pc_cols   <- paste0("PC", 1:K)
y         <- model_df$correct
group     <- model_df$age_group
X         <- as.matrix(model_df[, pc_cols])
theta_hat <- scale(model_df$theta_pv)[, 1]

cat(sprintf("N = %d | Younger (ref): %d | Older (focal): %d\n",
            nrow(model_df), sum(group == 0), sum(group == 1)))

# -- Step 5. DIF Detection: Mantel-Haenszel Test --------------------------------
tbl     <- with(model_df, table(Group = group, Response = correct, Score = score_stratum))
mh_test <- mantelhaen.test(tbl)
cat("\nMH Test (U01a): OR =", round(unname(mh_test$estimate), 3), "| p =", mh_test$p.value, "\n")

# -- Step 6. L2 Distance Function -----------------------------------------------
compute_L2 <- function(y, theta, group, X_sel = NULL) {
  df_r <- data.frame(y = y[group==0], theta = theta[group==0])
  df_f <- data.frame(y = y[group==1], theta = theta[group==1])
  newdat <- data.frame(theta = theta)
  if (!is.null(X_sel)) {
    xn <- paste0("X", seq_len(ncol(X_sel))); colnames(X_sel) <- xn
    df_r <- cbind(df_r, X_sel[group==0,,drop=FALSE])
    df_f <- cbind(df_f, X_sel[group==1,,drop=FALSE])
    newdat <- cbind(newdat, X_sel)
    fmla <- as.formula(paste("y ~ theta +", paste(xn, collapse="+")))
  } else { fmla <- y ~ theta }
  fit_r <- glm(fmla, data=df_r, family=binomial, control=list(maxit=200))
  fit_f <- glm(fmla, data=df_f, family=binomial, control=list(maxit=200))
  sum((predict(fit_f, newdat, type="response") - predict(fit_r, newdat, type="response"))^2)
}

# -- Step 7. Forward Stepwise Selection (Qi 2023) -------------------------------
selected <- integer(0)
d_prev   <- compute_L2(y, theta_hat, group)
trace_df <- data.frame(n_features=0, L2_empirical=d_prev, L2_threshold=qchisq(0.95, df=1))

for (step in seq_len(min(K, 25))) {
  remaining <- setdiff(seq_len(K), selected)
  best_feat <- NULL; best_d <- Inf
  for (k in remaining) {
    d_cand <- tryCatch(
      compute_L2(y, theta_hat, group, X[, c(selected,k), drop=FALSE]),
      error = function(e) Inf)
    if (d_cand < best_d) { best_d <- d_cand; best_feat <- k }
  }
  if (is.null(best_feat) || best_d >= d_prev) {
    for (s in step:25)
      trace_df <- rbind(trace_df, 
                        data.frame(n_features=s, L2_empirical=d_prev, L2_threshold=qchisq(0.95, df=s)))
    break
  }
  selected <- c(selected, best_feat); d_prev <- best_d
  trace_df <- rbind(trace_df, data.frame(n_features=length(selected),
                                         L2_empirical=best_d, L2_threshold=qchisq(0.95, df=length(selected))))
  p_val <- pchisq(best_d, df=length(selected), lower.tail=FALSE)
  cat(sprintf("Step %d: PC%d | L2=%.4f | p=%.4f\n", step, best_feat, best_d, p_val))
  if (p_val >= 0.05) { cat("-> DIF non-significant. Stop.\n"); break }
}
cat(sprintf("Selected: PC%s\n", paste(selected, collapse=", PC")))

# these labels reflect results from the U.S. sample as reported      
step_labels <- data.frame(
  n_features = c(1, 2, 3),
  label      = c("PC1\np=0.0001", "PC2\np=0.0383", "PC6\np=0.1741"),
  L2_empirical = trace_df$L2_empirical[trace_df$n_features %in% c(1,2,3)]
)

p_l2 <- trace_df %>%
  pivot_longer(c("L2_empirical","L2_threshold"), names_to="Type", values_to="Value") %>%
  mutate(Type = ifelse(Type=="L2_empirical", "Empirical L2", "L2 Threshold")) %>%
  ggplot(aes(x=n_features, y=Value, color=Type, linetype=Type)) +
  geom_line(linewidth=0.8) +
  geom_point(data=step_labels, aes(x=n_features, y=L2_empirical),
             color="#E69F00", size=3, inherit.aes=FALSE) +
  geom_text(data=step_labels, aes(x=n_features, y=L2_empirical, label=label),
            vjust=-0.8, size=3, color="gray30", inherit.aes=FALSE) +
  scale_color_manual(values=c("Empirical L2"="#E69F00", "L2 Threshold"="#56B4E9")) +
  labs(x="Features Dimension", y=expression(L^2~"Distance"),
       color="Type", linetype="Type") +
  theme_bw()
print(p_l2)

# -- Step 8. DIF-corrected Theta ------------------------------------------------
eta_hat         <- prcomp(X[, selected, drop=FALSE], scale.=TRUE)$x[, 1]
theta_corrected <- as.numeric(scale(residuals(lm(theta_hat ~ eta_hat))))

# Identify respondents with largest and smallest corrections
drag_use <- ps1_data %>%
  group_by(SEQID) %>%
  summarise(
    used_dragdrop = as.integer(any(grepl("mail_drag|mail_drop", action_event))),
    n_dragdrop    = sum(grepl("mail_drag|mail_drop", action_event)),
    .groups = "drop"
  )

diff_df <- data.frame(
  SEQID           = model_df$SEQID,
  theta_irt       = theta_hat,
  theta_corrected = theta_corrected,
  age_group       = factor(group, labels = c("Younger", "Older")),
  diff            = theta_corrected - theta_hat
) %>% left_join(drag_use, by = "SEQID")

top10    <- diff_df %>% slice_max(diff, n=10) %>% mutate(highlight="Largest increase")
bottom10 <- diff_df %>% slice_min(diff, n=10) %>% mutate(highlight="Largest decrease")
highlight_df <- bind_rows(top10, bottom10)

cat("\n--- Top 10 (largest increase after correction) ---\n")
top10 %>% select(SEQID, age_group, used_dragdrop, diff) %>% print()
cat("\n--- Bottom 10 (largest decrease after correction) ---\n")
bottom10 %>% select(SEQID, age_group, used_dragdrop, diff) %>% print()

p_theta <- diff_df %>%
  ggplot(aes(x=theta_irt, y=theta_corrected)) +
  geom_point(alpha=0.3, size=0.7, color="gray30") +
  geom_point(data=highlight_df,
             aes(shape=highlight, color=highlight), size=3) +
  geom_abline(linetype="dashed", color="gray40") +
  scale_shape_manual(values=c("Largest increase"=2, "Largest decrease"=0)) +
  scale_color_manual(values=c("Largest increase"="#56B4E9", "Largest decrease"="#E69F00")) +
  labs(x="IRT-based estimate", y="DIF-corrected estimate",
       shape=NULL, color=NULL) +
  theme_bw() + theme(legend.position="bottom")
print(p_theta)

# -- Step 9. Interpretation: Nuisance Trait vs. Drag-and-Drop ------------------
eta_tilde <- residuals(lm(eta_hat ~ theta_corrected))

interp_df <- data.frame(
  SEQID     = model_df$SEQID,
  eta_tilde = eta_tilde,
  age_group = factor(group, labels = c("Younger", "Older")),
  correct   = y
) %>% left_join(drag_use, by = "SEQID")

cat("\nCorr(eta_tilde, used_dragdrop):", round(cor(interp_df$eta_tilde, interp_df$used_dragdrop), 3))
cat("\nCorr(eta_tilde, n_dragdrop):", round(cor(interp_df$eta_tilde, interp_df$n_dragdrop), 3))

interp_df %>%
  filter(!is.na(used_dragdrop)) %>%
  group_by(used_dragdrop) %>%
  summarise(accuracy=mean(correct, na.rm=TRUE), n=n(), .groups="drop")

mean_age <- interp_df %>%
  group_by(age_group) %>%
  summarise(m=mean(eta_tilde), .groups="drop")

p_age <- ggplot(interp_df, aes(x=eta_tilde, fill=age_group, color=age_group)) +
  geom_density(alpha=0.4) +
  geom_vline(data=mean_age, aes(xintercept=m, color=age_group),
             linetype="dashed", linewidth=0.6) +
  labs(x="Nuisance Ability", y="Density", fill="Group", color="Group") +
  theme_bw()
print(p_age)

drag_df   <- interp_df %>%
  filter(!is.na(used_dragdrop)) %>%
  mutate(strategy=ifelse(used_dragdrop==1, "Drag/Drop", "Others"))
mean_drag <- drag_df %>%
  group_by(strategy) %>%
  summarise(m=mean(eta_tilde), .groups="drop")

p_drag <- ggplot(drag_df, aes(x=eta_tilde, fill=strategy, color=strategy)) +
  geom_density(alpha=0.4) +
  geom_vline(data=mean_drag, aes(xintercept=m, color=strategy),
             linetype="dashed", linewidth=0.6) +
  labs(x="Nuisance Ability", y="Density", fill="Strategy", color="Strategy") +
  theme_bw()
print(p_drag)

