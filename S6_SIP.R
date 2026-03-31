rm(list = ls())

# ============================================================
# Subtask Identification Procedure (SIP)
# Following Wang et al. (2023)
# Note: run S6_HMM_PREPROCESSING.R first to generate ps1_data2.csv
# ============================================================

# ── User Configuration ────────────────────────────────────────────
path     <- "path/to/your/data/"      # directory containing ps1_data2.csv, prgusap1.csv
plot_dir <- "path/to/your/figures/"   # directory to save output figures
# ─────────────────────────────────────────────────────────────────

library(torch)
library(tidyverse)
library(ggExtra)
library(patchwork)
library(gridExtra)

# ── Step 0. Load Data ──────────────────────────────────────────────
ps1_data       <- read.csv(paste0(path, "preprocessing/ps1_data2.csv"), header = TRUE, row.names = 1)
ps1_score_data <- read.csv(paste0(path, "prgusap1.csv"), row.names = 1)

# ── Step 1. Outcome Variable ───────────────────────────────────────
# Dichotomize U01a: score == 3 is correct
score <- ps1_score_data %>%
  mutate(SEQID   = paste0("US_", SEQID),
         score   = as.integer(U01a000S),
         correct = as.integer(score == 3)) %>%
  filter(!is.na(score)) %>%
  select(SEQID, score, correct)

# ── Step 2. Prepare Sequences ──────────────────────────────────────
item_data <- ps1_data %>%
  left_join(score, by = "SEQID") %>%
  filter(!is.na(correct))

actions    <- sort(unique(item_data$action_recoded))
M          <- length(actions)
action_idx <- setNames(seq_along(actions), actions)

seqs <- item_data %>%
  group_by(SEQID, correct) %>%
  summarise(seq = list(action_idx[action_recoded]), .groups = "drop") %>%
  mutate(seq_len = map_int(seq, length)) %>%
  filter(seq_len >= 10) # fewer than ten actions are excluded

N <- nrow(seqs)

# Train / validation / test split (70 / 15 / 15)
set.seed(42)
idx       <- sample(N)
n_train   <- floor(0.70 * N)
n_valid   <- floor(0.15 * N)
idx_train <- idx[1:n_train]
idx_valid <- idx[(n_train + 1):(n_train + n_valid)]
idx_test  <- idx[(n_train + n_valid + 1):N]

# ── Step 3. GRU-based Action Prediction Model ─────────────────────
# Compresses action history a_{1:t} into K-dimensional vector theta_t
# via a gated recurrent unit (GRU), then applies a multinomial logistic model
K <- 20

gru_model <- nn_module(
  initialize = function(M, K) {
    self$embed  <- nn_embedding(M + 1L, K, padding_idx = 1L)
    self$gru    <- nn_gru(K, K, batch_first = TRUE)
    self$linear <- nn_linear(K, M)
  },
  forward = function(x) {
    self$linear(self$gru(self$embed(x))[[1]])
  }
)

model     <- gru_model(M, K)
optimizer <- optim_rmsprop(model$parameters, lr = 1e-3)

# Validation-based early stopping
best_val_loss <- Inf; patience <- 5; wait <- 0; best_state <- NULL

for (epoch in seq_len(50)) {
  model$train(); train_loss <- 0
  for (i in idx_train) {
    s <- seqs$seq[[i]]
    if (length(s) < 2) next
    x_in  <- torch_tensor(s[-length(s)], dtype = torch_long())$unsqueeze(1L)
    y_out <- torch_tensor(s[-1],         dtype = torch_long())$unsqueeze(1L)
    optimizer$zero_grad()
    loss <- nnf_cross_entropy(model(x_in)$view(c(-1L, M)), y_out$view(-1L))
    loss$backward(); optimizer$step()
    train_loss <- train_loss + loss$item()
  }
  model$eval(); val_loss <- 0
  with_no_grad({
    for (i in idx_valid) {
      s <- seqs$seq[[i]]
      if (length(s) < 2) next
      x_in  <- torch_tensor(s[-length(s)], dtype = torch_long())$unsqueeze(1L)
      y_out <- torch_tensor(s[-1],         dtype = torch_long())$unsqueeze(1L)
      val_loss <- val_loss +
        nnf_cross_entropy(model(x_in)$view(c(-1L, M)), y_out$view(-1L))$item()
    }
  })
  cat(sprintf("Epoch %2d | train: %.4f | val: %.4f\n",
              epoch, train_loss / length(idx_train), val_loss / length(idx_valid)))
  if (val_loss < best_val_loss) {
    best_val_loss <- val_loss; best_state <- model$state_dict(); wait <- 0
  } else { wait <- wait + 1; if (wait >= patience) { cat("Early stopping.\n"); break } }
}
model$load_state_dict(best_state)

# ── Step 4. Shannon Entropy ────────────────────────────────────────
# H_t = -sum p_{tm} log p_{tm}; low within subtask, high at transitions
compute_entropy <- function(s) {
  if (length(s) < 2) return(numeric(0))
  model$eval()
  with_no_grad({
    logits <- model(torch_tensor(s[-length(s)], dtype = torch_long())$unsqueeze(1L))$view(c(-1L, M))
    pm     <- as.matrix(nnf_softmax(logits, dim = 2L)$to(device = "cpu"))
  })
  apply(pm, 1, function(p) { p <- pmax(p, 1e-10); -sum(p * log(p)) })
}

# ── Step 5. U-curve Segmentation (Algorithm 1, Wang et al. 2023) ──
# A subsequence h_{s:e} is a U-curve if:
#   min{H_s, H_e} - min_{s<=t<=e} H_t >= lambda * (max H - min H)
segment_ucurve <- function(h, lambda = 0.3) {
  T_h <- length(h)
  if (T_h < 2) return(integer(0))
  h_range <- max(h) - min(h)
  if (h_range == 0) return(integer(0))
  threshold <- lambda * h_range
  
  is_lmax <- c(FALSE, sapply(2:(T_h-1), function(i) h[i]>h[i-1] & h[i]>h[i+1]), FALSE)
  D       <- which(is_lmax)
  if (length(D) < 2) return(integer(0))
  
  h_pad     <- c(Inf, h, Inf)
  is_ucurve <- function(s, e)
    min(h_pad[s+1], h_pad[e+1]) - min(h_pad[(s+1):(e+1)]) >= threshold
  
  # Bidirectional filtering
  L <- integer(0); i <- 1
  while (i < length(D)) {
    for (j in (i+1):length(D)) {
      if (is_ucurve(D[i], D[j])) { L <- c(L, D[j]); i <- j; break }
    }
    if (!is_ucurve(D[i], D[length(D)])) break
  }
  R_seg <- integer(0); j <- length(D)
  while (j > 1) {
    for (i in (j-1):1) {
      if (is_ucurve(D[i], D[j])) { R_seg <- c(R_seg, D[i]); j <- i; break }
    }
    if (!is_ucurve(D[1], D[j])) break
  }
  sort(unique(c(L, R_seg)))
}

# ── Step 6. Subtask Clustering ─────────────────────────────────────
# Represent each subprocess as an action frequency profile z_l in [0,1]^M
# Cluster via k-means with Hellinger distance (= k-means on sqrt-transformed profiles)
profiles <- list(); seq_info <- list()

for (i in 1:N) {
  s  <- seqs$seq[[i]]
  if (length(s) < 5) next
  h  <- compute_entropy(s)
  bp <- segment_ucurve(h, lambda = 0.3)
  boundaries <- c(0L, bp, length(s))
  for (l in seq_len(length(boundaries) - 1)) {
    sub_s <- s[(boundaries[l]+1):boundaries[l+1]]
    profiles[[length(profiles)+1]] <- tabulate(sub_s, nbins = M) / length(sub_s)
    seq_info[[length(seq_info)+1]] <- list(SEQID   = seqs$SEQID[i], correct = seqs$correct[i], sub_idx = l)
  }
}

Z  <- do.call(rbind, profiles)
set.seed(42)
Rc <- 3
km <- kmeans(sqrt(Z), centers = Rc, nstart = 25, iter.max = 200)
cluster_labels <- km$cluster

# Inspect cluster centroids (top 3 actions) to assign substantive labels
for (k in seq_len(Rc)) {
  top3 <- actions[order(km$centers[k, ]^2, decreasing = TRUE)[1:3]]
  cat(sprintf("  Cluster %d: %s\n", k, paste(top3, collapse = " | ")))
}

# Assign labels based on inspection
subtask_names <- c("1" = "DRAG_DROP_VIEW", "2" = "VIEW", "3" = "BUTTON_END")
subtask_colors <- c("DRAG_DROP_VIEW" = "#80cbc4", "VIEW" = "#ef9a9a", "BUTTON_END" = "gray")

# ── Reconstruct Subtask Sequences ─────────────────────────────────
info_df <- bind_rows(lapply(seq_along(seq_info), function(idx) {
  si <- seq_info[[idx]]
  data.frame(SEQID = si$SEQID, correct = si$correct,
             sub_idx = si$sub_idx, cluster = cluster_labels[idx])
}))

subtask_seqs <- info_df %>%
  arrange(SEQID, sub_idx) %>%
  group_by(SEQID, correct) %>%
  summarise(subtask_seq = paste(rle(subtask_names[as.character(cluster)])$values,
                                collapse = " -> "),
            .groups = "drop") %>%
  mutate(seq_len = lengths(strsplit(subtask_seq, " -> ")))

# ── Figure 1. Entropy Process and Segmentation ────────────────────
# Select a representative respondent from the test set
get_entropy_plot <- function(sample_idx) {
  s_sample  <- seqs$seq[[sample_idx]]
  h_sample  <- compute_entropy(s_sample)
  bp_sample <- segment_ucurve(h_sample, lambda = 0.3)
  bounds    <- c(0L, bp_sample, length(s_sample))
  
  sub_cluster <- sapply(seq_len(length(bounds)-1), function(l) {
    sub_s <- s_sample[(bounds[l]+1):bounds[l+1]]
    fv    <- tabulate(sub_s, nbins = M) / length(sub_s)
    which.min(apply(km$centers^2, 1, function(c) sum((sqrt(fv)-sqrt(c))^2)))
  })
  
  seg_label_df <- data.frame(
    x     = sapply(seq_len(length(bounds)-1), function(l) mean(c(bounds[l]+1, bounds[l+1]))),
    label = subtask_names[as.character(sub_cluster)]
  )
  
  ggplot(data.frame(t = seq_along(h_sample), entropy = h_sample,
                    action = actions[s_sample[-length(s_sample)]]),
         aes(x = t, y = entropy)) +
    geom_line(linewidth = 0.7) + geom_point(size = 2) +
    geom_text(aes(label = action), vjust = -0.7, size = 2.3) +
    geom_vline(xintercept = bp_sample + 0.5,
               linetype = "dashed", color = "orange", linewidth = 0.7) +
    geom_text(data = seg_label_df,
              aes(x = x, y = min(h_sample) - 0.12, label = label),
              size = 2.8, color = "red", fontface = "bold") +
    scale_x_continuous(breaks = seq_along(h_sample)) +
    labs(x = "Step", y = "Entropy") +
    theme_bw() +
    theme(plot.margin = margin(t = 20, r = 10, b = 40, l = 10))
}

# Edit indices based on inspection; aim for sequences of length 15-25
correct_idx   <- 906
incorrect_idx <- 576

p1_correct   <- get_entropy_plot(correct_idx)
p1_incorrect <- get_entropy_plot(incorrect_idx)
p1_combined <- p1_correct / p1_incorrect

print(p1_combined)


# ── Figure 2. Subtask Sequence Visualization by Response Outcome ──
seq_long <- subtask_seqs %>%
  filter(seq_len <= 7) %>%
  arrange(correct, subtask_seq) %>%
  mutate(rank  = row_number(),
         group = ifelse(correct == 1, "Correct", "Incorrect")) %>%
  rowwise() %>%
  mutate(subtasks = list(strsplit(subtask_seq, " -> ")[[1]])) %>%
  unnest(subtasks) %>%
  group_by(SEQID) %>%
  mutate(position = row_number()) %>%
  ungroup()

p2 <- ggplot(seq_long, aes(x = position, y = rank, fill = subtasks)) +
  geom_tile(linewidth = 0.1, height = 1.2) +
  facet_wrap(~ group, scales = "free_y") +
  scale_fill_manual(values = subtask_colors, name = NULL) +
  scale_x_continuous(breaks = 1:7, name = "Subtasks") +
  labs(y = "Respondents") + theme_bw()

print(p2)

# ── Figure 3. Problem-solving Strategy and Response Time ──────────
# Two strategies identified from subtask sequences:
#   DRAG_DROP: uses drag-and-drop to move emails
#   VIEW_ONLY: views mails/folders without drag-and-drop

rt_df <- item_data %>%
  group_by(SEQID) %>%
  summarise(rt = max(timestamp) - min(timestamp), .groups = "drop") %>%
  mutate(log_rt = log(rt / 1000 + 1))

age_df <- ps1_score_data %>%
  mutate(SEQID = paste0("US_", SEQID)) %>%
  select(SEQID, age = AGEG10LFS)

strategy_full <- subtask_seqs %>%
  mutate(strategy = ifelse(str_detect(subtask_seq, "DRAG_DROP_VIEW"), "DRAG_DROP", "VIEW_ONLY")) %>%
  left_join(rt_df,  by = "SEQID") %>%
  left_join(age_df, by = "SEQID") %>%
  filter(!is.na(age), !is.na(log_rt)) %>%
  mutate(age_jitter = jitter(as.numeric(age), amount = 0.3))

# Summary statistics
strategy_full %>%
  group_by(strategy) %>%
  summarise(n = n(), pct_correct = mean(correct, na.rm = TRUE),
            mean_log_rt = mean(log_rt, na.rm = TRUE), mean_age = mean(age, na.rm = TRUE))

# Joint density plot of age and log response time by strategy
y_lim <- range(strategy_full$log_rt, na.rm = TRUE)

make_density_plot <- function(data, strategy_name, color_low, color_high) {
  p <- data %>% filter(strategy == strategy_name) %>%
    ggplot(aes(x = age_jitter, y = log_rt)) +
    geom_point(alpha = 0) +
    geom_density_2d_filled(alpha = 0.9, contour_var = "ndensity",
                           show.legend = FALSE) +
    scale_fill_manual(values = colorRampPalette(c(color_low, color_high))(12)) +
    scale_y_continuous(limits = y_lim) +
    annotate("text", x = -Inf, y = Inf, label = strategy_name,
             hjust = -0.8, vjust = 3, size = 5) +
    labs(x = "Age group", y = "Log response time") +
    theme_bw() + theme(legend.position = "none")
  ggMarginal(p, type = "density", fill = color_high,
             color = color_high, alpha = 0.4, size = 4)
}

p_drag <- make_density_plot(strategy_full, "DRAG_DROP", "#d6f5f5", "#00868B")
p_view <- make_density_plot(strategy_full, "VIEW_ONLY", "#fde8e8", "#C0392B")

grid.arrange(p_drag, p_view, ncol = 2)


#### save plot
ggsave(paste0(plot_dir, "sip_entropy.png"), p1_combined, width = 12, height = 9, dpi = 300)
ggsave(paste0(plot_dir, "sip_subtask_seq.png"), p2, width = 9, height = 5, dpi = 300)

png(paste0(plot_dir, "sip_density.png"),
    width = 9, height = 5, units = "in", res = 300)
grid.arrange(p_drag, p_view, ncol = 2)
dev.off()


