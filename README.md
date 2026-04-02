# Process Data Analysis Tutorial — R Code

Companion code for the paper:

> **A Tutorial on Process Data Analysis: Methods and Applications Using PIAAC**

This repository provides R implementations of six process data analysis methods, applied to the PIAAC (Programme for the International Assessment of Adult Competencies) log-file data.

---

## Data

The analyses use the **PIAAC 2012 U.S. log-file data**, publicly available from the [OECD PIAAC Data Portal](https://www.oecd.org/skills/piaac/data/).

Place the following two files in your data directory:

| File | Description |
|------|-------------|
| `ps1_usa.csv` | Preprocessed log-file data for item PS1 (U.S. sample) |
| `prgusap1.csv` | PIAAC background and score data (U.S. sample) |

---

## File Structure

| File | Section | Method |
|------|---------|--------|
| `preprocessing.R` | — | Action recoding: collapses `action_event` into 15 categories (`merged_event`). Run once before `S6_HMM.R` or `S6_SIP.R`. |
| `S5_indicator.R` | §5.1 | Descriptive Process Indicators (ToT, TFA, NoA) |
| `S5_ngram.R` | §5.2 | N-gram Analysis with TF-ISF weighting and K-means clustering |
| `S5_MDS.R` | §5.3 | Multidimensional Scaling (OSS dissimilarity + PCA rotation) |
| `S5_DIF.R` | §5.4 | DIF Analysis with process features (MDS + forward stepwise selection) |
| `S6_HMM.R` | §6.1 | Hidden Markov Models + GGM network analysis |
| `S6_SIP.R` | §6.2 | Subtask Identification Procedure (GRU-based entropy segmentation) |

---

## Required R Packages

```r
install.packages(c(
  "dplyr", "tidyr", "stringr", "ggplot2",      # core data wrangling and visualization
  "glmnet", "pls",                              # S5_MDS: regularized regression, PLS
  "depmixS4", "qgraph", "bootnet",              # S6_HMM: HMM fitting, network analysis
  "ggExtra", "patchwork", "gridExtra",          # S6_SIP: plot utilities
  "tidyverse"                                   # S6_HMM, S6_SIP
))
```

> **Note for `S6_SIP.R`:** The `torch` package requires a separate installation step.
> See the [torch for R installation guide](https://torch.mlverse.org/docs/articles/installation.html).

---

## Usage

1. Set `path` at the top of each script to your local data directory:
   ```r
   path <- "path/to/your/data/"
   ```

2. If using `S6_HMM.R` or `S6_SIP.R`, run `preprocessing.R` first. This appends the `merged_event` column to `ps1_usa.csv` and only needs to be done once.

---

## Citation

If you use this code, please cite the paper:

> [citation to be added upon publication]
