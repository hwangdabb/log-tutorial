# Process Data Analysis Tutorial — R Code

Companion code for the paper:

> **A Tutorial on Process Data Analysis: Methods and Applications Using PIAAC**

This repository provides R implementations of six process data analysis methods, applied to the PIAAC (Programme for the International Assessment of Adult Competencies) log-file data.

---

## Data

The analyses use the **PIAAC 2012 U.S. log-file data**, publicly available from the [OECD PIAAC Data Portal](https://www.oecd.org/skills/piaac/data/).

Place the following files in your data directory before running the scripts:

| File | Description |
|------|-------------|
| `prgusap1.csv` | PIAAC background and score data (U.S. sample) |
| `preprocessing/merged_ps1_1.csv` | Preprocessed log-file data for item PS1 |
| `preprocessing/ps1_data2.csv` | Action-recoded data — **output of `S6_HMM_PREPROCESSING.R`** (required by `S6_HMM.R` and `S6_SIP.R`) |

---

## File Structure

| File | Section | Method |
|------|---------|--------|
| `S4_indicator.R` | §5.1 | Descriptive Process Indicators (ToT, TFA, NoA) |
| `S5_ngram.R` | §5.2 | N-gram Analysis with TF-ISF weighting and K-means clustering |
| `S5_MDS.R` | §5.3 | Multidimensional Scaling (OSS dissimilarity + PCA rotation) |
| `S6_HMM_PREPROCESSING.R` | §6.1 | Action recoding — **run this first** before `S6_HMM.R` and `S6_SIP.R` |
| `S6_HMM.R` | §6.1 | Hidden Markov Models + GGM network analysis |
| `S6_SIP.R` | §6.2 | Subtask Identification Procedure (GRU-based entropy segmentation) |
| `S6_DIF.R` | §6.3 | DIF Analysis with process features (MDS + forward stepwise selection) |

---

## Required R Packages

```r
install.packages(c(
  "dplyr", "tidyr", "stringr", "ggplot2",      # core data wrangling and visualization
  "glmnet", "pls",                              # S5_MDS: regularized regression, PLS
  "depmixS4", "qgraph", "bootnet",              # S6_HMM: HMM fitting, network analysis
  "ggExtra", "patchwork", "gridExtra",          # S6_SIP: plot utilities
  "tidyverse"                                   # S6_HMM_PREPROCESSING, S6_HMM
))
```

> **Note for `S6_SIP.R`:** The `torch` package requires a separate installation step.
> See the [torch for R installation guide](https://torch.mlverse.org/docs/articles/installation.html).

---

## Usage

1. **Set paths** — At the top of each script, update `path` and `plot_dir` to your local directories:
   ```r
   path     <- "path/to/your/data/"      # data directory
   plot_dir <- "path/to/your/figures/"   # figure output directory
   ```

2. **Run order** — If you plan to use HMM or SIP, run preprocessing first:
   ```
   S6_HMM_PREPROCESSING.R   →   S6_HMM.R
                             →   S6_SIP.R
   ```
   All other scripts (`S4_indicator.R`, `S5_ngram.R`, `S5_MDS.R`, `S6_DIF.R`) can be run independently.

---

## Citation

If you use this code, please cite the paper:

> [citation to be added upon publication]
