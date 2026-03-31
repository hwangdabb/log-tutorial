# Process Data Analysis Tutorial — R Code

Companion code for the paper:

> **A Tutorial on Process Data Analysis: Methods and Applications Using PIAAC**

This repository provides R implementations of six process data analysis methods, applied to the PIAAC (Programme for the International Assessment of Adult Competencies) log-file data.

---

## Data

The analyses use the **PIAAC 2012 U.S. log-file data**, publicly available from the [OECD PIAAC Data Portal](https://www.oecd.org/skills/piaac/data/).

Place the following two files in your data directory before running any script:

| File | Description |
|------|-------------|
| `ps1_usa.csv` | Preprocessed log-file data for item PS1 (U.S. sample) |
| `prgusap1.csv` | PIAAC background and score data (U.S. sample) |

> **Note:** `S6_HMM.R` and `S6_SIP.R` apply an additional action recoding step inline before analysis. No separate preprocessing script is required.

---

## File Structure

| File | Section | Method |
|------|---------|--------|
| `S4_indicator.R` | §5.1 | Descriptive Process Indicators (ToT, TFA, NoA) |
| `S5_ngram.R` | §5.2 | N-gram Analysis with TF-ISF weighting and K-means clustering |
| `S5_MDS.R` | §5.3 | Multidimensional Scaling (OSS dissimilarity + PCA rotation) |
| `S6_HMM.R` | §6.1 | Hidden Markov Models + GGM network analysis |
| `S6_SIP.R` | §6.2 | Subtask Identification Procedure (GRU-based entropy segmentation) |
| `S6_DIF.R` | §6.3 | DIF Analysis with process features (MDS + forward stepwise selection) |

All scripts can be run independently.

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

Set `path` at the top of each script to your local data directory:

```r
path <- "path/to/your/data/"   # directory containing ps1_usa.csv, prgusap1.csv
```

---

## Citation

If you use this code, please cite the paper:

> [citation to be added upon publication]
