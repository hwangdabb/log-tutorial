# Process Data Analysis Tutorial

R and Python preprocessing code and R implementations of six analytical methods, companion to the paper:

> **A Tutorial on Process Data Analysis: Methods and Applications Using PIAAC**

This repository provides R implementations of six process data analysis methods, applied to the PIAAC (Programme for the International Assessment of Adult Competencies) log-file data.

---

## Data

The analyses use the **PIAAC 2012 U.S. log-file data**. Place the following two files in your data directory:


| File | Description |
|------|-------------|
| `ps1_usa.csv` | Preprocessed log-file data for item PS1 (U.S. sample) |
| `prgusap1.csv` | PIAAC background and score data (U.S. sample) |

**How to obtain the data:**
- `ps1_usa.csv`: The raw PIAAC log files are available via the [GESIS Data Archive](https://search.gesis.org/) (study code: ZA6712) upon registration. After downloading the U.S. ZIP file, use the PIAAC LogDataAnalyzer (LDA), available from the OECD 
PIAAC Log File Website, to export the raw log data, then apply the preprocessing steps described in Section 3 of the paper to obtain this file. For a detailed description of the LDA workflow, see Goldhammer et al. (2020, Chapter 10).
- `prgusap1.csv`: Available from the [OECD PIAAC Data Portal](https://www.oecd.org/en/data/datasets/piaac-1st-cycle-database.html#data) as part of the PIAAC Public Use File (U.S. sample).

---

## File Structure

### Preprocessing (`Data_Preprocess/`)

| File | Description |
|------|-------------|
| `Data_Preprocess/R_Preprocess.R` | Reference R preprocessing implementation |
| `Data_Preprocess/Python_Preprocess.py` | Reference Python preprocessing implementation |

**Prompts (`Data_Preprocess/prompts/`)**

| File | Description |
|------|-------------|
| `prompts/system_prompt.md` | System prompt used for all models |
| `prompts/prompt_r.md` | User prompt for R code generation |
| `prompts/prompt_py.md` | User prompt for Python code generation |

| File | Model | Language |
|------|-------|----------|
| `scripts/gpt-5.4.R` | GPT-5.4 | R |
| `scripts/gpt-5.4.py` | GPT-5.4 | Python |
| `scripts/claude-sonnet-4.6.R` | Claude Sonnet 4.6 | R |
| `scripts/claude-sonnet-4.6.py` | Claude Sonnet 4.6 | Python |
| `scripts/gemini-3.1-flash-lite-preview.R` | Gemini 3.1 Flash-Lite | R |
| `scripts/gemini-3.1-flash-lite-preview.py` | Gemini 3.1 Flash-Lite | Python |
| `scripts/deepseek-v3.2.R` | DeepSeek v3.2 | R |
| `scripts/deepseek-v3.2.py` | DeepSeek v3.2 | Python |
| `scripts/qwen3.5-27b.R` | Qwen 3.5-27b | R |
| `scripts/qwen3.5-27b.py` | Qwen 3.5-27b | Python |

### Analytical Methods ('/')
| File | Section | Method |
|------|---------|--------|
| `S6_Preprocess.R` | — | Action recoding: collapses `action_event` into 15 categories (`merged_event`). Run once before `S6_HMM.R` or `S6_SIP.R`. |
| `S5_indicator.R` | §5.1 | Descriptive Process Indicators (ToT, TFA, NoA) |
| `S5_ngram.R` | §5.2 | N-gram Analysis with TF-ISF weighting and K-means clustering |
| `S5_MDS.R` | §5.3 | Multidimensional Scaling (OSS dissimilarity + PCA rotation) |
| `S5_DIF.R` | §5.4 | DIF Analysis with process features (MDS + forward stepwise selection) |
| `S6_HMM.R` | §6.1 | Hidden Markov Models + GGM network analysis |
| `S6_SIP.R` | §6.2 | Subtask Identification Procedure (GRU-based entropy segmentation) |

---

## Requirements

### R Packages
```r
install.packages(c(
  "dplyr", "tidyr", "stringr", "ggplot2",       # core data wrangling and visualization
  "smacof",                                     # S5_MDS: MDS fitting
  "glmnet", "pls",                              # S5_MDS: regularized regression, PLS
  "depmixS4", "qgraph", "bootnet",              # S6_HMM: HMM fitting, network analysis
  "ggExtra", "patchwork", "gridExtra",          # S6_SIP: plot utilities
  "tidyverse"                                   # S6_HMM, S6_SIP
))
```

> **Note for `S6_SIP.R`:** The `torch` package requires a separate installation step.
> See the [torch for R installation guide](https://torch.mlverse.org/docs/articles/installation.html).

### Python Packages (Preprocessing only)
```bash
pip install pandas numpy
```
---

## Usage

1. Set `path` at the top of each script to your local data directory:
   ```r
   path <- "path/to/your/data/"
   ```

2. If using `S6_HMM.R` or `S6_SIP.R`, run `S6_Preprocess.R` first. This appends the `merged_event` column to `ps1_usa.csv` and only needs to be done once.

---

## Citation

If you use this code, please cite:

> Hwangbo, D., Park, J., Jeon, M., & Jin, I. H. (2026). 
> A tutorial on process data analysis: Methods and applications 
> using PIAAC. *Journal of the Korean Statistical Society*. 
> Submitted.
