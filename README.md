
# forced-labor-trustability

This repository is a companion to the manuscript “Towards a responsible
machine learning approach to identify forced labor at sea”, from Rocío
Joo et al.

## Reproducibility

For reproducibility purposes, we created a
[script](https://github.com/GlobalFishingWatch/forced-labor-trustability/tree/main/scripts/model_repro_training_data.R)
with all the reproducible analyses of the paper. Due to confidentiality
agreements, the negative cases used for validation cannot be shared, so
we modified the code to run it without them. Since there are differences
in how Mac and Linux/Windows handle random seeds when using the `ranger`
package. For that reason, we have copied our results in both Mac and
Linux for user comparison in the script. The data (with anonymized
vessels) to run the script is
[here](https://github.com/GlobalFishingWatch/forced-labor-trustability/tree/main/data/training_repro.csv).

## Original codes for data processing and analysis

-   These cannot be run without access to Global Fishing Watch tables in
    Big Query.
-   First step: [Run
    queries](https://github.com/GlobalFishingWatch/forced-labor-trustability/blob/main/scripts/01_queries_premodel.r)
    to match tables of vessel information and compute movement patterns.
-   Second step:
    [Process](https://github.com/GlobalFishingWatch/forced-labor-trustability/blob/main/scripts/02_format_data.r)
    the data to be in the right format for the model.
-   Third, fourth and fifth steps: run sensitivity analyses for the
    [number of
    bags](https://github.com/GlobalFishingWatch/forced-labor-trustability/blob/main/scripts/03_sensitivity_bags.r),
    the [hyperparameter
    values](https://github.com/GlobalFishingWatch/forced-labor-trustability/blob/main/scripts/04_sensitivity_hyper.r)
    of the random forests, and the number of [initial random
    seeds](https://github.com/GlobalFishingWatch/forced-labor-trustability/blob/main/scripts/05_sensitivity_seeds.r).
-   Sixth: With those optimal values,
    [run](https://github.com/GlobalFishingWatch/forced-labor-trustability/blob/main/scripts/06_model_run_non_repro.r)
    the model, do predictions, compute performance and fairness
-   Seventh: Run an additional [analysis of the
    ports](https://github.com/GlobalFishingWatch/forced-labor-trustability/blob/original/scripts/07_port_analysis.r)
    used by those predicted as positives.
