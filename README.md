
# forced-labor-trustability

This repository is a companion to the manuscript “Towards a responsible
machine learning approach to identify forced labor at sea”, from Rocío
Joo et al.

## `original` branch

-   It contains the
    [codes](https://github.com/GlobalFishingWatch/forced-labor-trustability/tree/original/scripts)
    to do the analyses of the paper.

-   There is no data available in this branch, so the codes cannot be
    run by external users.

-   First step: [Run
    queries](https://github.com/GlobalFishingWatch/prj-forced-labor/tree/model-paper/scripts/01_queries_premodel.r)
    to match tables of vessel information and compute movement patterns.

-   Second step: [Process](https://github.com/GlobalFishingWatch/prj-forced-labor/tree/model-paper/scripts/02_format_data.r)
    the data to be in the right format for the model.
    
-   Third, fourth and fifth steps: 
    [bag](https://github.com/GlobalFishingWatch/prj-forced-labor/tree/model-paper/scripts/03_sensitivity_bags.r),
    [random forest hyperparameter](https://github.com/GlobalFishingWatch/prj-forced-labor/tree/model-paper/scripts/04_sensitivity_hyper.r),
    and [seed](https://github.com/GlobalFishingWatch/prj-forced-labor/tree/model-paper/scripts/05_sensitivity_seeds.r) 
    sensitivity analyses to fix the values of those parameters.

-   Sixth: With those optimal values, 
    [run](https://github.com/GlobalFishingWatch/prj-forced-labor/tree/model-paper/scripts/model_run_non_repro.r)
    the model, do predictions, compute performance and fairness

-   Need to add ports and other calculations

The reproducible codes (with data) used for the model and outputs are in
branch `main`.
