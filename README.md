
# forced-labor-trustability

This repository is a companion to the manuscript “Towards a responsible
machine learning approach to identify forced labor at sea”, from Rocío
Joo et al.

## `main` branch

-   It contains the
    [codes](https://github.com/GlobalFishingWatch/forced-labor-trustability/tree/main/scripts/model_repro_training_data.R)
    to reproduce the analyses of the paper.
-   It also contains a [data
    set](https://github.com/GlobalFishingWatch/forced-labor-trustability/tree/main/data/training_repro.csv)
    with anonymized vessels.

We removed confidential and sensitive information from the data set: \*
Negative cases have been removed, so specificity cannot be computed. \*
Sensitive variables such as flag and vessel ID were removed, so flag
barplots and the port analysis in the discussion cannot be replicated.

The complete codes (without data) used for the model and outputs are in
branch `original`.