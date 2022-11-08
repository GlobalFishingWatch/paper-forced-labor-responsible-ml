import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mpcolors
from matplotlib.colorbar import ColorbarBase


data = pd.read_csv("./outputs/hyper_data.csv")
levels = 21
min_val = 0.840
max_val = 0.860
lvl_vals = np.linspace(min_val, max_val, levels, endpoint=True)
cmap = plt.get_cmap('inferno')
dpi = 300


fig, ((ax0, ax1), (ax2, ax3)) = plt.subplots(2, 2, figsize=(8, 8), facecolor="white")

norm = mpcolors.BoundaryNorm(lvl_vals, ncolors=cmap.N, clip=True)


def plot(data, ax, x_name, y_name, other_name, other_value, x_lbl, y_lbl, loc):
    df = data[data[other_name] == other_value]
    X = sorted(set(df[x_name]))
    Y = sorted(set(df[y_name]))
    Z = df.sort_values([y_name, x_name]).mean_performance.values
    Z = np.clip(Z, min_val, max_val)
    Z = Z.reshape((len(Y), len(X)))
    fcnt = ax.contourf(X, Y, Z, levels=lvl_vals, alpha=0.5, cmap=cmap, norm=norm)
    cnt = ax.contour(X, Y, Z, levels=lvl_vals, cmap=cmap, norm=norm)
    ax.set_xlabel(x_lbl)
    ax.set_ylabel(y_lbl)
    ax.plot([loc[0]], [loc[1]], "wo", markersize=10)
    return fcnt, cnt


plot(
    data,
    ax0,
    "regularization.factor",
    "mtry",
    "min_n",
    15,
    x_lbl="regularization",
    y_lbl="mtry",
    loc=(0.5, 1.0),
)

plot(
    data, 
    ax1,
    "min_n",
    "mtry",
    "regularization.factor",
    0.5,
    x_lbl="min_n",
    y_lbl="mtry",
    loc=(15, 1.0),
)

plot(
    data,
    ax2,
    "regularization.factor",
    "min_n",
    "mtry",
    1.0,
    x_lbl="regularization",
    y_lbl="min_n",
    loc=(0.5, 15),
)

cax = ax3.inset_axes([-0.05, 0.05, 0.03, 0.9])

ColorbarBase(
    cax,
    cmap=cmap,
    norm=norm,
    orientation="vertical",
    ticks=[0.84, 0.85, 0.86],
    label="ROC AUC",
)

ax3.axis("off")

plt.savefig("./outputs/hyper_sensitivity.png", dpi=dpi)
plt.savefig("./outputs/hyper_sensitivity.svg", dpi=dpi)
