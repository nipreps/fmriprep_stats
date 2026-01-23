# emacs: -*- mode: python; py-indent-offset: 4; indent-tabs-mode: nil -*-
"""Plotting utilities extracted from the analysis notebook."""

import datetime
import calendar
from packaging import version
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import colors as mpl_colors
from scipy.interpolate import RBFInterpolator

plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = 'Inter Tight'

# -----------------------------------------------------------------------------
# Helper utilities copied from the notebook
# -----------------------------------------------------------------------------

def _parse(vstr):
    vstr = str(vstr)
    vstr = vstr[int(vstr.startswith("v")) :]
    try:
        return version.parse(vstr)
    except Exception:
        return version.parse("0.0")


_vparse = np.vectorize(_parse)


# -----------------------------------------------------------------------------
# Plotting functions
# -----------------------------------------------------------------------------

def plot_performance(
    unique_started: pd.DataFrame,
    unique_success: pd.DataFrame,
    drop_cutoff: str | None = None,
    out_file: str = "weekly.png",
):
    """Generate the performance plot.

    Parameters
    ----------
    unique_started : pandas.DataFrame
        Records of started runs.
    unique_success : pandas.DataFrame
        Records of successful runs.
    drop_cutoff : str, optional
        Ignore versions older than this tag.
    out_file : str, optional
        Path to save the resulting figure.
    """

    if drop_cutoff:
        cutoff = version.parse(drop_cutoff)
        unique_started = unique_started[_vparse(unique_started.environment_version.values) > cutoff]
        unique_success = unique_success[_vparse(unique_success.environment_version.values) > cutoff]

    grouped_started = unique_started.groupby(
        [
            unique_started["date_minus_time"].dt.isocalendar().year,
            unique_started["date_minus_time"].dt.isocalendar().week,
        ]
    )["id"].count()

    grouped_success = unique_success.groupby(
        [
            unique_success["date_minus_time"].dt.isocalendar().year,
            unique_success["date_minus_time"].dt.isocalendar().week,
        ]
    )["id"].count()

    unique_started_success = unique_success.loc[
        unique_success["run_uuid"].isin(unique_started["run_uuid"])
    ]

    grouped_started_success = unique_started_success.groupby(
        [
            unique_started_success["date_minus_time"].dt.isocalendar().year,
            unique_started_success["date_minus_time"].dt.isocalendar().week,
        ]
    )["id"].count()

    indexes = grouped_success.index[1:-1]
    year_index = indexes.droplevel("week")
    years = sorted(year_index.unique())
    weeks_per_year = [(year_index == yr).sum() for yr in years]

    success_data = grouped_success[indexes]
    started_data = grouped_started[indexes]

    abs_success_mean = success_data.values.mean()
    success_ratio = 100 * success_data / started_data
    success_mean = success_ratio.values.mean()
    max_success = (np.argmax(success_ratio), success_ratio.values.max())
    max_date = indexes[max_success[0]]
    min_success = (np.argmin(success_ratio), success_ratio.values.min())
    min_date = indexes[min_success[0]]

    plt.clf()
    fig, axes = plt.subplots(
        nrows=1,
        ncols=len(years),
        sharey=True,
        gridspec_kw={"width_ratios": weeks_per_year, "wspace": 0.01, "hspace": 0.0},
        figsize=(14, 8),
    )

    xlength = [(year_index == yr).sum() for yr in years]
    yticks = [4000, 8000, 12000, 16000]

    axes[0].set_yticks(yticks, labels=yticks)
    axes[0].yaxis.set_tick_params(length=25, labelsize=16)
    axes[0].spines["left"].set_position(("outward", 30))
    axes[0].spines["left"].set_color("dimgray")
    axes[0].set_ylabel("runs/week", color="dimgrey", fontsize=14, rotation="horizontal")
    axes[0].yaxis.set_label_coords(-0.1, 1.02)
    axes[0].yaxis.set_label_position("left")

    axes_twins = []
    for ax_i, yr in enumerate(years):
        x = np.arange(len(started_data[year_index == yr]), dtype=float) + 0.5

        bar1 = axes[ax_i].bar(
            x,
            started_data[year_index == yr].values,
            width=0.7,
            label="Started",
            color="lightgrey",
        )
        bar2 = axes[ax_i].bar(
            x,
            success_data[year_index == yr].values,
            width=0.7,
            label="Successful",
            color="dimgrey",
        )

        axes[ax_i].spines["right"].set_visible(False)
        axes[ax_i].spines["top"].set_visible(False)
        axes[ax_i].spines["left"].set_visible(False)
        axes[ax_i].spines["bottom"].set_position(("outward", 45))

        axes[ax_i].set_xlim((0.0, xlength[ax_i]))
        axes[ax_i].set_xticks((0.0, xlength[ax_i]))
        axes[ax_i].set_xticklabels([])
        axes[ax_i].tick_params(direction="in", length=8)
        axes[ax_i].set_xlabel(f"{yr}", fontsize=18)

        axes[ax_i].yaxis.set_tick_params(length=0)
        if ax_i > 0:
            axes[ax_i].set_yticks([])

        ax2 = axes[ax_i].twinx()
        axes_twins.append(ax2)
        lineplot = ax2.plot(
            x,
            success_ratio[year_index == yr].values,
            "o-",
            label="Success (%)",
            color="slategray",
            mfc="white",
            zorder=4,
        )
        ax2.set_ylim(0, 100)
        ax2.set_yticks([])

        ax2.spines["right"].set_visible(False)
        ax2.spines["top"].set_visible(False)
        ax2.spines["left"].set_visible(False)
        ax2.spines["bottom"].set_visible(False)

        months = [
            datetime.datetime.strptime(f"{yr}-W{week}-1", "%Y-W%W-%w").month
            for _, week in indexes[year_index == yr]
        ]
        for mnum in sorted(set(months)):
            month_x = 0.5 * (xlength[ax_i] - months[::-1].index(mnum) + months.index(mnum))
            axes[ax_i].text(
                month_x,
                -1200 - 1200 * ((mnum + 1) % 2),
                f"{calendar.month_abbr[mnum]}",
                fontsize=16,
                ha="center",
            )

        axes[ax_i].set_zorder(10)
        ax2.set_zorder(11)

    for i in np.arange(4, 33, step=4, dtype=int):
        axes[0].annotate(
            f"{i},000",
            xy=(1, i * 1000),
            xytext=(-2, i * 1000),
            xycoords="data",
            annotation_clip=False,
            color="dimgrey",
            size=14,
            horizontalalignment="right",
            verticalalignment="center",
            arrowprops={"arrowstyle": "-", "color": "lightgrey"},
        ).set_zorder(0)

    axes[-1].annotate(
        "fMRIPrep averaged" f" {int(round(abs_success_mean, -2))}"
        "\nweekly successful runs,\n"
        f" out of {int(round(grouped_started.values[1:-1].mean(), -2))} runs/week.",
        xy=(0 - sum(xlength[:-1]), abs_success_mean),
        xytext=(xlength[-1] + 1, abs_success_mean),
        xycoords="data",
        annotation_clip=False,
        color="dimgrey",
        size=16,
        horizontalalignment="left",
        verticalalignment="center",
        arrowprops={"arrowstyle": "-", "color": "dimgrey"},
        zorder=0,
    ).set_zorder(0)

    axes_twins[-1].annotate(
        "Averaged weekly success\n" f"rate was {round(success_mean,1)}±{round(success_ratio.values.std(),0)}%.",
        xy=(0 - sum(xlength[:-1]), success_mean),
        xytext=(xlength[-1] + 1, success_mean),
        xycoords="data",
        annotation_clip=False,
        color="slategray",
        size=16,
        horizontalalignment="left",
        verticalalignment="center",
        arrowprops={"arrowstyle": "-", "color": "slategray", "linestyle": "--", "shrinkA": 0, "shrinkB": 0},
    ).set_zorder(0)

    y_idx = years.index(max_date[0])
    axes_twins[y_idx].annotate(
        f"On week {max_date[1]} of {max_date[0]}," "\nthe success rate\npeaked at " f"{round(max_success[1], 1)}%.",
        xy=(max_date[1] - 0.5, round(max_success[1], 1)),
        xytext=(12, round(max_success[1], 1)),
        xycoords="data",
        annotation_clip=False,
        color="slategray",
        size=16,
        arrowprops={"arrowstyle": "-", "color": "slategray"},
        horizontalalignment="left",
        verticalalignment="center",
    )

    y_idx = years.index(min_date[0])
    axes_twins[y_idx].annotate(
        f"On week {min_date[1]} of {min_date[0]}," "\nthe lowest success rate \n" f"({round(min_success[1],1)}%) was recorded.",
        xy=(min_date[1] - 0.5, round(min_success[1], 1)),
        xytext=(xlength[-1] + 1, round(max_success[1], 1)),
        xycoords="data",
        annotation_clip=False,
        color="slategray",
        size=16,
        arrowprops={"arrowstyle": "-", "color": "slategray", "connectionstyle": "angle,angleA=0,angleB=90"},
        horizontalalignment="left",
        verticalalignment="center",
    )

    patches = [bar1, bar2, lineplot[0]]
    fig.legend(
        patches,
        [p.get_label() for p in patches],
        loc="lower right",
        bbox_to_anchor=(1.1, 0.05),
        frameon=False,
        mode=None,
        prop={"size": 16},
    )

    plt.savefig(out_file, dpi=300, bbox_inches="tight", facecolor="w", edgecolor="w")


def plot_version_stream(
    unique_started: pd.DataFrame,
    unique_success: pd.DataFrame,
    drop_cutoff: str | None = None,
    out_file: str = "versionstream.png",
):
    """Generate the version stream plot."""
    def _label_color(color):
        rgb = np.array(mpl_colors.to_rgb(color))
        luminance = np.dot(rgb, [0.2126, 0.7152, 0.0722])
        return "w" if luminance < 0.5 else "k"

    def _label_text(label):
        return "20.2 (LTS)" if label in {"20.2", "v20.2"} else label

    if drop_cutoff:
        cutoff = version.parse(drop_cutoff)
        unique_started = unique_started[_vparse(unique_started.environment_version.values) > cutoff]
        unique_success = unique_success[_vparse(unique_success.environment_version.values) > cutoff]

    unique_started_success = unique_success.loc[
        unique_success["run_uuid"].isin(unique_started["run_uuid"])
    ]

    indexes = unique_started_success.groupby(
        [
            unique_started_success["date_minus_time"].dt.isocalendar().year,
            unique_started_success["date_minus_time"].dt.isocalendar().week,
        ]
    )["id"].count().index
    year_index = indexes.droplevel("week")
    years = sorted(year_index.unique())
    weeks_per_year = [(year_index == yr).sum() for yr in years]

    versions = sorted(unique_started.environment_version.unique())
    versions_success = {}
    versions_started = {}
    for ver in versions:
        ver_suc = unique_started_success[unique_started_success.environment_version == ver]
        ver_sta = unique_started[unique_started.environment_version == ver]
        versions_success[ver] = ver_suc.groupby(
            [
                ver_suc["date_minus_time"].dt.isocalendar().year,
                ver_suc["date_minus_time"].dt.isocalendar().week,
            ]
        )["id"].count()
        versions_started[ver] = ver_sta.groupby(
            [
                ver_sta["date_minus_time"].dt.isocalendar().year,
                ver_sta["date_minus_time"].dt.isocalendar().week,
            ]
        )["id"].count()

    versions_success = pd.DataFrame(versions_success)
    versions_started = pd.DataFrame(versions_started)

    versions_success = versions_success.loc[:, versions_success.sum(0) > 5000]

    data = versions_success[1:-1].fillna(0.0)
    xs = np.arange(len(data))
    xnew = np.linspace(0.0, len(data), num=14 * len(data))
    smoothed_data = RBFInterpolator(xs[:, np.newaxis], data.values)(xnew[:, np.newaxis])

    fig, axes = plt.subplots(
        nrows=1,
        ncols=len(years),
        sharey="row",
        gridspec_kw={"width_ratios": weeks_per_year, "wspace": 0.01, "hspace": 0.0},
        figsize=(20, 4),
    )

    labels = versions_success.columns
    colors = [plt.cm.YlGnBu_r(i / (len(labels))) for i, _ in enumerate(labels)]

    xlims = []
    year_start_index = 0
    for ax_i, yr in enumerate(years):
        if ax_i > 0:
            axes[ax_i].set_yticklabels([])
            axes[ax_i].set_yticks([])

        axes[ax_i].stackplot(
            xnew,
            smoothed_data.T,
            baseline="sym",
            labels=labels,
            colors=colors,
        )

        axes[ax_i].spines["right"].set_visible(False)
        axes[ax_i].spines["top"].set_visible(False)
        axes[ax_i].spines["left"].set_visible(False)

        year_end_index = year_start_index + weeks_per_year[ax_i]
        xlims.append((year_start_index, year_end_index))

        axes[ax_i].set_xlim(xlims[-1])
        axes[ax_i].set_xticklabels([])
        axes[ax_i].set_xticks(xlims[-1])
        axes[ax_i].tick_params(direction="in", length=8)
        axes[ax_i].set_xlabel(f"{yr}", fontsize=18)
        year_start_index = year_end_index

    smoothed_totals = smoothed_data.sum(axis=1)
    baseline_shift = -0.5 * smoothed_totals
    y_top = 0.5 * smoothed_totals.max()
    starts = {}
    for idx, label in enumerate(labels):
        series = data[label].to_numpy()
        nonzero = np.flatnonzero(series > 0)
        starts[label] = int(nonzero[0]) if nonzero.size else 0

    left_labels = [label for label in labels if starts[label] <= 1]
    later_labels = [label for label in labels if label not in left_labels]

    def _axis_for_x(xpos):
        for ax_i, (x_start, x_end) in enumerate(xlims):
            if x_start <= xpos <= x_end:
                return axes[ax_i]
        return axes[-1]

    def _center_y(label, xpos):
        x_idx = np.searchsorted(xnew, xpos)
        x_idx = min(max(x_idx, 0), len(xnew) - 1)
        if x_idx > 0 and abs(xnew[x_idx] - xpos) > abs(xnew[x_idx - 1] - xpos):
            x_idx -= 1
        values = smoothed_data[x_idx]
        cum = np.cumsum(values)
        label_idx = list(labels).index(label)
        center = baseline_shift[x_idx] + cum[label_idx] - values[label_idx] / 2.0
        return center

    left_positions = []
    for label in left_labels:
        y = _center_y(label, 0)
        left_positions.append((label, y))
    left_positions.sort(key=lambda item: item[1])
    min_gap = 800
    adjusted_left = []
    for label, y in left_positions:
        if adjusted_left and y - adjusted_left[-1][1] < min_gap:
            y = adjusted_left[-1][1] + min_gap
        adjusted_left.append((label, y))

    for label, y in adjusted_left:
        color = colors[list(labels).index(label)]
        axes[0].annotate(
            _label_text(label),
            xy=(0, y),
            xytext=(-2, y),
            textcoords="data",
            xycoords="data",
            fontweight=800,
            annotation_clip=False,
            color=_label_color(color),
            size=14,
            horizontalalignment="right",
            verticalalignment="center",
            bbox={
                "boxstyle": "round",
                "fc": color,
                "ec": color,
                "color": "w",
                "pad": 0.5,
            },
            arrowprops={
                "arrowstyle": "wedge,tail_width=.5",
                "color": color,
                "patchA": None,
                "patchB": None,
                "relpos": (0.8, 0.5),
            },
        )

    stagger = [0.88, 0.8, 0.72]
    for idx, label in enumerate(sorted(later_labels, key=lambda lab: starts[lab])):
        xpos = starts[label]
        color = colors[list(labels).index(label)]
        y = y_top * stagger[idx % len(stagger)]
        axis = _axis_for_x(xpos)
        axis.annotate(
            _label_text(label),
            xy=(xpos, _center_y(label, xpos)),
            xytext=(xpos + 1.5, y),
            textcoords="data",
            xycoords="data",
            fontweight=800,
            annotation_clip=False,
            color=_label_color(color),
            size=14,
            horizontalalignment="left",
            verticalalignment="center",
            bbox={
                "boxstyle": "round",
                "fc": color,
                "ec": color,
                "color": "w",
                "pad": 0.5,
            },
            arrowprops={
                "arrowstyle": "wedge,tail_width=.5",
                "color": color,
                "patchA": None,
                "patchB": None,
                "relpos": (0.3, 0.3),
            },
        )

    plt.savefig(out_file, dpi=300, bbox_inches="tight")
