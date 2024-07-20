import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import rcParams

from .utils import color_dict, marker_dict

rcParams["font.family"] = "serif"
rcParams["mathtext.default"] = "regular"
plt.rcParams.update({"font.size": 16})
plt.rcParams["axes.linewidth"] = 1.50

rcParams["xtick.major.size"] = 10  # Major tick size for x-axis
rcParams["xtick.minor.size"] = 5  # Minor tick size for x-axis
rcParams["ytick.major.size"] = 10  # Major tick size for y-axis
rcParams["ytick.minor.size"] = 5  # Minor tick size for y-axis


def plot_light_curve(
    lightcurve: pd.DataFrame,
    ztf_id: str = None,
    ax=None,
    save_path: str = None,
    time_axis: str = "mjd",
    **kwargs,
):
    """
    Plot the light curve of a locus object from the ANTARES database.

    Parameters
    ----------
    lightcurve : pd.DataFrame
        The formatted lightcurve.
    ztf_id : str
        The ZTF ID.
    ax : matplotlib.axes._subplots.AxesSubplot
        The axis to plot on.
    save_path : str
        The path to save the plot.
    time_axis : str
        The time axis to use. Options are "mjd" or "relative"
    kwargs : dict
        Additional keyword arguments to pass to `ax.plot

    Returns
    -------
    ax : matplotlib.axes._subplots.AxesSubplot
        The axis with the light curve
    """

    if ax is None:
        fig, ax = plt.subplots(1, 1, figsize=(10, 8))

    if time_axis != "mjd":
        lightcurve["mjd"] = lightcurve["mjd"] - lightcurve[lightcurve["non_detection"] == False]["mjd"].min()

    for band in lightcurve["band"].unique():
        band_data = lightcurve[lightcurve["band"] == band]

        detections = band_data[band_data["non_detection"] == False]
        non_detections = band_data[band_data["non_detection"] == True]

        ax.errorbar(
            detections["mjd"],
            detections["magnitude"],
            detections["magnitude_error"],
            fmt=marker_dict[band],
            label=band,
            **kwargs,
            color=color_dict[band],
            markersize=8,
            mec="black",
        )
        ax.scatter(
            non_detections["mjd"],
            non_detections["magnitude"],
            marker="v",
            color=color_dict[band],
            s=50,
            edgecolors="black",
        )

    ax.minorticks_on()
    ax.tick_params(which="both", direction="in", top=True, right=True)

    ax.set_xlabel("MJD")
    ax.set_ylabel("Apparent Magnitude")
    ax.invert_yaxis()
    ax.legend(frameon=False)
    ax.set_title(ztf_id)
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path + ztf_id + ".png", dpi=300)
        plt.close()

        return "none"
    else:
        return ax
