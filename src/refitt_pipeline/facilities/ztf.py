from concurrent.futures import ThreadPoolExecutor, as_completed

import antares_client
import pandas as pd
from antares_client.search import get_by_ztf_object_id
from nested_pandas import NestedFrame
from tqdm import tqdm

pd.set_option("future.no_silent_downcasting", True)


def format_antares_lc(locus: antares_client.search.Locus) -> pd.DataFrame:
    """
    Format the lightcurve of a locus object from the ANTARES database.

    Parameters
    ----------
    locus : antares_client.search.Locus
        The locus object from the ANTARES database.

    Returns
    -------
    lightcurve : pd.DataFrame
        The formatted lightcurve.
    """

    # Select relevant columns
    lc = locus.lightcurve[
        ["ant_mjd", "ant_survey", "ant_passband", "ant_mag", "ant_magerr", "ant_maglim"]
    ].copy()

    # Fill NaN values in 'ant_mag' with 'ant_maglim'
    lc["ant_mag"] = lc["ant_mag"].fillna(lc["ant_maglim"])

    # Drop the 'ant_maglim' column
    lc = lc.drop(columns=["ant_maglim"])

    # Rename columns
    lc = lc.rename(
        columns={
            "ant_mjd": "mjd",
            "ant_survey": "non_detection",
            "ant_passband": "band",
            "ant_mag": "magnitude",
            "ant_magerr": "magnitude_error",
        }
    )

    # Replace values in 'non_detection' and 'band' columns
    lc["non_detection"] = lc["non_detection"].replace([1, 2], [False, True])
    lc["band"] = lc["band"].replace(
        ["g", "R", "r", "G", "i", "I"], ["ztfg", "ztfr", "ztfr", "ztfg", "ztfi", "ztfi"]
    )

    # Set index
    lc.index = [locus.properties["ztf_object_id"]] * len(lc)

    return lc


def format_antares_meta(locus: antares_client.search.Locus):
    """
    Format the metadata of a locus object from the ANTARES database.

    Parameters
    ----------
    locus : antares_client.search.Locus
        The locus object from the ANTARES database.

    Returns
    -------
    meta : pd.DataFrame
        The formatted metadata.
    """
    meta = pd.DataFrame(
        [
            {
                key: locus.properties.get(key, None)
                for key in [
                    "ztf_object_id",
                    "num_mag_values",
                    "num_alerts",
                    "brightest_alert_magnitude",
                    "brightest_alert_observation_time",
                    "newest_alert_magnitude",
                    "newest_alert_observation_time",
                    "oldest_alert_magnitude",
                    "oldest_alert_observation_time",
                    "survey",
                ]
            }
        ]
    )

    meta["ra"] = locus.ra
    meta["dec"] = locus.dec

    meta.index = [locus.properties["ztf_object_id"]] * len(meta)

    return meta


def query_ztf_lightcurve(ztf_object_id: str = None):
    """
    Query the lightcurve of a ZTF object from the ANTARES database.

    Parameters
    ----------
    ztf_object_id : str
        The ZTF object ID.

    Returns
    -------
    lightcurve : pd.DataFrame
        The formatted lightcurve.
    meta : pd.DataFrame
        The formatted metadata.
    """

    locus = get_by_ztf_object_id(ztf_object_id)

    lightcurve, meta = format_antares_lc(locus), format_antares_meta(locus)

    return lightcurve, meta


def lcs_to_nested_df(lightcurves, meta):
    """
    Convert lightcurves and metadata to a nested dataframe.

    Parameters
    ----------
    lightcurves : pd.DataFrame
        The lightcurves.
    meta : pd.DataFrame
        The metadata.

    Returns
    -------
    nested_df : NestedFrame
        The nested dataframe.
    """

    nested_df = NestedFrame(meta)

    nested_df = nested_df.add_nested(lightcurves, "lightcurve")

    return nested_df


# def query_ztf_lightcurves(ztf_object_ids: list, save_path: str=None, save_by_layer: bool=True):
#     lightcurves, metas = [], []

#     for ztf_object_id in tqdm(ztf_object_ids):

#         try:
#             lightcurve, meta = query_ztf_lightcurve(ztf_object_id)
#             lightcurves.append(lightcurve)
#             metas.append(meta)
#         except Exception:
#             continue
#     lightcurves = pd.concat(lightcurves)
#     metas = pd.concat(metas)

#     nested_df = lcs_to_nested_df(lightcurves, metas).reset_index(drop=True)

#     if save_path is not None:

#         nested_df.to_parquet(save_path, by_layer=save_by_layer)

#     return nested_df


def query_ztf_lightcurves(
    ztf_object_ids: list, save_path: str = None, save_by_layer: bool = True, max_workers: int = 4
):
    """
    Query the lightcurves of a list of ZTF objects from the ANTARES database.

    Parameters
    ----------
    ztf_object_ids : list
        The list of ZTF object IDs.
    save_path : str
        The path to save the nested dataframe.
    save_by_layer : bool
        Whether to save the nested dataframe by layer.
    max_workers : int
        The maximum number of workers to use for multi-processing.

    Returns
    -------
    nested_df : NestedFrame
        The nested dataframe.
    """

    lightcurves, metas = [], []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_id = {
            executor.submit(query_ztf_lightcurve, ztf_object_id): ztf_object_id
            for ztf_object_id in ztf_object_ids
        }
        for future in tqdm(as_completed(future_to_id), total=len(future_to_id)):
            try:
                lightcurve, meta = future.result()
                lightcurves.append(lightcurve)
                metas.append(meta)
            except Exception:
                # print(f"Error processing {ztf_object_id}: {e}")
                continue
    lightcurves = pd.concat(lightcurves)
    metas = pd.concat(metas)
    nested_df = lcs_to_nested_df(lightcurves, metas).reset_index(drop=True)
    if save_path is not None:
        nested_df.to_parquet(save_path, by_layer=save_by_layer)
    return nested_df
