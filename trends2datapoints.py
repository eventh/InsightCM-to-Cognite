#!/usr/bin/env python
# coding: utf-8
"""
A module for extracting timeseries datapoints from InsightCM trend data export,
for storing it in Cognite Data Platform (CDP).

Trend data should be in zip files that contain MetaData.json, Assets.json,
and chartdata.xlsx.

The module will unzip it to a temp folder, extract asset and metadata from
json files. It will get timestamps and values from excel file. Next it will
check if asset exists, and if timeseries exists. If no timeseries exists it
will save metadata. Finally it posts all datapoints to CDP.
"""
import argparse
import glob
import json
import logging
import os
import os.path
import sys
import zipfile
from collections import namedtuple
from tempfile import TemporaryDirectory

import pandas
from cognite import CogniteClient
from cognite.client.stable.datapoints import Datapoint
from cognite.client.stable.time_series import TimeSeries

# import xlrd  # To have pandas parse excel file


TimeData = namedtuple("TimeData", ["name", "trendId", "assetId", "assetName", "metadata"])
logger = logging.getLogger(__name__)


def parse_cli_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Tool for extracting datapoints from InsightCM trend files to CDP")
    parser.add_argument("--path", "-p", type=str, required=True, help="Required, path to folder or zip file of trends")
    parser.add_argument("--apikey", "-k", type=str, required=False, help="Optional, CDP API KEY")
    parser.add_argument(
        "--save-files",
        "-s",
        required=False,
        default=False,
        action="store_true",
        help="Optional, content of zip files can be deleted or saved after processing",
    )
    return parser.parse_args()


def process_timeseries_metadata(path_to_assets, path_to_metadata):
    """Find asset and timeseries metadata from json files."""
    if not path_to_assets or not path_to_metadata:
        logger.error("Missing paths to assets or metadata {} {}".format(path_to_assets, path_to_metadata))
        return

    # Find trend id to map to right timeseries and asset
    with open(path_to_metadata) as fp:
        meta = json.load(fp)
    trend_id = None
    if meta and "Instructions" in meta[0] and meta[0]["Instructions"]:
        trend_id = meta[0]["Instructions"][0].get("Props", {}).get("TrendPointId")
    if not trend_id:
        logger.error("Can't find TrendPointId in {} {}".format(path_to_metadata, meta))
        return

    # Find asset and timeseries with metadata
    with open(path_to_assets) as fp:
        assets = json.load(fp)
    if not assets or "FullName" not in assets[0] or "Id" not in assets[0]:
        logger.error("Can't find require info in {} {}".format(path_to_assets, assets))
        return
    asset_id = assets[0]["Id"]
    asset_name = assets[0]["FullName"]
    properties = assets[0].get("Properties", {})

    if "Metrics" in assets[0] and assets[0]["Metrics"]:
        trends = [i for i in assets[0]["Metrics"] if i.get("Id") == trend_id]
        if not trends:
            logger.error("Unknown timeseries/trend {} {} in {}".format(trend_id, assets[0], path_to_metadata))
            return
        properties.update(trends[0])

    name = asset_name  # Timeseries name in CDP
    if properties.get("Name"):
        name += " {}".format(properties["Name"])
    if properties.get("Unit"):
        name += " ({})".format(properties["Unit"])
    name = name.replace(" ", "_")

    return TimeData(name, trend_id, asset_id, asset_name, properties)


def process_datapoints_excel_file(path):
    """Extract timestamp and values from 'path'."""
    with open(path, "rb") as fp:
        data = pandas.read_excel(fp, skiprows=[0, 1])

    points = []
    for i, row in data.iterrows():
        panda_ts, value_str = row[0], row[1]
        try:
            value = float(value_str)
        except ValueError as exc:
            logger.warn("Failed to convert float {} {}".format(value_str, exc))
            continue
        timestamp = panda_ts.value // 10 ** 6  # From nano to microseconds
        points.append((timestamp, value))

    if not points:
        logger.error("No valid datapoints found in file {}".format(path))
    return points


def find_cdp_asset(client, asset_uid, asset_name):
    """Search for CDP asset with UID that matches 'asset_uid'."""
    res = client.assets.get_assets(metadata={"UID": asset_uid}, autopaging=True)
    match = [i for i in res if i.to_json().get("metadata", {}).get("UID") == asset_uid]
    if match:
        id_ = match[0].to_json()["id"]
        logger.debug("Asset {} has id {} in CDP".format(asset_name, id_))
        return id_
    else:
        logger.warn("Asset {} not found in CDP".format(asset_name))


def find_cdp_timeseries(client, name):
    """Check if timeseries with the given 'name' already exists in CDP."""
    res = client.time_series.get_time_series(prefix=name, include_metadata=True, autopaging=True)
    match = [i for i in res if i.to_json().get("name") == name]
    if match:
        found = match[0].to_json()
        logger.debug("Timeseries {} has id {} in CDP".format(name, found["id"]))
        return found
    else:
        logger.info("Timeseries {} does not exists in CDP".format(name))


def update_cdp_timeseries(client, name, metadata, asset_name=None, asset_id=None):
    """Insert timeseries metadata into CDP."""
    vargs = {"is_string": False}
    if asset_id:
        vargs["asset_id"] = asset_id
    if "Unit" in metadata and metadata["Unit"]:
        vargs["unit"] = metadata["Unit"]
    if "Type" in metadata:
        vargs["description"] = metadata["Type"]
    if asset_name:
        metadata["assetName"] = asset_name

    series = TimeSeries(name=name, metadata=metadata, **vargs)
    client.time_series.post_time_series([series])


def insert_cdp_datapoints(client, name, datapoints, limit=1000):
    """Store datapoints in CDP, 'limit' amount in each call."""
    if len(datapoints) > limit:
        insert_cdp_datapoints(client, name, datapoints[limit:])
    objs = [Datapoint(timestamp=ts, value=value) for ts, value in datapoints[:limit]]
    logger.debug("Sending {} to CDP {}".format(len(datapoints), name))
    client.datapoints.post_datapoints(name, objs)


def process_inputs(input_path, save_files=False):
    """Unzip file, and processed documents inside zip."""

    def match_path(paths, filename):
        """Case in-sensitive finding filename in list of paths."""
        for path in paths:
            if os.path.basename(path).lower() == filename.lower():
                return path

    def process(output_path):
        with zipfile.ZipFile(input_path) as zp:
            zp.extractall(output_path)
            files = [os.path.join(output_path, i) for i in zp.namelist()]

        timeseries = process_timeseries_metadata(match_path(files, "assets.json"), match_path(files, "metadata.json"))
        datapoints = process_datapoints_excel_file(match_path(files, "chartdata.xlsx"))
        return timeseries, datapoints

    base_path, prefix = os.path.split(input_path)
    prefix = os.path.splitext(prefix)[0]
    if save_files:
        real_path = os.path.join(base_path, prefix)
        if not os.path.exists(real_path):
            os.makedirs(real_path)
        return process(real_path)
    else:
        with TemporaryDirectory(prefix=prefix, dir=base_path) as tmp_path:
            return process(tmp_path)


def process_datapoints(client, timeseries, datapoints, path):
    """Send datapoints to CDP, but create timeseries metadata first if needed."""
    cdp_asset_id = find_cdp_asset(client, timeseries.assetId, timeseries.assetName)

    if not find_cdp_timeseries(client, timeseries.name):
        update_cdp_timeseries(client, timeseries.name, timeseries.metadata, timeseries.assetName, cdp_asset_id)

    insert_cdp_datapoints(client, timeseries.name, datapoints)
    logger.info("Finished processing {} datapoints from {}".format(len(datapoints), path))


def main(args):
    logging.basicConfig(level=logging.INFO)

    if os.path.isdir(args.path):
        files = glob.glob(os.path.join(args.path, "*.zip"))
    elif os.path.exists(args.path) and os.path.splitext(args.path)[1] == ".zip":
        files = [args.path]
    else:
        logger.fatal("--path must point to either folder or trend zip file: {}".format(args.path))
        sys.exit(2)

    client = CogniteClient(api_key=args.apikey if args.apikey else os.environ.get("COGNITE_API_KEY"))

    for path in files:
        timeseries, datapoints = process_inputs(path, save_files=args.save_files)
        if timeseries and datapoints:
            process_datapoints(client, timeseries, datapoints, path)


if __name__ == "__main__":
    main(parse_cli_args())
