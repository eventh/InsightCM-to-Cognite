#!/usr/bin/env python
# coding: utf-8
"""
A module for extracting sequence data from TDMS files.
"""
import argparse
import glob
import logging
import os
import os.path
import sys
from datetime import date

from cognite import CogniteClient
from cognite.client.experimental.sequences import Column, Row, RowValue, Sequence
from cognite.client.stable.datapoints import Datapoint
from cognite.client.stable.time_series import TimeSeries

from nptdms import TdmsFile

logger = logging.getLogger()


def parse_cli_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Tool for extracting sequences from NI TDMS files to CDP")
    parser.add_argument("--path", "-p", type=str, required=True, help="Required, path to folder or TDMS file")
    parser.add_argument("--apikey", "-k", type=str, required=False, help="Optional, CDP API KEY")
    parser.add_argument(
        "--only-static",
        "-s",
        required=False,
        default=False,
        action="store_true",
        help="Optional, only process static values, not waveforms",
    )
    return parser.parse_args()


def find_cdp_asset_id(client, metadata):
    """Map sequence to CDP asset by external asset UID."""
    asset_uid = metadata.get("NI_CM_AssetNodeId")
    asset_name = metadata.get("NI_CM_AssetName")
    if not asset_uid:
        logger.warning("Sequence does not have asset name: {}".format(metadata))
        return

    res = client.assets.get_assets(metadata={"UID": asset_uid}, autopaging=True)
    match = [i for i in res if i.to_json().get("metadata", {}).get("UID") == asset_uid]
    if match:
        id_ = match[0].to_json()["id"]
        logger.debug("Asset {} has id {} in CDP".format(asset_name, id_))
        return id_
    else:
        logger.warning("Asset {} not found in CDP".format(asset_name))


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
    if "unit_string" in metadata and metadata["unit_string"]:
        vargs["unit"] = metadata["unit_string"]
    if "NI_CM_Reason" in metadata:
        vargs["description"] = metadata["NI_CM_Reason"]
    if asset_name:
        metadata["assetName"] = asset_name
    metadata.update({k: str(v) for k, v in metadata.items() if isinstance(v, date)})
    client.time_series.post_time_series([TimeSeries(name=name, metadata=metadata, **vargs)])


def process_static_data(client, metadata, path):
    """TDMS can hold a single value for a static group, we send it as datapoints to CDP."""
    timestamp = int(metadata["DateTime"].timestamp() * 1000)
    try:
        value = float(metadata["Value"])
    except ValueError as exc:
        logger.warning("{} Failed to convert float {} {}".format(path, metadata["Value"], exc))
        return

    asset_name = metadata.get("NI_CM_AssetName")
    if not asset_name:
        logger.error("{} Tried to create timeseries for static value without asset name".format(path))
        return

    name = asset_name.replace(" ", "_")

    if not find_cdp_timeseries(client, name):
        asset_id = find_cdp_asset_id(client, metadata)
        update_cdp_timeseries(client, name, metadata, asset_name, asset_id)

    client.datapoints.post_datapoints(name, [Datapoint(timestamp=timestamp, value=value)])


def create_sequence(client, channel, metadata):
    """Create the CDP sequence, with columns and metadata etc."""
    name = "{}-{}".format(metadata["NI_CM_AssetName"], metadata["name"]).replace(" ", "_")
    asset_id = find_cdp_asset_id(client, metadata)
    time_type = str(channel.time_track(True, "ns").dtype)
    data_type = str(channel.data.dtype)
    metadata.update({k: str(v) for k, v in metadata.items() if isinstance(v, date)})

    columns = [Column(name="time", value_type=time_type), Column(name="value", value_type=data_type)]
    return Sequence(name=name, asset_id=asset_id, columns=columns, description=channel.path, metadata=metadata)


def create_seq_rows(channel, columns):
    """Create the rows, each has timestamp and a value."""
    rows = []
    iterator = channel.as_dataframe(True).iterrows()
    next(iterator)  # Skip header
    for i, row in enumerate(iterator):
        rows.append(Row(i, [RowValue(columns[0].id, str(row[0])), RowValue(columns[1].id, float(row[1].values[0]))]))
    return rows


def process_tdms_file(client, tdms, path, only_static=False):
    """Handle all groups of sequence data and static data, and post to CDP."""
    properties = tdms.object().properties
    channels = [c for group in tdms.groups() for c in tdms.group_channels(group)]

    for channel in channels:
        metadata = dict(properties)
        metadata.update(channel.properties)
        metadata["Group"] = channel.group
        metadata["Channel"] = channel.channel

        if not channel.has_data or not channel.data.any():
            if channel.property("Value") and metadata.get("DateTime"):  # Static value channels
                process_static_data(client, metadata, path)
            else:
                logger.warning("{}: {} channel has no data {}".format(path, channel.path, metadata))
            continue

        if only_static:
            continue

        sequence = create_sequence(client, channel, metadata)

        res = client.experimental.sequences.post_sequences([sequence])
        if res:
            rows = create_seq_rows(channel, res.columns)
            client.experimental.sequences.post_data_to_sequence(res.id, rows)
            logger.info("Sent {} rows to sequence {}".format(len(rows), res.name))
        else:
            logger.error("{} Failed to create sequence {}".format(path, sequence.name))


def main(args):
    logging.basicConfig(level=logging.INFO)

    if os.path.isdir(args.path):
        files = glob.glob(os.path.join(args.path, "*.tdms"))
    elif os.path.exists(args.path) and os.path.splitext(args.path)[1] == ".tdms":
        files = [args.path]
    else:
        logger.fatal("--path must point to either folder or tdms file: {}".format(args.path))
        sys.exit(2)

    client = CogniteClient(api_key=args.apikey if args.apikey else os.environ.get("COGNITE_API_KEY"))

    for path in files:
        with open(path, "rb") as fp:
            try:
                tdms = TdmsFile(fp)
            except Exception as exc:
                logger.error("Fatal: failed to parse TDMS file {}: {}".format(path, exc))
                continue
            else:
                process_tdms_file(client, tdms, path, args.only_static)


if __name__ == "__main__":
    main(parse_cli_args())
