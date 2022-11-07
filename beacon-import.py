#!/usr/bin/env python3
"""Pushes genomic variant information from a galaxy to a beacon instance.

Usage:
    ./beacon-push.py
"""

import argparse
import asyncio
import datetime
import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List
from argparse import Namespace

# import utilities from beacon-python
# pip install git+https://github.com/CSCfi/beacon-python
#
# TODO can we install it like this
import asyncpg
from beacon_api.utils.db_load import BeaconDB
from requests import Response
from bioblend.galaxy import GalaxyInstance

# cyvcf2 is a vcf parser
from cyvcf2 import VCF, Variant


class BeaconExtendedDB(BeaconDB):
    """
    This class is used to hijack beacons internal database class from the "beacon_api.utils" package
    """

    async def get_variant_indices(self, start: int, ref: str, alt: str) -> List[int]:
        """
        Returns database indices of all occurrences of the given variant

            Parameters:
                start (int): start position of the variant
                ref (str): sequence in the reference
                alt (str): sequence of the variant

            Returns:
                list of matching indices (possibly empty)
        """

        self._conn: asyncpg.Connection
        rows = await self._conn.fetch(
            f"SELECT index FROM beacon_data_table where start={start} AND reference='{ref}' and alternate='{alt}'")
        return [row["index"] for row in rows]

    async def clear_database(self):
        """
        Removes all data from beacons internal database

        The database schema will remain unchanged
        """
        await self._conn.execute("DELETE FROM beacon_data_table")
        await self._conn.execute("DELETE FROM beacon_dataset_table")
        await self._conn.execute("DELETE FROM beacon_dataset_counts_table")


    async def update_dataset_counts(self):
        """
        Calculates and sets actual counts for the accumulated datasets

            Parameters:
                None
            
            Returns:
                Nothing
        """
        records: List[asyncpg.Record] = await self._conn.fetch(
            "SELECT datasetid, COUNT(*) as count, SUM(callcount) as callcount FROM beacon_data_table GROUP BY datasetid")

        # clear beacon_dataset_counts table
        # beacon_init function will add multiple lines for the same dataset, one for each file that is imported
        await self._conn.execute("DELETE FROM beacon_dataset_counts_table")

        # persist the actual count values
        for dataset in records:
            await self._conn.execute(
                f"INSERT INTO beacon_dataset_counts_table(datasetid, callcount, variantcount) " +
                f"VALUES('{dataset['datasetid']}', {dataset['count']}, {dataset['callcount']})")

        # hide the sample count
        await self._conn.execute("UPDATE beacon_dataset_table SET samplecount = NULL")



# global variable for beacon connection
db: BeaconExtendedDB = BeaconExtendedDB()

def parse_arguments() -> Namespace:
    """
    Defines and parses command line arguments for this script

        Parameters:
            None.

        Returns:
            args (Namespace): argparse.Namespace object containing the parsed arguments
    """
    parser = argparse.ArgumentParser(description="Push genomic variants from galaxy to beacon.")
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    # arguments controlling output 
    parser.add_argument("-v", "--verbosity", action="count", default=0,
                        help="log verbosity, can be repeated up to three times")

    # arguments controlling galaxy connection
    parser.add_argument("-u", "--galaxy-url", type=str, metavar="", default="http://localhost:8080", dest="galaxy_url",
                        help="galaxy hostname or IP")
    parser.add_argument("-k", "--galaxy-key", type=str, metavar="", default="6edbc8a89bbff89bb5232867edc1183c",
                        dest="galaxy_key", help="API key of a galaxy user WITH ADMIN PRIVILEGES")

    # sub-parser for command "rebuild"
    parser_rebuild = subparsers.add_parser('rebuild')
    parser_rebuild.add_argument("-s", "--store-origins", default=False, dest="store_origins",
                                action="store_true",
                                help="make a local file containing variantIDs with the dataset they stem from")
    parser_rebuild.add_argument("-o", "--origins-file", type=str, metavar="", default="/tmp/variant-origins.txt",
                                dest="origins_file",
                                help="full file path of where variant origins should be stored (if enabled)")
    
    # database connection
    parser_rebuild.add_argument("-H", "--db-host", type=str, metavar="", default="localhost", dest="database_host",
                                help="hostname/IP of the beacon database")
    parser_rebuild.add_argument("-P", "--db-port", type=str, metavar="", default="5432", dest="database_port",
                                help="port of the beacon database")
    parser_rebuild.add_argument("-U", "--db-user", type=str, metavar="", default="beacon", dest="database_user",
                                help="login user for the beacon database")
    parser_rebuild.add_argument("-W", "--db-password", type=str, metavar="", default="beacon", dest="database_password",
                                help="login password for the beacon database")
    parser_rebuild.add_argument("-N", "--db-name", type=str, metavar="", default="beacondb", dest="database_name",
                                help="name of the beacon database")

    # sub-parser for command search
    parser_search = subparsers.add_parser('search')
    parser_search.add_argument("-s", "--start", type=int, metavar="", dest="start", required=True,
                               help="start position of the searched variant")
    parser_search.add_argument("-r", "--ref", type=str, metavar="", dest="ref", required=True,
                               help="sequence in the reference")
    parser_search.add_argument("-a", "--alt", type=str, metavar="", dest="alt", required=True,
                               help="alternate sequence found in the variant")

    return parser.parse_args()


def set_up_logging(verbosity: int):
    """
    Configures the logger for this script

        Parameters:
            verbosity (int):

        Returns:
            Nothing.
    """

    # configure log level to match the given verbosity
    if verbosity > 1:
        logging.basicConfig(level=logging.DEBUG)
    elif verbosity == 1:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARN)


def set_up_galaxy_instance(galaxy_url: str, galaxy_key: str) -> GalaxyInstance:
    """
    Returns a galaxy instance with the given URL and api key.
    Exits immediately if either connection or authentication to galaxy fails.

        Parameters:
            galaxy_url (str): Base URL of a galaxy instance
            galaxy_key (str): API key with admin privileges

        Returns:
            gi (GalaxyInstance): Galaxy instance with confirmed admin access to the given galaxy instance
    """

    logging.info(f"trying to connect to galaxy at {galaxy_url}")

    # configure a galaxy instance with galaxy_url and api_key
    try:
        gi = GalaxyInstance(galaxy_url, key=galaxy_key)
    except Exception as e:
        # if galaxy_url does not follow the scheme <protocol>://<host>:<port>, GalaxyInstance attempts guessing the URL 
        # this exception is thrown when neither "http://<galaxy_url>:80" nor "https://<galaxy_url>:443" are accessible
        logging.critical(f"failed to guess URL from \"{galaxy_url}\" - {e}")
        exit(2)

    # test network connection and successful authentification
    try:
        response = gi.make_get_request(galaxy_url + "/api/whoami")
        content = json.loads(response.content)

        # this request should not fail
        if response.status_code != 200:
            logging.critical(
                f"connection test failed - got HTTP status \"{response.status_code}\" with message \"{content['err_msg']}\"")
            exit(2)

        logging.info(f"connection successful - logged in as user \"{content['username']}\"")

    except Exception as e:
        # if the network connection fails, GalaxyInstance will throw an exception
        logging.critical(f"exception during connection test - \"{e}\"")
        exit(2)

    # test connection with a GET to /api/whoami

    # TODO do the test
    # resp = gi.make_get_request(galaxy_url+ "/api/whoami")
    # resp = gi.make_get_request(galaxy_url + "/api/configuration?keys=allow_user_deletion").content
    return gi


def get_beacon_histories(gi: GalaxyInstance) -> List[str]:
    """
    Fetches beacon history IDs from galaxy

        Parameters:
            gi (GalaxyInstance): galaxy instance from which to fetch history IDs

        Returns:
            beacon_histories (List[str]): IDs of all histories that should be imported to beacon
    """
    # history ids will be collected in this lsit
    history_ids: List[str] = []

    # get histories from galaxy api
    # URL is used because the name filter is not supported by bioblend as of now
    response: Response = gi.make_get_request(f"{gi.base_url}/api/histories?q=name&qv=Beacon%20Export%20%F0%9F%93%A1&all=true&deleted=false")

    # check if the reuest was successful
    if response.status_code != 200:
        logging.critical("failed to get histories from galaxy")
        logging.critical(f"got status {response.status_code} with content {response.content}")
        exit(2)

    # retrieve histories from response body
    histories: List[Dict] = json.loads(response.content)

    # for each history double check if the user has beacon sharing enabled
    for history in histories:
        history_details: Dict[str, Any] = gi.histories.show_history(history["id"])
        user_details: Dict[str, Any] = gi.users.show_user(history_details["user_id"])

        # skip adding the history if beacon_enabled is not set for the owner account
        history_user_preferences: Dict[str, str] = user_details["preferences"]
        if "beacon_enabled" not in history_user_preferences or history_user_preferences["beacon_enabled"] != "1":
            continue

        history_ids.append(history["id"])

    return history_ids


class MissingFieldException(Exception):
    """
    Exception for a missing field in a dict
    """
    pass


@dataclass
class GalaxyDataset:
    """
    Representation of a galaxy dataset

    Contains attributes that are used in the scope of this script which is only a subset of
    attributes returned by the galaxy api
    """
    name: str
    id: str
    uuid: str
    extension: str
    reference_name: str

    def __init__(self, info: Dict):
        """
        Constructs a GalaxyDataset from a dictionary

        Raises MissingFieldException if an expected key is missing from the given info
        """
        for key in ["name", "id", "uuid", "extension", "metadata_dbkey"]:
            # check for exceptions instead of silently using default
            if not key in info:
                raise MissingFieldException(f"key \"{key}\" not defined")

        self.name = info["name"]
        self.id = info["id"]
        self.uuid = info["uuid"]
        self.extension = info["extension"]
        self.reference_name = info["metadata_dbkey"]


def get_datasets(gi: GalaxyInstance, history_id: str) -> List[GalaxyDataset]:
    """
    Fetches a given histories datasets from galaxy

        Parameters:
            gi (GalaxyInstance): galaxy instance to be used for the request
            history_id (str): (encoded) ID of the galaxy history

        Returns:
            datasets (List[GalaxyDataset]): list of all datasets in the given history
    """

    # datasets = gi.histories.show_matching_datasets(history_id)

    datasets: List[GalaxyDataset] = []

    offset: int = 0
    limit: int = 500

    # galaxy api uses paging for datasets. This while loop continuously retrieves pages of *limit* datasets
    # The loop breaks the first time an empty page comes up
    while True:
        # TODO extensions only allow one entry atm
        # retrieve a list of datasets from the galaxy api
        api_dataset_list = gi.datasets.get_datasets(
            history_id=history_id,
            deleted=False,
            extension=["vcf", "vcf_bgzip"],
            limit=limit,
            offset=offset
        )

        # each api_dataset_list_entry is a dictionary with the fields:
        #    "id", "name", "history_id", "hid", "history_content_type", "deleted", "visible",
        #    "type_id", "type", "create_time", "update_time", "url", "tags", "dataset_id",
        #    "state", "extension", "purged"
        for api_dataset_list_entry in api_dataset_list:
            dataset_info = gi.datasets.show_dataset(dataset_id=api_dataset_list_entry["id"])

            # read dataset information from api
            try:
                dataset = GalaxyDataset(dataset_info)
            except MissingFieldException as e:
                # the exception is thrown by the constructor of GalaxyDataset which checks if all keys that are used
                # actually exists
                logging.warning(
                    f"not reading dataset {api_dataset_list_entry['id']} because {e} from api response")

            # filter for valid human references
            match = re.match(r"(GRCh\d+|hg\d+).*", dataset.reference_name)
            if match is None:
                # skip datasets with unknown references
                logging.warning(
                    f"not reading dataset {dataset.name} with unknown reference \"{dataset.reference_name}\"")
                continue

            # set reference name to the first match group
            #
            # THIS WILL REMOVE PATCH LEVEL FROM THE REFERENCE
            # therefore all patch levels will be grouped under the major version of the reference
            dataset.reference_name = match.group(1)

            datasets.append(dataset)
        offset += limit

        # no entries left
        if len(api_dataset_list) == 0:
            break

    # return the finished dataset list
    return datasets


@dataclass
class BeaconMetadata:
    """
    Dataset metadata to be consumed by beacon during import.

    All metadate fields will be accessible via beacons api. Given values should be ones that can
    shared without exposing too much information (i.e. do not expose internal galaxy IDs, real filenames, etc)

        Attributes:
            name (string): The name for the dataset
            dataset_id (string): Unique identifier for the dataset - any string may be given
            description (string): A short description of the dataset
            assembly_id (string): Identifier of the reference genome used to calculate the variants
            external_url (string): URL will be returned by beacon api along with matching variants from this dataset
            access_type (string): beacon supports CONTROLLED, REGISTERED, PUBLIC
                TODO... other attributes
    """
    name: str
    dataset_id: str
    description: str
    assembly_id: str
    external_url: str
    access_type: str
    create_date_time: str
    update_date_time: str
    call_count: int
    version: str
    variant_count: int

    def __json__(self) -> str:
        """
        Returns metadata json, converting snake_case attribute names to camelCase
        """
        data = {}
        # looping through all attributes and their values
        for attribute, value in vars(self).items():
            # convert attribute name to camel case (replacing _([a-z]) by upper case of the matching letter)
            key = re.sub(r"_([a-z])", lambda x: x.group(1).upper(), attribute)
            data[key] = value
        return json.dumps(data)


def prepare_metadata_file(dataset: GalaxyDataset, output_path: str) -> None:
    """
    Prepares a metadata file to be consumed by beacon database import scripts

        Parameters:
            dataset (GalaxyDataset): The dataset which will be described by the metadata
            output_path (string): Full destination path for the metadata file

        Returns:
            True if the metadata file has been written and False if ..
    """

    # assemble metadata from collected information
    metadata = BeaconMetadata(
        name=f"Galaxy.eu variants for {dataset.reference_name}",
        dataset_id=f"galaxy-{dataset.reference_name.lower()}",
        description="variants shared by galaxy.eu users",
        assembly_id=dataset.reference_name,
        external_url="usegalaxy.eu",
        access_type="PUBLIC",
        create_date_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        update_date_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        call_count=0,
        version="v0.1",
        variant_count=0 # the variant count will be updated after all datasets have been processed 
    )

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(metadata.__json__())


def download_dataset(gi: GalaxyInstance, dataset: GalaxyDataset, filename: str) -> None:
    """
    Downloads a dataset from galaxy to a given path

        Parameters:
            gi (GalaxyInstance): galaxy instance to download from
            dataset (GalaxyDataset): the dataset to download
            filename (str): output filename including complete path

        Returns:
            Nothing

    """
    try:
        gi.datasets.download_dataset(dataset.id, filename, use_default_filename=False)
    except Exception as e:
        # TODO catch exceptions
        logging.critical(f"something went wrong while downloading file - {e}")


def beacon_import(dataset_file: str, metadata_file: str) -> None:
    """
    Import a dataset to beacon

        Parameters:
            dataset_file (str): full path to the dataset file
            metadata_file (str): full path to a file containing matching metadata for the dataset
                metadata should be in BeaconMetadata format

        Returns:
            Nothing

        Note:
            This function uses BeaconDB from the beacon-python package found at https://github.com/CSCfi/beacon-python

            The connection settings are configured by ENVIRONMENT_VARIABLE or  "default value" if not set

                host: DATABASE_URL / "localhost",
                port: DATABASE_PORT / "5432",
                user: DATABASE_USER / beacon",
                password: DATABASE_PASSWORD / beacon",
                database: DATABASE_NAME / beacondb",

    """
    loop = asyncio.get_event_loop()
    global db

    dataset_vcf: VCF
    dataset_vcf = VCF(dataset_file)

    # insert dataset metadata into the database, prior to inserting actual variant data
    dataset_id = loop.run_until_complete(db.load_metadata(dataset_vcf, metadata_file, dataset_file))

    # insert data into the database
    # setting "min_ac=0" instead of the default "min_ac=1" to prevent "pop from empty list" errors
    loop.run_until_complete(db.load_datafile(dataset_vcf, dataset_file, dataset_id, min_ac=0))


async def persist_variant_origins(dataset_id: str, dataset: VCF, record):
    """
    Maps dataset_id to variant index in a separate file (which is hard-coded)

        Note:
            We do not want any information in the beacon database that may be used to reconstruct an actual file
            uploaded to galaxy. Additionally, we do not want to change the dataset import functions provided by
            beacon python.
            Therefore, a separate file is maintained linking variant indices and dataset IDs. Since we do not interfer
            with the actual variant import these indices have to be queried individually from beacons database.

        Parameters:
            dataset_id (str): Dataset id as returned by the galaxy api
            dataset (VCF): The actual dataset
            record (Any): Output file in which to persist the records

        Returns:
            Nothing.
    """

    variant: Variant
    for variant in dataset:
        for alt in variant.ALT:
            for index in await db.get_variant_indices(variant.start, variant.REF, alt):
                record.write(f"{index} {dataset_id}\n")


async def update_variant_counts():
    """
    asd
    """

    await db.update_dataset_counts()



def cleanup(dataset_file: str, metadata_file: str):
    """
    Removes given files
    """
    os.remove(dataset_file)
    os.remove(metadata_file)


def command_rebuild(args: Namespace):
    """
    Rebuilds beacon database based on datasets retrieved from galaxy

        Parameters:
             None.

        Returns:
            Nothing.

        Note:
            This function uses args from the rebuild subparser
    """

    global db
    gi = set_up_galaxy_instance(args.galaxy_url, args.galaxy_key)

    loop = asyncio.get_event_loop()

    # connect to beacons database
    os.environ['DATABASE_URL'] = args.database_host
    os.environ['DATABASE_PORT'] = args.database_port
    os.environ['DATABASE_USER'] = args.database_user
    os.environ['DATABASE_PASSWORD'] = args.database_password
    os.environ['DATABASE_NAME'] = args.database_name

    loop.run_until_complete(db.connection())

    # delete all data before the new import
    loop.run_until_complete(db.clear_database())

    if args.store_origins:
        try:
            os.remove(args.origins_file)
        except:
            # the file probably does not exist
            pass

        # open a file to store variant origins
        variant_origins_file = open(args.origins_file, "a")

    # load data from beacon histories
    for history_id in get_beacon_histories(gi):
        for dataset in get_datasets(gi, history_id):
            # dataset import happens here
            logging.info(f"next file is {dataset.name}")

            # destination paths for downloaded dataset and metadata
            dataset_file = f"/tmp/dataset-{dataset.uuid}"
            metadata_file = f"/tmp/metadata-{dataset.uuid}"

            # download dataset from galaxy
            download_dataset(gi, dataset, dataset_file)
            prepare_metadata_file(dataset, metadata_file)

            beacon_import(dataset_file, metadata_file)

            # save the origin of the variants in beacon database
            if args.store_origins:
                loop.run_until_complete(persist_variant_origins(dataset.id, VCF(dataset_file), variant_origins_file))

    # calculate variant counts
    logging.info("Setting variant counts")
    loop.run_until_complete(update_variant_counts())


def command_search(args: Namespace):
    """
    Searches a variant (as specified in command line args) across all datasets

    Note:
        This will download each dataset
    """
    gi = set_up_galaxy_instance(args.galaxy_url, args.galaxy_key)

    print(f"searching variant {args.ref} -> {args.alt} at position {args.start} (each dot is one dataset)\n")
    # load data from beacon histories
    for history_id in get_beacon_histories(gi):
        for dataset in get_datasets(gi, history_id):

            dataset_file = f"/tmp/searching-{dataset.uuid}"
            download_dataset(gi, dataset, dataset_file)

            dataset_vcf: VCF
            dataset_vcf = VCF(dataset_file)

            variant: Variant
            for variant in dataset_vcf:

                if variant.start == args.start and variant.REF == args.ref and args.alt in variant.ALT:
                    print(f"found variant in dataset {dataset.id} ({dataset.name})")

            os.remove(dataset_file)


def main():
    """
    Main function runs sub commands based on the given command line arguments
    """
    args = parse_arguments()

    set_up_logging(args.verbosity)

    if args.command == "rebuild":
        command_rebuild(args)

    if args.command == "search":
        command_search(args)


if __name__ == '__main__':
    """
    Execute the script
    """
    main()
