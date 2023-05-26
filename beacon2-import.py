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
# import asyncpg
# from db_load import BeaconDB
from requests import Response
from bioblend.galaxy import GalaxyInstance

# cyvcf2 is a vcf parser
# from cyvcf2 import VCF, Variant

from pymongo import MongoClient
import conf
import json



class BeaconExtendedDB:
    def __int__(self):
        self.client=MongoClient()
        database_user= ''
        database_password = ''
        database_host = ''
        database_port = ''
        database_name = ''
        database_auth_source = ''
    async def connection(self):
        self.client = MongoClient(
            # connect to MongoDB database
            "mongodb://{}:{}@{}:{}/{}?authSource={}".format(
                # conf.database_user,
                # conf.database_password,
                # conf.database_host,
                # conf.database_port,
                # conf.database_name,
                # conf.database_auth_source,
                self.database_user,
                self.database_password,
                self.database_host,
                self.database_port,
                self.database_name,
                self.database_auth_source
            )
        )
    async def createCollection(self):
        names=["analyses","biosamples","cohorts","cohorts","datasets","genomicVariations","individuals","runs"]
        existing_names=self.client.beacon.list_collection_names()
        print(existing_names)
        for name in names:
            if name not in existing_names:
                self.client.beacon.create_collection(name)
                # Create Collections
                # self.client.beacon.createCollection("analyses")
                # self.client.beacon.createCollection("biosamples")
                # self.client.beacon.createCollection("cohorts")
                # self.client.beacon.createCollection("datasets")
                # self.client.beacon.createCollection("genomicVariations")
                # self.client.beacon.createCollection("individuals")
                # self.client.beacon.createCollection("runs")
                self.client.beacon[name].create_index([("$**", "text")])
                # Create indexes
                # self.client.beacon.analyses.create_index([("$**", "text")])
                # self.client.beacon.biosamples.create_index([("$**", "text")])
                # self.client.beacon.cohorts.create_index([("$**", "text")])
                # self.client.beacon.datasets.create_index([("$**", "text")])
                # self.client.beacon.genomicVariations.create_index([("$**", "text")])
                # self.client.beacon.individuals.create_index([("$**", "text")])
                # self.client.beacon.runs.create_index([("$**", "text")])

    async def clear_database(self):
        db.client.beacon.analyses.delete_many({})
        db.client.beacon.biosamples.delete_many({})
        db.client.beacon.cohorts.delete_many({})
        db.client.beacon.datasets.delete_many({})
        db.client.beacon.genomicVariations.delete_many({})
        db.client.beacon.individuals.delete_many({})
        db.client.beacon.runs.delete_many({})

    async def update_dataset_counts(self):
        pass



# global variable for beacon connection
db: BeaconExtendedDB = BeaconExtendedDB()
loop = asyncio.get_event_loop()


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
    parser_rebuild.add_argument("-A", "--conf-auth-source", type=str, metavar="admin", default="admin",
                                dest="database_auth_source",
                                help="auth source for the beacon database")
    parser_rebuild.add_argument("-H", "--conf-host", type=str, metavar="", default="127.0.0.1", dest="database_host",
                                help="hostname/IP of the beacon database")
    parser_rebuild.add_argument("-P", "--conf-port", type=str, metavar="", default="27017", dest="database_port",
                                help="port of the beacon database")
    parser_rebuild.add_argument("-U", "--conf-user", type=str, metavar="", default="root", dest="database_user",
                                help="login user for the beacon database")
    parser_rebuild.add_argument("-W", "--conf-password", type=str, metavar="", default="example",
                                dest="database_password",
                                help="login password for the beacon database")
    parser_rebuild.add_argument("-N", "--conf-name", type=str, metavar="", default="beacon", dest="database_name",
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


def string_as_bool(string: Any) -> bool:
    """
    Returns the bool value corresponding to the given value

    used to typesave the api responses
    """
    if str(string).lower() in ("true", "yes", "on", "1"):
        return True
    else:
        return False


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
    response: Response = gi.make_get_request(
        f"{gi.base_url}/api/histories?q=name&qv=Beacon%20Export%20%F0%9F%93%A1&all=true&deleted=false")

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
        if "beacon_enabled" not in history_user_preferences or not string_as_bool(
                history_user_preferences["beacon_enabled"]):
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
            extension=["json", "json_bgzip"],
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
            # match = re.match(r"(GRCh\d+|hg\d+).*", dataset.reference_name)
            # if match is None:
            #     # skip datasets with unknown references
            #     logging.warning(
            #         f"not reading dataset {dataset.name} with unknown reference \"{dataset.reference_name}\"")
            #     continue

            # set reference name to the first match group
            #
            # THIS WILL REMOVE PATCH LEVEL FROM THE REFERENCE
            # therefore all patch levels will be grouped under the major version of the reference
            # dataset.reference_name = match.group(1)

            datasets.append(dataset)
        offset += limit

        # no entries left
        if len(api_dataset_list) == 0:
            break

    # return the finished dataset list
    return datasets



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


def import_to_mongodb(BFF_files: dict):
    """
    Import a dataset to beacon

        Parameters:
            BFF_files (str): full path to the Beacon frindly files

        Returns:
            Nothing

        Note:
            This function uses MongoDB from the beacon2-ri-api package found at https://github.com/EGA-archive/beacon2-ri-api/tree/master/deploy

            The connection settings are configured by ENVIRONMENT_VARIABLE or  "default value" if not set (not sure about the values need to check)

                host: DATABASE_URL / "???",
                port: DATABASE_PORT / "???",
                user: DATABASE_USER / ???",
                password: DATABASE_PASSWORD / beacon",
                database: DATABASE_NAME / mongodb",

    """

    Entities = ['analyses_file', 'biosamples_file', 'cohorts_file', 'datasets_file', 'genomicVariations_file',
                'individuals_file', 'runs_file']
    global db
    # Import  BFF files
    for key in BFF_files.keys():
        with open(BFF_files[key]) as f:
            data = json.load(f)
            collection_name = key
            db.client.beacon[collection_name].insert_many(data)



async def persist_variant_origins(dataset_id: str, dataset: str, record):
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
    global db
    with open(dataset) as j_f:
        data = json.load(j_f)
        for variant in data:
            ALT = variant['alternateBases']
            start = variant['position']['start'][0]
            REF = variant['referenceBases']
            res=db.client.beacon.genomicVariations.find({"alternateBases":ALT},{"start":start},{"referenceBases":REF})
            record.write(f'{res["_id"]}{dataset_id}\n')

async def update_variant_counts():
    """
    asd
    """

    await db.update_dataset_counts()


def cleanup(dataset_file: str, metadata_file: str):
    """
    Removes given files
    """
    # os.remove(dataset_file)
    # os.remove(metadata_file)
    pass


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

    db.database_user= args.database_user
    db.database_password = args.database_password
    db.database_host = args.database_host
    db.database_port = args.database_port
    db.database_name = args.database_name
    db.database_auth_source = args.database_auth_source
    


    loop.run_until_complete(db.connection())
    # loop.run_until_complete(db.createCollection())
    # # delete all data before the new import
    loop.run_until_complete(db.clear_database())
    


    
    if args.store_origins:
        try:
            os.remove(args.origins_file)
        except:
            # the file probably does not exist
            pass

        # open a file to store variant origins
        variant_origins_file = open(args.origins_file, "a")
    path_dict={"analyses":"/tmp/analyses-","biosamples":"/tmp/biosamples-","cohorts":"/tmp/cohorts-","datasets":"/tmp/datasets-","genomicVariations":"/tmp/genomicVariations-", "individuals":"/tmp/individuals-","runs":"/tmp/runs-"}
    # load data from beacon histories

    BFF_files = {}
    for history_id in get_beacon_histories(gi):
        for dataset in get_datasets(gi, history_id):
            logging.info(f"next file is {dataset.name}")
            # destination paths for downloaded dataset and metadata
            # analyses_file = f"/tmp/analyses-{dataset.uuid}"
            # biosamples_file = f"/tmp/biosamples-{dataset.uuid}"
            # cohorts_file = f"/tmp/cohorts-{dataset.uuid}"
            # datasets_file = f"/tmp/ datasets-{dataset.uuid}"
            # genomicVariations_file = f"/tmp/genomicVariations-{dataset.uuid}"
            # individuals_file = f"/tmp/individuals-{dataset.uuid}"
            # runs_file = f"/tmp/runs-{dataset.uuid}"
            name=dataset.name.split('.')[0]
            print (name)
            path=path_dict[name]+dataset.uuid
            download_dataset(gi, dataset, path)
            BFF_files[name]=path

            # 
            # for dataset_file in BFF_files:
            #     download_dataset(gi, dataset, dataset_file)

            # download dataset from galaxy
            # download_dataset(gi, dataset, dataset_file)
            # prepare_metadata_file(dataset, metadata_file)
    import_to_mongodb(BFF_files)

            # save the origin of the variants in beacon database
    if args.store_origins:
        loop.run_until_complete(persist_variant_origins(dataset.id, genomicVariations_file, variant_origins_file))

    # calculate variant counts
    # logging.info("Setting variant counts")
    # loop.run_until_complete(update_variant_counts())


def command_search(args: Namespace):
    """
    Searches a variant (as specified in command line args) across all datasets

    Note:
        This will download each dataset
    """
    gi = set_up_galaxy_instance(args.galaxy_url, args.galaxy_key)


    path_dict={"analyses":"/tmp/analyses-","biosamples":"/tmp/biosamples-","cohorts":"/tmp/cohorts-","datasets":"/tmp/datasets-","genomicVariations":"/tmp/genomicVariations-", "individuals":"/tmp/individuals-","runs":"/tmp/runs-"}
    print(f"searching variant {args.ref} -> {args.alt} at position {args.start} (each dot is one dataset)\n")
    # load data from beacon histories
    for history_id in get_beacon_histories(gi):
        for dataset in get_datasets(gi, history_id):
            name=dataset.name.split('.')[0]
            if name == "genomicVariations":
                genomicVariations_file = f"/tmp/genomicVariations-{dataset.uuid}"
                download_dataset(gi, dataset, genomicVariations_file)
                with open(genomicVariations_file) as j_f:
                    data = json.load(j_f)
                    for variant in data:
                        ALT = variant['alternateBases']
                        start = variant['position']['start'][0]
                        REF = variant['referenceBases']
                        if start == args.start and REF == args.ref and args.alt in ALT:
                            print(f"found variant in dataset {dataset.id} ({dataset.name})")
                os.remove(genomicVariations_file)

            # variant: Variant
            # for variant in dataset_vcf:
            #
            #     if variant.start == args.start and variant.REF == args.ref and args.alt in variant.ALT:
            #         print(f"found variant in dataset {dataset.id} ({dataset.name})")
            # for dataset_file in BFF_files:
            #     os.remove(dataset_file



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
