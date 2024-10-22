import argparse
import logging
import re
import os
from argparse import Namespace
from utils import *
from pymongo import MongoClient
import json

class BeaconDB:
    def __init__(self):
        # Initialize the BeaconDB class with default database credentials
        self.client = MongoClient()
        self.database_user = ''
        self.database_password = ''
        self.database_host = ''
        self.database_port = ''
        self.database_name = ''
        self.database_auth_source = ''

    def connection(self):
        # Establish connection to the MongoDB database
        try:
            self.client = MongoClient(
                "mongodb://{}:{}@{}:{}/{}?authSource={}".format(
                    self.database_user,
                    self.database_password,
                    self.database_host,
                    self.database_port,
                    self.database_name,
                    self.database_auth_source
                )
            )
            return True
        except:
            print(f'Failed to connect MongoDB user:{self.database_user} db:{self.database_name}')
            logging.info(f'Failed to connect MongoDB user:{self.database_user} db:{self.database_name}')
            return False

    def clear_database(self):
        # Clear all collections in the current database
        existing_names = self.client[self.database_name].list_collection_names()
        for name in existing_names:
            try:
                self.client[self.database_name][name].drop()
            except:
                print(f'Warning: Failed to clear database user:{self.database_user} db:{self.database_name}')
                logging.info(f'Warning: Failed to clear database user:{self.database_user} db:{self.database_name}')
                return False
        return True

    def get_variant_indices(self, start: int, ref: str, alt: str, var_id: str) -> list:
        # Get variant indices from the genomicVariations collection
        rows = self.client[self.database_name]['genomicVariations'].find({'alternateBases': alt, 'referenceBases': ref, 'variantInternalId': var_id})
        rows_l = [row for row in rows]
        rows_res_list = []
        for row_dict in rows_l:
            if row_dict['position']['start'][0] == start:
                rows_res_list.append(row_dict)
        return [str(row["_id"]) for row in rows_res_list]

    def update_dataset_counts(self):
        # Update dataset counts in the datasets collection
        try:
            rows = self.client[self.database_name]['genomicVariations'].find({})
            count = 0
            for row in rows:
                count += 1
            self.client[self.database_name]['datasets'].update_many({}, {'$set': {'data_count': str(count)}})
            return f'There are {str(count)} data'
        except:
            return 'There are some errors in update dataset counts'


db: BeaconDB = BeaconDB()

def parse_arguments() -> Namespace:
    # Defines and parses command line arguments for this script
    parser = argparse.ArgumentParser(description="Push genomic variants from galaxy to beacon.")
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True

    # Arguments controlling output
    parser.add_argument('-d', '--debug', help="Print lots of debugging statements", action="store_const", dest="loglevel", const=logging.DEBUG, default=logging.WARNING)
    parser.add_argument('-v', '--verbose', help="Be verbose", action="store_const", dest="loglevel", const=logging.INFO)

    # Arguments controlling galaxy connection
    parser.add_argument("-u", "--galaxy-url", type=str, metavar="", default="http://localhost:8080", dest="galaxy_url",
                        help="galaxy hostname or IP")
    parser.add_argument("-k", "--galaxy-key", type=str, metavar="", default="",
                        dest="galaxy_key", help="API key of a galaxy user WITH ADMIN PRIVILEGES")

    # Sub-parser for command "rebuild"
    parser_rebuild = subparsers.add_parser('rebuild')
    parser_rebuild.add_argument("-s", "--store-origins", default=False, dest="store_origins",
                                action="store_true",
                                help="make a local file containing variantIDs with the dataset they stem from")
    parser_rebuild.add_argument("-o", "--origins-file", type=str, metavar="", default="/tmp/variant-origins.txt",
                                dest="origins_file",
                                help="full file path of where variant origins should be stored (if enabled)")

    # Database connection arguments
    parser_rebuild.add_argument("-A", "--db-auth-source", type=str, metavar="admin", default="admin",
                                dest="database_auth_source",
                                help="auth source for the beacon database")
    parser_rebuild.add_argument("-H", "--db-host", type=str, metavar="", default="127.0.0.1", dest="database_host",
                                help="hostname/IP of the beacon database")
    parser_rebuild.add_argument("-P", "--db-port", type=str, metavar="", default="27017", dest="database_port",
                                help="port of the beacon database")
    parser_rebuild.add_argument("-U", "--db-user", type=str, metavar="", default="root", dest="database_user",
                                help="login user for the beacon database")
    parser_rebuild.add_argument("-W", "--db-password", type=str, metavar="", default="example",
                                dest="database_password",
                                help="login password for the beacon database")
    parser_rebuild.add_argument("-N", "--db-name", type=str, metavar="", default="beacon", dest="database_name",
                                help="name of the beacon database")

    return parser.parse_args()

def get_datasets(gi: GalaxyInstance, history_id: str) -> list:
    # Fetches a given history's datasets from Galaxy
    datasets = []
    offset = 0
    limit = 500

    # Galaxy API uses paging for datasets. This while loop continuously retrieves pages of *limit* datasets
    while True:
        # Retrieve a list of datasets from the Galaxy API
        api_dataset_list = gi.datasets.get_datasets(
            history_id=history_id,
            deleted=False,
            extension=["json", "json_bgzip"],
            limit=limit,
            offset=offset
        )

        # Each api_dataset_list_entry is a dictionary with dataset details
        for api_dataset_list_entry in api_dataset_list:
            dataset_info = gi.datasets.show_dataset(dataset_id=api_dataset_list_entry["id"])
            try:
                dataset = GalaxyDataset(dataset_info)
            except MissingFieldException as e:
                logging.warning(f"Not reading dataset {api_dataset_list_entry['id']} because {e} from API response")
                continue

            # Filter for valid human references
            match = re.match(r"(GRCh\d+|hg\d+).*", dataset.reference_name)
            if match is None:
                logging.warning(f"Not reading dataset {dataset.name} with unknown reference \"{dataset.reference_name}\"")
                continue

            # Set reference name to the first match group (removes patch level from reference)
            dataset.reference_name = match.group(1)
            datasets.append(dataset)
        offset += limit

        # No entries left
        if len(api_dataset_list) == 0:
            break

    return datasets

def download_dataset(gi: GalaxyInstance, dataset: GalaxyDataset, filename: str) -> None:
    # Downloads a dataset from Galaxy to a given path
    try:
        gi.datasets.download_dataset(dataset.id, filename, use_default_filename=False)
    except Exception as e:
        logging.critical(f"Something went wrong while downloading file - {e} filename:{filename}")

def import_to_mongodb(collection_name, datafile_path):
    # Import data from a given file path into the specified MongoDB collection
    try:
        with open(datafile_path) as f:
            data = json.load(f)
            db.client[db.database_name][collection_name].insert_many(data)
        return True
    except:
        print(f"The downloaded file probably does not exist. file name:{datafile_path}")
        logging.info(f"The downloaded file probably does not exist. file name:{datafile_path}")
        return False

def persist_variant_origins(dataset_id: str, dataset: str, record):
    # Maps dataset_id to variant index in a separate file
    try:
        with open(dataset) as j_f:
            data = json.load(j_f)
            for variant in data:
                try:
                    ALT = variant['alternateBases']
                    start = variant['position']['start'][0]
                    REF = variant['referenceBases']
                    var_id = variant['variantInternalId']
                except:
                    print(f'Some fields may not be found')
                    continue
                try:
                    res_list = db.get_variant_indices(start, REF, ALT, var_id)
                    for res_id in res_list:
                        record.write(f'data_id:{res_id} dataset_id:{dataset_id} alternateBases:{ALT} start:{start} referenceBases:{REF} variantInternalId:{var_id}\n')
                except:
                    print(f'Something went wrong when searching this field and recording')
                    continue
    except:
        print(f'The dataset file probably does not exist dataset:{dataset_id}')
        logging.info(f'The dataset file probably does not exist dataset:{dataset_id}')
        return False

def update_variant_counts():
    # Update variant counts in the dataset
    info = db.update_dataset_counts()
    return info

def command_rebuild(args: Namespace):
    # Rebuild the beacon database based on datasets retrieved from Galaxy
    global db
    gi = set_up_galaxy_instance(args.galaxy_url, args.galaxy_key)

    db.database_user = args.database_user
    db.database_password = args.database_password
    db.database_host = args.database_host
    db.database_port = args.database_port
    db.database_name = args.database_name
    db.database_auth_source = args.database_auth_source

    if not db.connection():
        return False
    
    db.clear_database()

    if args.store_origins:
        if os.path.exists(args.origins_file):
            os.remove(args.origins_file)
        try:
            variant_origins_file = open(args.origins_file, "a")
        except:
            print(f"Cannot open origins_file {args.origins_file}")
            logging.info(f"Cannot open origins_file {args.origins_file}")
            return False

    path_dict = {
        "analyses": "analyses",
        "biosamples": "biosamples",
        "cohorts": "cohorts",
        "genomicVariations": "genomicVariations",
        "individuals": "individuals",
        "runs": "runs"
    }
    
    for history_id in get_beacon_histories(gi):
        for dataset in get_datasets(gi, history_id):
            logging.info(f"Next file is {dataset.name}")
            for key in path_dict:
                if key in dataset.name:
                    # Match dataset name to appropriate collection and construct file path
                    collection_name = path_dict[key]
                    path = f"/tmp/{collection_name}-{dataset.uuid}"
                    download_dataset(gi, dataset, path)
                    if not import_to_mongodb(collection_name, path):
                        return False
                    if key == 'genomicVariations' and args.store_origins:
                        persist_variant_origins(dataset.id, path, variant_origins_file)

    logging.info("Setting variant counts")
    info = update_variant_counts()
    logging.info(f"{info}")

def main():
    # Main function to run sub commands based on the given command line arguments
    args = parse_arguments()

    logging.basicConfig(level=args.loglevel)

    if args.command == "rebuild":
        command_rebuild(args)

if __name__ == '__main__':
    main()
