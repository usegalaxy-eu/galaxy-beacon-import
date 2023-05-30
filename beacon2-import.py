import argparse
import logging

import os

from argparse import Namespace


from utils import *


from pymongo import MongoClient
import json


class BeaconDB:
    def __int__(self):
        self.client = MongoClient()
        self.database_user = ''
        self.database_password = ''
        self.database_host = ''
        self.database_port = ''
        self.database_name = ''
        self.database_auth_source = ''

    def connection(self):
        try:
            self.client = MongoClient(
                # connect to MongoDB database
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


    def createCollection(self):
        names = ["analyses", "biosamples", "cohorts", "cohorts", "datasets", "genomicVariations", "individuals", "runs"]
        existing_names = self.client.beacon.list_collection_names()
        for name in names:
            if name not in existing_names:
                self.client.beacon.create_collection(name)
                self.client.beacon[name].create_index([("$**", "text")])


    def clear_database(self):
        try:
            db.client.beacon.analyses.delete_many({})
            db.client.beacon.biosamples.delete_many({})
            db.client.beacon.cohorts.delete_many({})
            db.client.beacon.datasets.delete_many({})
            db.client.beacon.genomicVariations.delete_many({})
            db.client.beacon.individuals.delete_many({})
            db.client.beacon.runs.delete_many({})
            return True
        except:
            print(f'Warning: Failed to clear database user:{self.database_user} db:{self.database_name}')
            logging.info(f'Warning: Failed to clear database user:{self.database_user} db:{self.database_name}')
            return False

    def update_dataset_counts(self):
        pass


db: BeaconDB = BeaconDB()



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
    parser.add_argument("-k", "--galaxy-key", type=str, metavar="", default="",
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




def import_to_mongodb(BFF_files: dict):
    """
    Import a dataset to beacon

        Parameters:
            BFF_files (dict): key is the file name, and value is full path to the Beacon friendly files

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


    # Import  BFF files
    for key in BFF_files.keys():
        try:
            with open(BFF_files[key]) as f:
                data = json.load(f)
                db.client.beacon.beacon[key].insert_many(data)
        except:
            print(f"the downloaded file probably does not exist file name:{key} file path:{BFF_files[key]}")
            logging.info(f"the downloaded file probably does not exist file name:{key} file path:{BFF_files[key]}")
            return False



def persist_variant_origins(dataset_id: str, dataset: str, record):
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
    try:
        with open(dataset) as j_f:
                data = json.load(j_f)
                for variant in data:
                    try:
                        ALT = variant['alternateBases']
                        start = variant['position']['start'][0]
                        REF = variant['referenceBases']
                        res = db.client.beacon.genomicVariations.find({"alternateBases": ALT}, {"start": start},
                                                                      {"referenceBases": REF})
                        record.write(f'{res["_id"]}{dataset_id}\n')
                    except:
                        print(f'some fields may not be found in {variant}')
                        continue
    except:
        print(f'the dataset file probably does not exist dataset:{dataset_id}')
        logging.info(f'the dataset file probably does not exist dataset:{dataset_id}')
        return False


def update_variant_counts():
    """
    asd
    """

    db.update_dataset_counts()




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

    db.database_user = args.database_user
    db.database_password = args.database_password
    db.database_host = args.database_host
    db.database_port = args.database_port
    db.database_name = args.database_name
    db.database_auth_source = args.database_auth_source

    if not db.connection():
        return False
    # # delete all data before the new import
    db.clear_database()



    if args.store_origins:
        if os.path.exists(args.origins_file):
            os.remove(args.origins_file)
        # open a file to store variant origins
        try:
            variant_origins_file = open(args.origins_file, "a")
        except:
            print(f"Can not open origins_file {args.origins_file}")
            logging.info(f"Can not open origins_file {args.origins_file}")
            return False

    path_dict = {"analyses": "/tmp/analyses-", "biosamples": "/tmp/biosamples-", "cohorts": "/tmp/cohorts-",
                 "datasets": "/tmp/datasets-", "genomicVariations": "/tmp/genomicVariations-",
                 "individuals": "/tmp/individuals-", "runs": "/tmp/runs-"}
    # load data from beacon histories

    BFF_files = {}
    for history_id in get_beacon_histories(gi):
        for dataset in get_datasets(gi, history_id):
            logging.info(f"next file is {dataset.name}")
            name = dataset.name.split('.')[0]
            path = path_dict[name] + dataset.uuid
            download_dataset(gi, dataset, path)
            BFF_files[name] = path
            if name=='genomicVariations':
                if args.store_origins:
                    persist_variant_origins(dataset.id, path, variant_origins_file)

    if BFF_files == {}:
        print(f"Did not get any files from Galaxy galaxy_key:{args.galaxy_key}")
        logging.info(f"Did not get any files from Galaxy galaxy_key:{args.galaxy_key}")
        return False


    if not import_to_mongodb(BFF_files):
        return False



def command_search(args: Namespace):
    """
    Searches a variant (as specified in command line args) across all datasets

    Note:
        This will download each dataset
    """
    gi = set_up_galaxy_instance(args.galaxy_url, args.galaxy_key)

    path_dict = {"analyses": "/tmp/analyses-", "biosamples": "/tmp/biosamples-", "cohorts": "/tmp/cohorts-",
                 "datasets": "/tmp/datasets-", "genomicVariations": "/tmp/genomicVariations-",
                 "individuals": "/tmp/individuals-", "runs": "/tmp/runs-"}
    print(f"searching variant {args.ref} -> {args.alt} at position {args.start} (each dot is one dataset)\n")
    # load data from beacon histories
    sign=False
    for history_id in get_beacon_histories(gi):
        for dataset in get_datasets(gi, history_id):
            name = dataset.name.split('.')[0]
            if name == "genomicVariations":
                sign=True
                genomicVariations_file = f"/tmp/genomicVariations-{dataset.uuid}"
                download_dataset(gi, dataset, genomicVariations_file)
                try:
                    with open(genomicVariations_file) as j_f:
                        data = json.load(j_f)
                        for variant in data:
                            try:
                                ALT = variant['alternateBases']
                                start = variant['position']['start'][0]
                                REF = variant['referenceBases']
                                if start == args.start and REF == args.ref and args.alt in ALT:
                                    print(f"found variant in dataset {dataset.id} ({dataset.name})")
                            except:
                                print(f'some fields may not be found in {variant}')
                                continue
                    os.remove(genomicVariations_file)
                except:
                    print(f'can not open genomicVariations_file file path:{genomicVariations_file}')
                    logging.info(f'can not open genomicVariations_file file path:{genomicVariations_file}')
                    return False
    if not sign:
        print(f'can not find genomicVariations_file galaxy_key:{args.galaxy_key}')
        logging.info(f'can not find genomicVariations_file galaxy_key:{args.galaxy_key}')
        return False




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