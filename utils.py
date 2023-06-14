from bioblend.galaxy import GalaxyInstance
from dataclasses import dataclass
from typing import Any, Dict, List
from requests import Response
import logging
import json



#This is Shared 
class MissingFieldException(Exception):
    """
    Exception for a missing field in a dict
    """
    pass


#This is shared 
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


## This is Shared
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

#This is Shared
def string_as_bool(string: Any) -> bool:
    """
    Returns the bool value corresponding to the given value

    used to typesave the api responses
    """
    if str(string).lower() in ("true", "yes", "on", "1"):
        return True
    else:
        return False

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
        if "beacon_enabled" not in history_user_preferences or not string_as_bool(history_user_preferences["beacon_enabled"]):
            continue

        history_ids.append(history["id"])

    return history_ids