# Galaxy Beacon import

Utility to import variants from specific galaxy histories to a beacon instance.

##

# Try it locally

## 1. Set up galaxy

You will need a galxy instance where you have admin privileges. You can for example clone galaxy and run it with the included `run.sh` script.

* To select VCF files in the galaxy frontend enable the setting `enable_beacon_integration` in your galaxy.yml
* To get api access add your email in the setting `admin_users`

## 2. Provide a dataset

Perform the following actions in your local galaxy (`http://localhost:8080`)

* Log in as any user
* Go to `User -> Preferences ` and select `Manage Beacon`
* In the Modal, press the green button to enable beacon for this user
* Use the button to switch to your beacon history
* Upload any `.vcf` file to the history

## 3. Start beacon-python

Beacon python is an implementation of beacon in python. It will be the target of the import.

The project also comes with a `docker-compose.yml` so starting a local instance is pretty straight forward.

    git clone https://github.com/CSCfi/beacon-python.git
    cd beacon-python/deploy
    docker-compose up -d


## 4. Prepare import script

Thus far the script has been tested under `Python 3.8.10`. It requires some modules that can be installed from 
requirements.txt.

    git clone https://github.com/Paprikant/galaxy-beacon-import.git
    cd galaxy-beacon-import
    pip3 install -r requirements.txt
    
## 5. Run the import

    ./beacon-import.py -k <api-key-from-step-2>
