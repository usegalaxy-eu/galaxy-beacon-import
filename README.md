# galaxy-beacon-import

Utility to import variants from specific galaxy histories to a beacon instance.

##


# Try it locally

## 1. Get the right galaxy

The script needs a custom endpoint of the galaxy API which is included in the following branch

    git clone -b branch url.git
    cd galaxy

## 2. Create galaxy admin user

The beacon import can only be run by admin users. To create an admin user follow these steps

* Create a `galaxy.yml` from sample


    cp config/galaxy.yml.sample config/galaxy.yml

* Edit `config/galaxy.yml`, adding your email to `admin_users`


    sed -i 's/#admin_users: null/admin_users: <your-email>/g' config/galaxy.yml



* Start galaxy

    
    ./run.sh


* Open galaxy in your browser and register with the same email added to `admin_users`

* Go to `User -> Preferences` and select `Manage API Key`. You will see an input field and a button that reads
`Create a new key`. Press this button and remember the key for later.

## 3. Provide a dataset

Perform the following actions in your local galaxy (`http://localhost:8080`)

* Log in as any user
* Go to `User -> Preferences ` and select `Manage Beacon`
* In the Modal, press the green button to enable beacon for this user
* Create a history called `___BEACON_PICKUP___`
* Upload any `.vcf` file to the history

## 4. Start beacon-python

Beacon python is an implementation of beacon in python. It will be the target of the import.

The project also comes with a `docker-compose.yml` so starting a local instance is pretty straight forward.

    git clone https://github.com/CSCfi/beacon-python.git
    cd beacon-python/deploy
    docker-compose up -d


## 6. Prepare import script

Thus far the script has been tested under `Python 3.8.10`. It requires some modules that can be installed from 
requirements.txt.

    git clone https://github.com/Paprikant/galaxy-beacon-import.git
    cd galaxy-beacon-import
    pip3 install -r requirements.txt
    
## 7. Run the import

    ./beacon-import.py -k <api-key-from-step-2>