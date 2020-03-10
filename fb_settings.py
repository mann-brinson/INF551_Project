from db_specs import db_specs # file with all the information on the databases in the dropdown menu
# from firebase_admin import db # Import database module (not necessary right now)
import pyrebase # to interact with firebase
from django_req import database # contains the user input


# retrieve the correct url for the user input
firebaseurl=db_specs[database]['firebaseurl']

# configure firebase for retrieval (remove when app in place)
config = {
  "apiKey": "apiKey",
  "authDomain": "projectId.firebaseapp.com",
  "databaseURL": firebaseurl,
  "storageBucket": "projectId.appspot.com"
}

firebase = pyrebase.initialize_app(config)
# Get a reference to the database service
db = firebase.database()

# use user input to retrieve the correct database/table characteristics
tables=list()
prim_keys=list()
# retrieve specs from the db_specs file
for tbl in db_specs[database]['tables']:
    # retrieve the table names:
    tables.append(tbl)
    # retrieve the primary keys:
    prim_keys.append(db_specs[database]['tables'][tbl]['primarykeys'])

