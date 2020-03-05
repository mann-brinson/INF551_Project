from django_req import django_request # temp file: contains the user input
from db_specs import db_specs # file with all the information on the databases in the dropdown menu
import requests  # to interact with firebase
import json # to convert csv to json format
from collections import Counter # to count output frequency
from firebase_admin import db # Import database module
import pyrebase

# input from user from the django_request file.
database, searchterm = django_request()
# retrieve the correct url for the user input
firebaseurl=db_specs[database]['firebaseurl']
# Parse multiple words in searchterm into separate keywords
keywords = searchterm.lower().split() 


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
# first, initialize empty lists
tables=list()
columns=list()
prim_keys=list()
frgn_keys=list()
# then fill with the correct specs from the db_specs file
for tbl in db_specs[database]['tables']:
	# retrieve the table names:
	tables.append(tbl)
	# retrieve the column names:
	columns.append(db_specs[database]['tables'][tbl]['columns'])
	# retrieve the primary and foreign keys:
	prim_keys.append(db_specs[database]['tables'][tbl]['primarykeys'])
	frgn_keys.append(db_specs[database]['tables'][tbl]['foreignkeys'])


# REPLACE WITH TABLE IN ALL CAPS ONCE WORLD DATABASE IS RE-UPLOADED FROM MYSQL
if database=='world':
	temp_table='table'
if database in ['alumni','kickstarter']:
	temp_table='TABLE'


firebase_output=list()

for word in keywords:
	try:
		# retrieve search results from the index (multiple observations for each keyword)
		responses=requests.get(firebaseurl+'/index/'+word+'.json').json()	
		# parse the search results
		for response in responses:
			# assign the "child" for the firebase query
			child=response[temp_table]
			# in order to know which primary key to search on, need to know the table
			tableindex=tables.index(response[temp_table])
			# based on the table, the primary key(s) is(are) assigned
			obs_prim_keys=prim_keys[tableindex]
			# if there is one primary key: 
			if len(obs_prim_keys)==1:
				obs_prim_key=obs_prim_keys[0] 
				# assign the value of the primary key from the response for the firebase query
				query_val=response[obs_prim_key]
				# search firebase for the entry that contains that query value at the right location
				firebase_queries = db.child(child).order_by_child(obs_prim_key).equal_to(query_val).get()
				# the queries are returned as pyrebase objects for which you can retrieve keys
				for firebase_query in firebase_queries:
					# the key then functions as a node in the final retrieval of the original entry in firebase
					firebase_query_id=firebase_query.key()
					firebase_output.append(requests.get(firebaseurl+'/'+child+'/'+firebase_query_id+'.json').json())
			# if there are two primary keys for a given table
			if len(obs_prim_keys)==2:
				# assign the first primary key and value
				obs_prim_key1=obs_prim_keys[0] 
				query_val1=response[obs_prim_key1]
				# assign the second primary key and value
				obs_prim_key2=obs_prim_keys[1] 
				query_val2=response[obs_prim_key2]	
				# search firebase for the entries that contains that query value at the right location
				# there could be multiple at this time.
				firebase_queries = db.child(child).order_by_child(obs_prim_key1).equal_to(query_val1).get()		 
				for firebase_query in firebase_queries:
					firebase_query_id=firebase_query.key()
					# NEED TO ADD SECTION TO PULL OUT THE RIGHT OBSERVATIONS THAT ALSO MATCH 
					# ON THE SECOND PRIMARY KEY FOR THE WORLD (COUNTRYLANGUAGE) DATABASE. 
					# FIRST THIS NEED TO BE ADDED TO FIREBASE IN ORDER TO TEST 
					firebase_output.append(requests.get(firebaseurl+'/'+child+'/'+firebase_query_id+'.json').json())

	# if there's no node in the firebase index, catch the error
	except TypeError:
		print(f'Keyword "{word}" does not exist in database')
		break #go to the next keyword


for line in firebase_output: 
	print(line)

