from search import firebase_output  # result from the first search
import fb_settings
import django_req # contains the user input
import requests  # to interact with firebase

def link_search(database=django_req.database, link_key=django_req.link_key, link_value=django_req.link_value):
	for keys in fb_settings.prim_keys:
		for key in keys:
			if key==link_key:
				tableindex=fb_settings.prim_keys.index(keys)
	child=fb_settings.tables[tableindex]
	
	firebase_response = fb_settings.db.child(child).order_by_child(link_key).equal_to(link_value).get()
	for firebase_query in firebase_response:
	    # the key then functions as a node in the final retrieval of the original entry in firebase
	    firebase_query_key=firebase_query.key()
	    link_output = requests.get(fb_settings.firebaseurl+'/'+child+'/'+firebase_query_key+'.json').json()
	return link_output

link_output=link_search()
print(link_output)
