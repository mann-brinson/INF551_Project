import fb_settings
import django_req # contains the user input
import ast # to convert dictionaries as strings back to dictionaries
import requests  # to interact with firebase
import pyrebase # to interact with firebase


def search_firebase(database=django_req.database, searchterm=django_req.searchterm):
    
    # Parse multiple words in searchterm into separate keywords
    keywords = searchterm.lower().split() 
    
    firebase_output=list()   
    for word in keywords:
        try:
            # retrieve search results from the index (multiple observations for each keyword)
            responses=requests.get(fb_settings.firebaseurl+'/index/'+word+'.json').json()   
            # parse the search results
            for response in responses:
                # assign the "child" for the firebase query, which is the table name 
                child=response['table']
                # the tables are ordered in a list (for each database), and can use the index to get the primary key (see db_specs.py)
                # based on the table index, the primary key(s) is(are) assigned
                tableindex=fb_settings.tables.index(response['table'])
                obs_prim_keys=fb_settings.prim_keys[tableindex]
                # if there is one primary key: 
                if len(obs_prim_keys)==1:
                    obs_prim_key=obs_prim_keys[0] 
                    # assign the value of the primary key from the response as the firebase query
                    query_val=response[obs_prim_key]
                    # search firebase for the entry that contains that query value at the right location
                    firebase_queries = fb_settings.db.child(child).order_by_child(obs_prim_key).equal_to(query_val).get()
                    # the queries are returned as pyrebase objects for which you can retrieve keys
                    for firebase_query in firebase_queries:
                        # the key then functions as a node in the final retrieval of the original entry in firebase
                        firebase_query_key=firebase_query.key()
                        firebase_output.append(requests.get(fb_settings.firebaseurl+'/'+child+'/'+firebase_query_key+'.json').json())
                # if there are two primary keys for a given table, we have to make sure it's a double match
                if len(obs_prim_keys)==2:
                    
                    # assign the first primary key and value
                    obs_prim_key_a=obs_prim_keys[0] 
                    query_val_a=response[obs_prim_key_a]
                    # assign the second primary key and value
                    obs_prim_key_b=obs_prim_keys[1] 
                    query_val_b=response[obs_prim_key_b]    
                    # DOUBLE CHECK THE FOLLOWING CODE ONCE FIREBASE IS UP TO DATE
                    # NOT SURE IF I NEED TO ITERATE OVER THINGS SINCE MULTIPLE ONES CAN MATCH
                    # search for the original entries matching the first and second key
                    firebase_queries_a = fb_settings.db.child(child).order_by_child(obs_prim_key_a).equal_to(query_val_a).get()  
                    firebase_queries_b = fb_settings.db.child(child).order_by_child(obs_prim_key_b).equal_to(query_val_b).get()
    
                    firebase_output_a=list()
                    firebase_output_b=list()
                    for firebase_query in firebase_queries_a:
                        firebase_query_key_a=firebase_query.key()
                        firebase_output_a.append(requests.get(fb_settings.firebaseurl+'/'+child+'/'+firebase_query_key_a+'.json').json())
                    for firebase_query in firebase_queries_b:
                        firebase_query_key_b=firebase_query.key()
                        firebase_output_b.append(requests.get(fb_settings.firebaseurl+'/'+child+'/'+firebase_query_key_b+'.json').json())
                    # then look for overlap between the two, and only keep if they're in both lists
                    for dict_a in firebase_output_a:
                        #print(f'dict_a={dict_a}')
                        for dict_b in firebase_output_b:
                            #print(f'dict_b={dict_b}')
                            if dict_a == dict_b:
                                firebase_output.append(dict_a)
                                break
        # if there's no node in the firebase index, catch the error
        except TypeError:
            print(f'Keyword "{word}" does not exist in database')
            break #go to the next keyword

    # if there's a results that got added for multiple keywords, they'll occur twice in the output
    # the following section counts how many of the same results there are, 
    # and orders them by their frequency (highest first). Since they output is in a list of dictionaries, 
    # a few back and forth conversions are necessary (dict->str->dict) 
    # only reorder the output if there is more than one entry and more than one keyword
    if len(firebase_output)>1 and len(keywords)>1:
        count_output=dict()
        ordered_output=list()
        # iterate over the results, and count when they occur more than once 
        # (meaning that they had a match with multiple keywords). This creates a dictionary 
        # with the original dictionaries as keys (as a string representation) and the counter as the value
        for index in range(len(firebase_output)):
            if str(firebase_output[index]) not in count_output:
                count_output[str(firebase_output[index])]=1
            else:
                count_output[str(firebase_output[index])]+=1
        # then pick out the results that were found most often first 
        # starting with the max nr of original keywords and counting backwards
        for i in range(len(keywords), 0, -1):
            for output, count in count_output.items():
                if count==i:
                    # and add them back in this order as a dictionary to the final list 
                    # using ast.literal_eval to safely convert back to dict from string
                    ordered_output.append(ast.literal_eval(output))
        # return the ordered list of dictionaries
        firebase_output=ordered_output
    return firebase_output

firebase_output = search_firebase()

for output in firebase_output:
    print(output)
