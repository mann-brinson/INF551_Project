#GOAL: Convert the world.sql into json, so it can be uploaded to Firebase
import os
import mysql.connector
import json
import requests
import re

def select_from_table(table):
    '''Select all rows and column headers from specified table.
    Currently assumes we are selecting from WORLD database
    mycursor - MySQL cursor object returned from connect_to_mysql()
    table - the table name to select from'''
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password='testpassword123',
                                   database='world',
                                   auth_plugin='mysql_native_password'
                                  )
    mycursor = mydb.cursor()

    #Get the table headers into a list
    col_headers = []
    sql = "desc " + table
    mycursor.execute(sql)
    [col_headers.append(column[0].lower()) for column in mycursor.fetchall()]

    #Get the row contents into a list of lists
    lo_rowlist = []
    sql = "SELECT * FROM " + table
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    for row in myresult:
        rowlist = []
        for item in row:
            rowlist.append(str(item))
        lo_rowlist.append(rowlist)

    return col_headers, lo_rowlist

def header_data(col_headers, lo_rowlist):
    '''Simply add the col_headers to the lo_rowlist.'''
    header_data = []
    header_data.append(col_headers)
    for rowlist in lo_rowlist:
        header_data.append(rowlist)
    return header_data

def listDict(header_data):
    '''Convert list of rows - with header - to list of dict'''
    #Make a list of attributes
    list_of_attr = []
    for item in header_data[0]:
        list_of_attr.append(item)

    #For each row, build up a rowdict and then add to list_of_rowdict
    list_of_rowdict = []
    row_counter = 0
    for row in header_data[1:]: #Assume data starts after header
        rowdict = {}
        attr_counter = 0
        while attr_counter < len(row):
            key = list_of_attr[attr_counter]
            value = row[attr_counter]
            rowdict[key] = value
            attr_counter += 1
        list_of_rowdict.append(rowdict)
        row_counter += 1
    return list_of_rowdict

def convert_json(listDict):
    '''Convert listDict to json.'''
    json_table = json.dumps(listDict)
    return json_table

def json_to_firebase(json_table, full_path):
    '''Put the json_table to firebase.''' #NOTE: May want to change to DELETE + PUT, or PATCH
    response = requests.put(full_path, json_table)

def build_inv_index(inv_index_full, list_of_rowdict, table, p_key):
    ''' Takes in the inv_index_full, and builds it with unique key-value pairs, per table.
    inv_index_full - inverted index that we are adding to
    list_of_rowdict - list-of-dicts, where each dict is a row in table
    table - the table name
    primary_key - the name of the unique id (Ex: 'Code', 'ID', 'Language')
    '''
    regex = re.compile('[^A-Za-z0-9 ]+')
    
    for row in list_of_rowdict:
        rowid = row[p_key]
        rowid2 = rowid #Convert to lowercase
        rowid3 = re.sub(r'[~`-]', '', rowid2)
        rowid4 = re.sub(r'\\', '', rowid3)
        rowid5 = regex.sub('', rowid4)
        rowid6 = rowid5.strip()
        
        row_cols = list(row.keys())
        for col in row_cols:
            val = row[col]

            #STEP 1: Check if the value is a string
            #Screen if attribute value is int
            try:
                val_int = int(val)
                continue 
            except ValueError:
                pass #Test passed - move onto next screening

            #Screen if attribute value is float
            try:
                val_float = float(val)
                continue 
            except ValueError:
                pass #Test passed - move onto next screening

            #Screen out attributes that are 'NULL'
            if val == 'NULL':
                continue
            elif val == '':
                continue
            else: 
                #STEP 2: Preprocess the string, before adding to index
                val_list = val.split(' ') #Split on space
                for word in val_list:
                    word2 = word.lower() #Convert to lowercase
                    word3 = re.sub(r'[~`!@#$%^&*-_+=|?]', '', word2)
                    word4 = re.sub(r'\\', '', word3)
                    word5 = regex.sub('', word4)
                    if word5 == '': #Removes keys that are nothing ''
                        continue
                    
                    # If the string value does not yet in inv_index_full, add it
                    if word5 not in inv_index_full:
                        inv_index_full[word5] = [{'table': table.lower(), 'column': col.lower(), p_key.lower(): rowid6}]

                    # If the word already exists in dictionary, append to that word value list 
                    else:
                        inv_index_full[word5].append({'table': table.lower(), 'column': col.lower(), p_key.lower(): rowid6})

def main():
    '''This is a driver function, that will run for each specified table in db. '''
    tables = ['country', 'city', 'countrylanguage']
    primary_keys = ['code', 'id', 'id']
    index = 'index'
    root = 'https://inf551-world-project.firebaseio.com/'
    suffix = '.json'
    inv_index_full = dict()
    for p_key, table in zip(primary_keys, tables):
        #print(p_key)
        col_headers, lo_rowlist = select_from_table(table)
        header_d = header_data(col_headers, lo_rowlist)
        list_of_rowdict = listDict(header_d)
        json_table = convert_json(list_of_rowdict)
        full_path = root + table + suffix
        json_to_firebase(json_table, full_path)
        
        #Build up an inverted index, and convert to json
        build_inv_index(inv_index_full, list_of_rowdict, table, p_key)
        
    #Convert the inv_index to json
    json_inv_index = convert_json(inv_index_full)

    #Put the json_inv_index file into firebase
    full_path = root + index + suffix
    json_to_firebase(json_inv_index, full_path)

if __name__ == "__main__":
    main()
    print("Done. Check Firebase for uploaded data.")
