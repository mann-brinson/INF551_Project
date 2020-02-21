#GOAL: Convert the world.sql into json, so it can be uploaded to Firebase
import os
import mysql.connector
import json
import requests

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
    [col_headers.append(column[0]) for column in mycursor.fetchall()]

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

def main():
    '''This is a driver function, that will run for each specified table in db. '''
    tables = ['country', 'city', 'countrylanguage']
    root = 'https://inf551world.firebaseio.com/'
    suffix = '.json'
    for table in tables:
        col_headers, lo_rowlist = select_from_table(table)
        header_d = header_data(col_headers, lo_rowlist)
        list_of_rowdict = listDict(header_d)
        json_table = convert_json(list_of_rowdict)
        full_path = root + table + suffix
        json_to_firebase(json_table, full_path)

if __name__ == "__main__":
    main()
    print("Done. Check Firebase for uploaded data.")