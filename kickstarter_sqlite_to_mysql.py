#GOAL: Extract from kickstarter database a sample of tables, 
# based on projects in Santa Monica

import os
import sqlite3
import pandas as pd
import mysql.connector

def sqlite_sample(sql, columns):
    '''Selects from sqlite database, and creates dataframe as result
    sql - the SQL SELECT statement; a tuple containing the query as a string.
    columns - list of columns to pull'''
    conn = sqlite3.connect('/Users/markmann/Desktop/INF551_Homework/Project/dataverse_files/hdd/kickstarter.db')
    cur = conn.cursor()

    cur.execute(sql)
    response = cur.fetchall()
    table_df = pd.DataFrame(response, columns=columns)
    conn.close()
    return table_df

#PROJECTS
def add_to_mysql_project(projects_df):
    '''Takes dataframe, and adds it to mysql
    Assumes for the PROJECT table only, at this point. 
    '''
    #CONNECT TO MYSQL
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password='testpassword123',
                                   database='kickstarter_samp',
                                   auth_plugin='mysql_native_password'
                                  )
    mycursor = mydb.cursor()

    #CREATE MYSQL PROJECTS TABLE
    mycursor.execute("DROP TABLE IF EXISTS project")
    mycursor.execute("CREATE TABLE project (project_id INT(10), name VARCHAR(200), state VARCHAR(2), country VARCHAR(2), creator_id INT(10), location_id INT(7), category_id INT(3), goal INT(6), pledged FLOAT(10,2), backers_count INT(4), blurb VARCHAR(200))")

    #INSERT INTO MYSQL PROJECT TABLE
    sql = 'INSERT INTO project (project_id, name, state, country, creator_id, location_id, category_id, goal, pledged, backers_count, blurb) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    for i in range(projects_df.shape[0]):
        val = (str(projects_df['project_id'][i]), str(projects_df['name'][i]), str(projects_df['state'][i]), str(projects_df['country'][i]), str(projects_df['creator_id'][i]), str(projects_df['location_id'][i]), str(projects_df['category_id'][i]), str(projects_df['goal'][i]), str(projects_df['pledged'][i]), str(projects_df['backers_count'][i]), str(projects_df['blurb'][i]))
        mycursor.execute(sql, val)
    mydb.commit()

def project_driver():
    sql_project = ('SELECT project.id, project.name, location.state, project.country, project.creator_id, project.location_id, project.category_id, project.goal, project.pledged, project.backers_count, project.blurb '
                   'FROM project '
                   'JOIN location on project.location_id = location.id '
                   'WHERE location.country = "US" AND location.state = "CA" AND location.id = "2488892"'
                   'ORDER BY project.last_modification ')
    columns = ['project_id', 'name', 'state', 'country', 'creator_id', 'location_id', 'category_id', 'goal', 'pledged', 'backers_count', 'blurb']
    projects_df = sqlite_sample(sql_project, columns)
    add_to_mysql_project(projects_df)

#CREATOR
def add_to_mysql_creator(creator_df):
    '''Takes dataframe, and adds it to mysql
    Assumes for the CREATOR table only, at this point. 
    '''
    #CONNECT TO MYSQL
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password='testpassword123',
                                   database='kickstarter_samp',
                                   auth_plugin='mysql_native_password'
                                  )
    mycursor = mydb.cursor()

    #CREATE MYSQL PROJECTS TABLE
    mycursor.execute("DROP TABLE IF EXISTS creator")
    mycursor.execute("CREATE TABLE creator (creator_id INT(10), name VARCHAR(200), slug VARCHAR(50))")

    #INSERT INTO MYSQL PROJECT TABLE
    sql = 'INSERT INTO creator (creator_id, name, slug) VALUES (%s, %s, %s)'
    for i in range(creator_df.shape[0]):
        val = (str(creator_df['creator_id'][i]), str(creator_df['name'][i]), str(creator_df['slug'][i]))
        mycursor.execute(sql, val)
    mydb.commit()

def creator_driver():
    sql_creator = ('SELECT DISTINCT creator.id, creator.name, creator.slug '
                   'FROM creator '
                   'JOIN (SELECT project.id, project.creator_id '
                       'FROM project '
                       'JOIN location on project.location_id = location.id '
                       'WHERE location.country = "US" AND location.state = "CA" AND location.id = "2488892") ca_projects on creator.id = ca_projects.creator_id '
                    )
    columns = ['creator_id', 'name', 'slug']
    creator_df = sqlite_sample(sql_creator, columns)
    #return creator_df
    add_to_mysql_creator(creator_df)

#COMMENTS
def add_to_mysql_comments(comments_df):
    '''Takes dataframe, and adds it to mysql
    Assumes for the COMMENTS table only, at this point. 
    '''
    #CONNECT TO MYSQL
    mydb = mysql.connector.connect(host="localhost",
                                   user="root",
                                   password='testpassword123',
                                   database='kickstarter_samp',
                                   auth_plugin='mysql_native_password'
                                  )
    mycursor = mydb.cursor()

    #CREATE MYSQL COMMENTS TABLE
    mycursor.execute("DROP TABLE IF EXISTS comments")
    mycursor.execute("CREATE TABLE comments (comment_id INT(10), projectid INT(10), user_name VARCHAR(50), body VARCHAR(3000))")

    #INSERT INTO MYSQL PROJECT TABLE
    sql = 'INSERT INTO comments (comment_id, projectid, user_name, body) VALUES (%s, %s, %s, %s)'
    for i in range(comments_df.shape[0]):
        val = (str(comments_df['comment_id'][i]), str(comments_df['projectid'][i]), str(comments_df['user_name'][i]), str(comments_df['body'][i]))
        mycursor.execute(sql, val)
    mydb.commit()

def comments_driver():
    sql_comments = ('SELECT comments.id, comments.projectid, comments.user_name, comments.body '
                   'FROM comments '
                   'JOIN (SELECT project.id project_id '
                       'FROM project '
                       'JOIN location on project.location_id = location.id '
                       'WHERE location.country = "US" AND location.state = "CA" AND location.id = "2488892") ca_projects on comments.projectid = ca_projects.project_id '
                    'LIMIT 1000'
                  )
    columns = ['comment_id', 'projectid', 'user_name', 'body']
    comments_df = sqlite_sample(sql_comments, columns)
    add_to_mysql_comments(comments_df)

#LOCATION - eventually

def main():
    project_driver()
    creator_driver()
    comments_driver()

if __name__ == "__main__":
    main()
    print("Done. Check MySQL for uploaded data.")