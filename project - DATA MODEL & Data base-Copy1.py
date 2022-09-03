#!/usr/bin/env python
# coding: utf-8

# # Example

# # creating a table with postgresSQL

# In[1]:


get_ipython().system('pip install psycopg2')


# In[2]:


import psycopg2


# # create a connection to the database 

# In[3]:


try:
    conn = psycopg2.connect('host=127.0.0.1 dbname=postgres user=postgres password=Indra@123')
except psycopg2.Error as e:
    print('error : could not make connection to the postgress database')
    print(e)


# # use the connection to get a cursor that can be used to execute queries

# In[4]:


try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print('error : could not make connection to the postgress database')
    print(e)
    


# # set automatic commit to be true so that each action is commited without having to call conn.commit() after each command.

# In[5]:


conn.set_session(autocommit=True)


# # create a database to do the work in

# In[6]:


try:
    cur.execute('create database myfirstdb')  # sql syntax
except psycopg2.Error as e:
    print(e)


# # add the database name in the connect statemet .let's close our connection to the default database, reconnect to the udacity database , and get a new cursor.

# In[7]:


try:
    conn.close()
except psycog2.Error as e :
    print(e)

try:
    conn = psycopg2.connect('host=127.0.0.1 dbname=myfirstdb user=postgres password=Indra@123')
except psycog2.Error as e :
    print('Error : could not make connection to the myfirstdb database')
    print(e)    

try:
    curr = conn.cursor()
except psycog2.Error as e :
     print('Error : could not get cursor to the  database')
     print(e)    

conn.set_session(autocommit=True)


# # create table for students which includes below columns
# student_id
# 
# name
# 
# age
# 
# gender
# 
# subject
# 
# marks

# In[8]:


try:
    curr.execute('create table if not exists students (student_id int,name varchar,age int,gender varchar,subject varchar,marks int);')
except psycopg2.Error as e :
    print('Error : Issue creating table')
    print(e)


# # insert the following two rows in the table
# First Row : 1,'Raj',23,'Male','Python',85
# 
# Second Row : 2,'Priya',22,'Female','Python',86

# In[9]:


try:
    curr.execute('insert into students (student_id ,name ,age ,gender ,subject ,marks )\
    values (%s,%s,%s,%s,%s,%s)',\
     (1,'Raj',23,'Male','python',85))            
except psycopg2.Error as e :
     print('Error : Inserting rows')
     print(e)
                 
try:
    curr.execute('insert into students (student_id ,name ,age ,gender ,subject ,marks )\
    values (%s,%s,%s,%s,%s,%s)',\
     (2,'Priya',22,'Female','Python',86))            
except psycopg2.Error as e :
     print('Error : Inserting rows')
     print(e)                 


# # validate your data was inserted into the table

# In[10]:


try:
    curr.execute('select * from students;')            
except psycopg2.Error as e :
     print('Error : selecting rows')
     print(e)
    
row = curr.fetchone()
while row:
    print(row)
    row = curr.fetchone()


# # finally close your cursor and connection

# In[11]:


curr.close()
conn.close()


# # project

# In[12]:


get_ipython().system('pip install pandas')


# In[13]:


import psycopg2
import pandas as pd


# In[14]:


def create_database():
    #connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=Indra@123")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    #create sparkify database with UTFB encoding
    cur.execute("drop database if exists project1")
    cur.execute("create database project1")
    
    #close connection to default database
    conn.close()
    
    #connect to sparkify database 
    conn = psycopg2.connect('host=127.0.0.1 dbname=project1 user=postgres password=Indra@123')
    cur = conn.cursor()
    
    return cur,conn


# In[15]:


def drop_tables(cur,conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


# In[16]:


def create_table(cur,conn):
     for query in create_table_queries:
        cur.execute(query)
        conn.commit()


# In[17]:


movies = pd.read_csv('C:/Users/lenovo/Downloads/ml-latest-small/ml-latest-small/movies.csv')


# In[18]:


movies.head(5)


# In[19]:


ratings = pd.read_csv('C:/Users/lenovo/Downloads/ml-latest-small/ml-latest-small/ratings.csv')


# In[20]:


ratings.head()


# In[21]:


ratings1 = ratings[['userId','movieId','rating']] # selecting only 3 cols


# In[22]:


ratings1.head()


# In[23]:


tags = pd.read_csv('C:/Users/lenovo/Downloads/ml-latest-small/ml-latest-small/tags.csv')


# In[24]:


tags.head()


# In[25]:


tags1 = tags[['userId','movieId','tag']]


# In[26]:


tags1.head()


# In[27]:


cur , conn = create_database()


# In[28]:


movie_titles_create_table1 = (""" create table if not exists movie_titles(
movieId int primary key,
title varchar,
genres varchar)"""
               ) 


# In[29]:


cur.execute(movie_titles_create_table1)
conn.commit()


# In[30]:


ratings_create_table2 = (""" create table if not exists ratings(
userId int,
movieId int ,
rating int)"""
            ) 


# In[31]:


cur.execute(ratings_create_table2)
conn.commit()


# In[32]:


tags_create_table3 = (""" create table if not exists tags(
userId int,
movieId int ,
tag varchar)"""
            ) 


# In[33]:


cur.execute(tags_create_table3)
conn.commit()


# In[34]:


movies_titles_insert_data = (''' insert into movie_titles(
movieId,
title,
genres)
values (%s,%s,%s) '''
      )


# In[35]:


for i,row in movies.iterrows():
    cur.execute(movies_titles_insert_data, list(row))


# In[36]:


conn.commit()


# In[37]:


ratings_insert_data = (''' insert into ratings(
userId,
movieId,
rating)
values (%s,%s,%s) '''
      )


# In[38]:


for i,row in ratings1.iterrows():
    cur.execute(ratings_insert_data, list(row))


# In[39]:


conn.commit()


# In[40]:


tags_insert_data = (''' insert into tags(
userId,
movieId,
tag)
values (%s,%s,%s) '''
      )


# In[41]:


for i,row in tags1.iterrows():
    cur.execute(tags_insert_data, list(row))


# In[42]:


conn.commit()


# In[ ]:




