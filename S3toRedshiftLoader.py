# -*- coding: utf-8 -*-
"""
Created on Mon Sep  3 19:01:13 2018

@author: aishwarya saraf
"""

import pandas as pd
import psycopg2
from aws_config import *

#JSON to CSV converter (if in case of complex JSONs normalisation of JSON can be achieved by json_normalize)
def JSON2CSV(json_file,csv_file):
    df = pd.read_json(json_file)
    df.to_csv(csv_file)
    
def S3Redshift_Engine():
    conn_string = "dbname='{}' port='{}' user='{}' password='{}' host='{}'".format(dbname,port,username,password,host_url)
    try:
        con = psycopg2.connect(conn_string)
        print("connection successful!")
    except:
        print("Unable to connect to Redshift")
    
    cur = con.cursor()
    try:
        cur.execute(query1)
        cur.execute(query2)
        cur.execute(query3)
        cur.execute(query4)
        cur.execute(query5)
        print("Load executed successfully")
    except:
        print("Failed to execute Load")
        con.close()

JSON2CSV("<path_to_Json_file>","<path_to_csv_file>")
S3Redshift_Engine()
