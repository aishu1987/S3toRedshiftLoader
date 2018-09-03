#Config file for S3toRedshift ETL Loader

dbname = "db1"
target_tablename = "tbl1"
stage_tablename = "tbl2"
port = "port"
username = "user1"
password = "pass1"
host_url = "https://....."
json_file = "s3://Bucket/FolderName.json"
csv_file = "s3://Bucket/FolderName.csv"
aws_access_key_id = "ACCESSKEYID"
aws_secret_access_key = "SECRETKEY"

#Create a staging table similar to target table
query1 = "CREATE TABLE stage_tablename LIKE target_tablename WHERE target_primarykey = stage_primarykey;"

#Load data from S3 to staging table
query2 ="""COPY stage_tablename FROM 's3://Bucket/FolderName.csv' CREDENTIALS "<aws_access_key_id>;<aws_secret_access_key>" [OPTIONS];"""

#Update the target table using staging table
query3 ="UPDATE target_tablename SET (Column_names) FROM stage_tablename WHERE target_primarykey = stage_primarykey;"

#Delete the matching rows in staging table
query4 ="DELETE FROM stage_tablename USING target_tablename WHERE target_primarykey = stage_primarykey;"

#Insert the remaining rows to target table
query5 ="INSERT INTO target_tablename SELECT (Column_names) FROM stage_tablename;"