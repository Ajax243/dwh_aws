# DATA WAREHOUSE ON AWS REDSHIFT CLUSTER
## Project submission



## 1- Project Description:
This project aims atusing python to load data from an Amazon S3 Bucket into two staging tables and proceeds with inserting these tables into an analytical database following the star schema.

## 2- Packages used:
- configparser
- psycopg2 
- sql_queries 
- boto3 
- json
- pandas
- sys 

## 3- Project files:
- dwh.cfg : contains AWS parameters that will be used to create resources
- create_cluster.py : contains a script that creates the requirements to create a cluster, creates a cluster and opens a connection to the cluster
- sql_queries.py : contains a script that contains all sql queries required to delete the tables, creates them and loads data into them
- create_tables.py: contains a script that connects to the database and runs all the sql queries contained in the sql_queries.py file that is needed for creating the tables
- etl.py: contains a script that loads data from the s3 bucket and inserts it into the previously created tables

## 4- Running the scripts:
- Set ```AWS_ACCESS_KEY_ID``` and ```AWS_SECRET_ACCESS_KEY``` in ```dwh.cfg``` file
- ADD a ```DB_PASSWORD``` in the ```CLUSTER``` and ```DWH``` sections in the ```dwh.cfg``` file
- run ```python create_cluster.py run```
- It will print out an arn_role, copy it into ```ARN``` in the ```IAM ROLE``` section in the ```dwh.cfg``` file
- It will also come back with a message that it couldn't open a connection as the cluster takes a while to be created.
- After the cluster status is ```available``` from the AWS console run ```python create_cluster.py``` again to open tcp connection to the cluster
- In the console, click the cluster name and copy the endpoint name at the top right corner of the page and remove everything starting with the port number until the end so that the last thing is '...amazon.com' and put it in the ```HOST``` in the ```dwh.cfg``` file
- Run ```python create_tables.py```
- Run ```etl.py```
- Run ```python create_cluster delete``` to delete the cluster and IAM role


