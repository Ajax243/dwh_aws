import pandas as pd
import boto3
import json
import configparser


#Load DWH Params from a file


config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)


def create_clients():
	ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

	s3 = boto3.resource('s3',
	                       region_name="us-west-2",
	                       aws_access_key_id=KEY,
	                       aws_secret_access_key=SECRET
	                   )

	iam = boto3.client('iam',aws_access_key_id=KEY,
	                     aws_secret_access_key=SECRET,
	                     region_name='us-west-2'
	                  )

	redshift = boto3.client('redshift',
	                       region_name="us-west-2",
	                       aws_access_key_id=KEY,
	                       aws_secret_access_key=SECRET
	                       )
	return ec2,s3, iam, redshift

def create_iam_role():
	try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
	except Exception as e:
	    print(e)

	iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
	                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
	                      )['ResponseMetadata']['HTTPStatusCode']


	roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
	return roleArn

def create_cluster():
	try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
	except Exception as e:
	    print(e)

def open_access():

	try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
	except Exception as e:
	    print(e)


def delete_resources():
	# Delete Redshift Cluster
	redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
	
	# Delete IAM role

	iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
	iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)



def main():



	ec2, s3, iam, redshift= create_clients

	role_arn=create_iam_role()

	create_cluster()
	for i in range(60):
            cluster = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_ID)['Clusters'][0]
            if cluster['ClusterStatus'] == 'available':
                break
    try: 
    	open_access()
    except Exception as e:
	    print(e)
if__name__=='__main__':
main()