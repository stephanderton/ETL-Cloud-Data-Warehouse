import configparser
import psycopg2
import pandas as pd
import boto3
import json
import time

# ---------------------------------------------------------------------------------
# Mange the Redshift Cluster using the AWS python SDK 
# ---------------------------------------------------------------------------------

# # STEP 0: Make sure you have an AWS secret and access key
# 
# - Create a new IAM user in your AWS account
# - Give it `AdministratorAccess`, From `Attach existing policies directly` Tab
# - Take note of the access key and secret 
# - Edit the file `dwh.cfg` in the same folder as this notebook and fill
# <font color='red'>
# <BR>
# [AWS]<BR>
# KEY= YOUR_AWS_KEY<BR>
# SECRET= YOUR_AWS_SECRET<BR>
# <font/>
# 
# # Load DWH Params from a file

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

# My own ADD
DWH_REGION             = config.get("DWH","DWH_REGION")


(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE",
                   "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", 
                   "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME",
                   "DWH_REGION"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, 
                   DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, 
                   DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME,
                   DWH_REGION]
             })

# ## Create clients for EC2, S3, IAM, and Redshift

ec2 = boto3.resource('ec2',
                      region_name           = DWH_REGION,
                      aws_access_key_id     = KEY,
                      aws_secret_access_key = SECRET
                     )

s3 = boto3.resource('s3',
                     region_name            = DWH_REGION,
                     aws_access_key_id      = KEY,
                     aws_secret_access_key  = SECRET
                    )

iam = boto3.client('iam',
                    region_name           = DWH_REGION,
                    aws_access_key_id     = KEY,
                    aws_secret_access_key = SECRET
                   )

redshift = boto3.client('redshift',
                        region_name           = DWH_REGION,
                        aws_access_key_id     = KEY,
                        aws_secret_access_key = SECRET
                       )

# ## STEP 1: IAM ROLE
# - Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

try:
    print('1.1 Creating a new IAM Role')
    dwhRole = iam.create_role(
        Path                     = '/',
        RoleName                 = DWH_IAM_ROLE_NAME,
        Description              = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument = json.dumps(
            {'Statement' : [{'Action'    : 'sts:AssumeRole',
                             'Effect'    : 'Allow',
                             'Principal' : {'Service': 'redshift.amazonaws.com'}
                            }],
             'Version'   : '2012-10-17'
            }
        )
    )
except Exception as e:
    print(e)


print('1.2 Attaching Policy')
iam.attach_role_policy(RoleName = DWH_IAM_ROLE_NAME,
                       PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']


print('1.3 Get the IAM role ARN')
roleArn = iam.get_role(RoleName = DWH_IAM_ROLE_NAME)['Role']['Arn']

print(roleArn)

# ## STEP 2:  Redshift Cluster
# 
# - Create a RedShift Cluster

try:
    response = redshift.create_cluster(        
        # TODO: add parameters for hardware
        ClusterType        = DWH_CLUSTER_TYPE,
        NodeType           = DWH_NODE_TYPE,
        NumberOfNodes      = int(DWH_NUM_NODES),
        
        # TODO: add parameters for identifiers & credentials
        DBName             = DWH_DB,
        ClusterIdentifier  = DWH_CLUSTER_IDENTIFIER,
        MasterUsername     = DWH_DB_USER,
        MasterUserPassword = DWH_DB_PASSWORD,

        # TODO: add parameter for role (to allow s3 access)
        IamRoles           = [roleArn]
    )
except Exception as e:
    print(e)

# ## 2.1 *Describe* the cluster to see its status
# - run this block several times until the cluster status becomes `Available`

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", 
                  "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data = x, columns = ["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)

# ### WAIT until the cluster is running

bClusterAvailable = False
nMinutes = 0


while True:
    time.sleep(60)
    nMinutes += 1
    print("waiting {} minutes...".format(nMinutes))

    # then check the status
    myClusterProps = redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    if  myClusterProps['ClusterStatus'] == 'available':
        bClusterAvailable = True
        break
    
    if nMinutes >= 7:
        print("7 minute time limit reached")
        break
    
# cluster should be available now, OR the 7 minute limit has passed
prettyRedshiftProps(myClusterProps)

# 2.2 Write the cluster endpoint and role ARN to the config file

if bClusterAvailable:
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)

    config['CLUSTER']['HOST'] = DWH_ENDPOINT
    config['IAM_ROLE']['ARN'] = "'{}'".format(DWH_ROLE_ARN)


# ## STEP 3: Open an incoming  TCP port to access the cluster endpoint

try:
    vpc = ec2.Vpc(id = myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    defaultSg.authorize_ingress(
        GroupName  = defaultSg.group_name,  # TODO: fill out
        CidrIp     = '0.0.0.0/0',  # TODO: fill out
        IpProtocol = 'TCP',  # TODO: fill out
        FromPort   = int(DWH_PORT),
        ToPort     = int(DWH_PORT)
    )
except Exception as e:
    print(e)


# ## STEP 4: Make sure you can connect to the cluster

try:
    conn_string = "host={} dbname={} user={} password={} port={}"
    conn_string = conn_string.format(*config['CLUSTER'].values())
    conn = psycopg2.connect( conn_string )
    cur = conn.cursor()
    print(conn_string)

except Exception as e:
    print(e)


# ## STEP 5: Clean up your resources

redshift.delete_cluster( ClusterIdentifier = DWH_CLUSTER_IDENTIFIER, 
                         SkipFinalClusterSnapshot = True )


myClusterProps = redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)

# ### WAIT...keep checking until the cluster is deleted

import time
bClusterDeleted = False
nMinutes = 0

try:
    while True:
        time.sleep(60)
        nMinutes += 1
        print("waiting... {} minutes".format(nMinutes))

        # then check the status
        myClusterProps = redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

        if nMinutes >= 10:
            print("10 minute time limit reached. Cluster may not have been deleted yet.")
            break

        if  myClusterProps['ClusterStatus'] == 'deleting':
            continue

except Exception as e:
    bClusterDeleted = True
    print(e)
    print("Meaning the Cluster was successfully deleted.")


if bClusterDeleted:
    iam.detach_role_policy(RoleName = DWH_IAM_ROLE_NAME, 
                           PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName = DWH_IAM_ROLE_NAME)


