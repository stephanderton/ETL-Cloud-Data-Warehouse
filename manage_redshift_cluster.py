import configparser
import psycopg2
import pandas as pd
import boto3
import json
import time

# ---------------------------------------------------------------------------------
# Mange the Redshift Cluster using the AWS python SDK 
# ---------------------------------------------------------------------------------


def load_config():
    """
    Load DWH Cluster and DB Params from config file

    """

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    DWH_CLUSTER_TYPE       = config['DWH']['DWH_CLUSTER_TYPE']
    DWH_NUM_NODES          = config['DWH']['DWH_NUM_NODES']
    DWH_NODE_TYPE          = config['DWH']['DWH_NODE_TYPE']
    DWH_CLUSTER_IDENTIFIER = config['DWH']['DWH_CLUSTER_IDENTIFIER']
    DWH_REGION             = config['DWH']['DWH_REGION']

    HOST                   = config['CLUSTER']['HOST']
    DB_NAME                = config['CLUSTER']['DB_NAME']
    DB_USER                = config['CLUSTER']['DB_USER']
    DB_PASSWORD            = config['CLUSTER']['DB_PASSWORD']
    DB_PORT                = config['CLUSTER']['DB_PORT']

    IAM_ROLE_NAME          = config['IAM_ROLE']['IAM_ROLE_NAME']
    IAM_POLICY_ARN         = config['IAM_ROLE']['IAM_POLICY_ARN']
    ARN                    = config['IAM_ROLE']['ARN']

    return DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, \
           DWH_CLUSTER_IDENTIFIER, DWH_REGION, \
           HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, \
           IAM_ROLE_NAME, IAM_POLICY_ARN, ARN


DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, \
DWH_CLUSTER_IDENTIFIER, DWH_REGION, \
HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, \
IAM_ROLE_NAME, IAM_POLICY_ARN, ARN  \
    = load_config()


def print_config():
    """
    """
    db_config = pd.DataFrame({"Param":
                    ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE",
                    "DWH_CLUSTER_IDENTIFIER", "DWH_REGION",
                    "HOST", "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_PORT",
                    "IAM_ROLE_NAME", "IAM_POLICY_ARN", "ARN"
                    ],
                "Value":
                    [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, 
                    DWH_CLUSTER_IDENTIFIER, DWH_REGION,
                    HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, 
                    IAM_ROLE_NAME, IAM_POLICY_ARN, ARN
                    ]
                })
    print(db_config)


#------------------------------------------------------------------------------
# Load AWS Params from separate config file

config_aws = configparser.ConfigParser()
config_aws.read_file(open('aws.cfg'))

AWS_KEY     = config_aws['AWS']['KEY']
AWS_SECRET  = config_aws['AWS']['SECRET']


# Create clients for IAM and Redshift

iam = boto3.client('iam',
                    region_name           = DWH_REGION,
                    aws_access_key_id     = AWS_KEY,
                    aws_secret_access_key = AWS_SECRET
                   )

redshift = boto3.client('redshift',
                         region_name           = DWH_REGION,
                         aws_access_key_id     = AWS_KEY,
                         aws_secret_access_key = AWS_SECRET
                       )


#------------------------------------------------------------------------------
# Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)

try:
    print('1.1 Creating a new IAM Role')
    dwhRole = iam.create_role(
        Path                     = '/',
        RoleName                 = IAM_ROLE_NAME,
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
iam.attach_role_policy(RoleName = IAM_ROLE_NAME,
                       PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']


print('1.3 Get the IAM role ARN')
roleArn = iam.get_role(RoleName = IAM_ROLE_NAME)['Role']['Arn']

print(roleArn)


#------------------------------------------------------------------------------
# Create a RedShift Cluster

try:
    response = redshift.create_cluster(        
        # parameters for hardware
        ClusterType        = DWH_CLUSTER_TYPE,
        NodeType           = DWH_NODE_TYPE,
        NumberOfNodes      = int(DWH_NUM_NODES),
        
        # parameters for identifiers & credentials
        DBName             = DB_NAME,
        ClusterIdentifier  = DWH_CLUSTER_IDENTIFIER,
        MasterUsername     = DB_USER,
        MasterUserPassword = DB_PASSWORD,

        #  parameter for role (to allow s3 access)
        IamRoles           = [roleArn]
    )
except Exception as e:
    print(e)


#------------------------------------------------------------------------------
# Describe the cluster to see its status

def pretty_Redshift_props(props):
    """
    """
        
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", 
                  "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data = x, columns = ["Key", "Value"])


myClusterProps = redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
pretty_Redshift_props(myClusterProps)

#------------------------------------------------------------------------------
# WAIT until the cluster is running

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
pretty_Redshift_props(myClusterProps)


#------------------------------------------------------------------------------
# Write the cluster endpoint and role ARN to the config file

if bClusterAvailable:
    HOST = myClusterProps['Endpoint']['Address']
    ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("HOST :: ", HOST)
    print("ARN  :: ", ARN)

    config['CLUSTER']['HOST'] = HOST
    config['IAM_ROLE']['ARN'] = "'{}'".format(ARN)


#------------------------------------------------------------------------------
# Open an incoming  TCP port to access the cluster endpoint

try:
    vpc = ec2.Vpc(id = myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    defaultSg.authorize_ingress(
        GroupName  = defaultSg.group_name,  # TODO: fill out
        CidrIp     = '0.0.0.0/0',  # TODO: fill out
        IpProtocol = 'TCP',  # TODO: fill out
        FromPort   = int(DB_PORT),
        ToPort     = int(DB_PORT)
    )
except Exception as e:
    print(e)


#------------------------------------------------------------------------------
# Make sure you can connect to the cluster

try:
    conn_string = "host={} dbname={} user={} password={} port={}"
    conn_string = conn_string.format(*config['CLUSTER'].values())
    conn = psycopg2.connect( conn_string )
    cur = conn.cursor()
    print(conn_string)

except Exception as e:
    print(e)


#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# Clean up your resources

redshift.delete_cluster( ClusterIdentifier = DWH_CLUSTER_IDENTIFIER, 
                         SkipFinalClusterSnapshot = True )


myClusterProps = redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
pretty_Redshift_props(myClusterProps)

# ### WAIT...keep checking until the cluster is deleted

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
    iam.detach_role_policy(RoleName = IAM_ROLE_NAME, 
                           PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName = IAM_ROLE_NAME)


