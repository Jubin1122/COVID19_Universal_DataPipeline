import pandas as pd 
import boto3
import json
import configparser

KEY                    = ''
SECRET                 = ''

DWH_CLUSTER_TYPE       = ''
DWH_NUM_NODES          = ''
DWH_NODE_TYPE          = ''

DWH_CLUSTER_IDENTIFIER = ''
DWH_DB                 = ''
DWH_DB_USER            = ''
DWH_DB_PASSWORD        = ''
DWH_PORT               = ''

DWH_IAM_ROLE_NAME      = ''

def load_DWH_Params():

    global KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, \
    DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
    DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME

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

def display_DWH_Params():

    global KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, \
    DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, \
    DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME

    x = pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })
 
    return x

def iam_client():

    global KEY, SECRET

    return boto3.client('iam', region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

def s3_client():

    global KEY, SECRETS

    return boto3.resource('s3', region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

def ec2_client():

    global KEY, SECRET

    return boto3.resource('ec2', region_name = "us-west-2",
                            aws_access_key_id = KEY,
                            aws_secret_access_key = SECRET)

def redshift_client():

    global KEY, SECRET

    return boto3.client('redshift', region_name = 'us-west-2',
                            aws_access_key_id = KEY,
                            aws_secret_access_key = SECRET)

def check_s3_bucket(s3):

    sample_bucket = s3.Bucket("awssampledbuswest2")

    for obj in sample_bucket.objects.filter(Prefix = "ssbgz"):
        print(obj)


def create_iam_role(iam):

    """
    Here, I am creating a IAM role that makes Redshift able to access 
    S3 bucket(Read Only)
    """

    global DWH_IAM_ROLE_NAME

    try:
        print("Creating a new IAM Role") 
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
    
    print("Attaching  Policy")
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    return roleArn


def create_cluster(redshift, rolearn):

    global DWH_CLUSTER_IDENTIFIER,DWH_DB_USER, DWH_DB,\
        DWH_DB_PASSWORD, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES

    try:

        response = redshift.create_cluster(
            ClusterType = DWH_CLUSTER_TYPE,
            NodeType = DWH_NODE_TYPE,
            NumberOfNodes = int(DWH_NUM_NODES),

            # Identifiers and credentials
            DBName = DWH_DB,
            ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
            MasterUsername = DWH_DB_USER,
            MasterUserPassword = DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles = [rolearn]
        )
    
    except Exception as e:
        print(e)

def ClusterProps(redshift):

    global DWH_CLUSTER_IDENTIFIER

    return redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

def get_cluster_status(redshift, myClusterProps):

    global DWH_CLUSTER_IDENTIFIER

    status = myClusterProps['ClusterStatus']

    return status

def show_cluster_status(redshift, myClusterProps):

    Clusterstaus = get_cluster_status(redshift, myClusterProps)

    if Clusterstaus == "available":
        return True
    
    else:
        return False


def describe_cluster(myClusterProps):

    # myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in myClusterProps.items() if k in keysToShow]
    
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def write_DWH_params(myClusterProps):
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    config.set("DWH", "DWH_ENDPOINT", myClusterProps['Endpoint']['Address'])
    config.set("IAM_ROLE", "DWH_ROLE_ARN", myClusterProps['IamRoles'][0]['IamRoleArn'])

    with open('dwh.cfg', 'w+') as configfile:
        config.write(configfile)


def open_TCP_port(ec2, myClusterProps):

    global DWH_PORT

    try:
        vpc = ec2.Vpc(id = myClusterProps['VpcId'])
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




def delete_cluster(redshift):
    
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)


def main():
    global s3
    load_DWH_Params()
    dwh_param = display_DWH_Params()
    print(dwh_param)
    iam = iam_client()
    ec2 = ec2_client()
    redshift = redshift_client()
    s3 = s3_client()
    rolearn = create_iam_role(iam)
    create_cluster(redshift, rolearn)
    myClusterProps = ClusterProps(redshift)
    cluster_status = show_cluster_status(redshift, myClusterProps)
    if cluster_status:
        print("Cluster is availaible")
    
    else:
        print("Not availaible")


if __name__ ==  '__main__':

    main()

