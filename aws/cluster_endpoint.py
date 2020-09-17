from create_cluster import *

def main():
    load_DWH_Params()
    redshift =redshift_client()
    ec2 = ec2_client()
    myClusterProps = ClusterProps(redshift)
    cluster_status = show_cluster_status(redshift, myClusterProps)

    if cluster_status:
        write_DWH_params(myClusterProps)
        open_TCP_port(ec2, myClusterProps)

    else:
        print('Waiting')

if __name__ == "__main__":
    main()