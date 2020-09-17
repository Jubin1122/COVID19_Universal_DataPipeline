from create_cluster import *

def main():
    load_DWH_Params()
    redshift = redshift_client()
    myClusterProps = ClusterProps(redshift)
    cluster_status = show_cluster_status(redshift, myClusterProps)

    if cluster_status:
        val = describe_cluster(myClusterProps)
        print(val)
    
    else:
        print("Cluster Not availaible")

if __name__ == "__main__":

    main()
