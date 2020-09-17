from create_cluster import *

def main():
    load_DWH_Params()
    redshift = redshift_client()
    myClusterProps = ClusterProps(redshift)
    cluster_status = show_cluster_status(redshift, myClusterProps)

    if cluster_status:
        delete_cluster(redshift)
    
    else:
        print("Cluster Not availaible; Unable to delete Cluster")

if __name__ == "__main__":

    main()
