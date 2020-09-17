import configparser
import psycopg2
from sql_queries import count_table_order, count_table_queries

def analyze_tables(cur, conn):
    """
    Count the total number of records in the analytical tables
    """
    i = 0
    for query in count_table_queries:
        print(" Analytical Table: {}..".format(count_table_order[i]))
        cur.execute(query)
        results = cur.fetchone()

        for res in results:
            print("   ", res)
        i = i + 1
        print("  [Finished]  ")
        
        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    analyze_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()