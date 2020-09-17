import configparser
import psycopg2
from sql_queries import insert_table_order, insert_table_queries


def insert_tables(cur, conn):
    """
    Insert data from the staging tables to the analytical tables
    """
    i = 0
    for query in insert_table_queries:
        print("Inserting data into {}..".format(insert_table_order[i]))
        cur.execute(query)
        conn.commit()
        i = i + 1
        print("  [Finished]  ")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    insert_tables(cur, conn)
    count_rows(cur, conn)


    conn.close()


if __name__ == "__main__":
    main()
