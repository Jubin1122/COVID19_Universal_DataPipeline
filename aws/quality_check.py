import configparser
import psycopg2
from sql_queries import count_table_order, count_table_queries, table_order, duplcate_queries

def Quality_check(cur, conn):
    """
    Whether insertion is successfull or not
    """
    i = 0
    for query in count_table_queries:
        print(" Analytical Table: {}..".format(count_table_order[i]))
        cur.execute(query)
        results = cur.fetchone()

        for res in results:
            if res <1:
                print("Data quality check failed. {} returned no results".format(count_table_order[i]))
            else:
                print("Rows successfully inserted:", results)
        i = i + 1

def check_duplicate_records(cur, conn):
    """
    Check whether the table contain duplicate records or not
    """
    i = 0
    for query in duplcate_queries:
        print(" Analytical Table: {}..".format(table_order[i]))
        cur.execute(query)
        res = cur.fetchall()

        if res[0]['duplicates'] > 1:
            print("Duplicate records are present")
        else:
            print("No duplicate records found") 
        i = i + 1


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    Quality_check(cur, conn)
    check_duplicate_records(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
