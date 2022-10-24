import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, drop_table_queries, create_table_queries, create_schemas_queries, drop_schemas_queries

# use drop query tables from sql_queries.py file
def drop_staging_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# use create query tables from sql_queries.py file
def create_staging_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# use copy query tables from sql_queries.py file to load S3 json files
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

# use insert query tables from sql_queries.py file to load analytics tables 
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()



def create_schemas(cur, conn):
    '''
    Function to create schemas. This function uses the variable 'create_schemas_queries' defined in the 'sql_queries.py' file.
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in create_schemas_queries:
        cur.execute(query)
        conn.commit()        

def drop_schemas(cur, conn):
    '''
    Function to drop schemas. This function uses the variable 'drop_schemas_queries' defined in the 'sql_queries.py' file.
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    '''
    for query in drop_schemas_queries:
        cur.execute(query)
        conn.commit()        


def main():
    # you get file properties that has all configuracion keys to AWS and database
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # you create database conection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # you drop and create staging tables to load all S3 files
    drop_staging_tables(cur, conn)
    create_staging_tables(cur, conn)
    load_staging_tables(cur, conn)

    # you made insert data into analytics tables from staging tables 
    insert_tables(cur, conn)

    # close database connection
    conn.close()


if __name__ == "__main__":
    main()