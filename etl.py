import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    To read and load JSON files from S3 into staging_events and staging_songs tables.
    Parameters:
        cur: cursor.
        conn: connection.
    Returns:
        staging_events_table and staging_songs_table tables with filled data.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    To load data from staging_events and staging_songs tables into songplay, song, users, artist, time tables.
    Parameters:
        cur: cursor.
        conn: connection.
    Returns:
        songplay_table, song_table, user_table, artist_table, time_table tables with filled data.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        cur.execute("""
        select d.query, substring(d.filename,14,20), 
        d.line_number as line, 
        substring(d.value,1,16) as value,
        substring(le.err_reason,1,48) as err_reason
        from stl_loaderror_detail d, stl_load_errors le
        where d.query = le.query
        and d.query = pg_last_copy_id(); 
        """)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()