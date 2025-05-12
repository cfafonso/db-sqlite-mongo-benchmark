

def query_sqlite_db(cur, query):
    """
    Executes a SQL query on a SQLite database and returns all results.

    Args:
        cur (sqlite3.Cursor): SQLite cursor object for executing SQL statements.
        query (str): the SQL query to execute.
    
    Returns:
        list: a list of tuples containing all records that match the query.
    """
    
    cur.execute(query)
    return cur.fetchall()