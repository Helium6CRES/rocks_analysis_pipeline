import psycopg2
from psycopg2 import Error
import pandas as pd
import typing
import pandas.io.sql as psql

# Simplify to not have an insert capability. 
def he6cres_db_query(query: str ) -> typing.Union[None, pd.DataFrame]: 
    
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                      password="chirality",
                                      host="10.66.192.47",
                                      port="5432",
                                      database="he6cres_db")

        # Create a cursor to perform database operations
        cursor = connection.cursor()

        # Execute a sql_command
        cursor.execute(query)
        cols = [desc[0] for desc in cursor.description]
        query_result = pd.DataFrame(cursor.fetchall(), columns=cols)

        
    except (Exception, Error) as error:
        print("Error while connecting to he6cres_db", error)
        query_result = None
        
    finally:
        if connection:
            cursor.close()
            connection.close()

    return query_result
