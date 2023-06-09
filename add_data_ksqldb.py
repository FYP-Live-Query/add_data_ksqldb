from ksql import KSQLAPI
from datetime import datetime
import time
from random import randrange
class MyClass:
    def __init__(self, ksql_url):
        self.api_client = KSQLAPI(ksql_url)
        
    def insert(self, stream_name, rows):

        values = []
        for row in rows:
            row['ID'] = randrange(50000)
                # Add the current time as the eventTimestamp attribute
            row["eventTimestamp"] = f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
                # Add the values of each row to the list
            values.append(f"({', '.join(str(value) for value in row.values())})")
            # Construct the SQL query with all the values
        query = f"INSERT INTO {stream_name} ({', '.join(rows[0].keys())}) VALUES {', '.join(values)};"
        print(query)
            # Execute the SQL query
        try:
            self.api_client.ksql(query)
        except Exception as e:
            pass
        return None

            
            # for row in rows:
            #     # Add the current time as the eventTimestamp attribute
            #     row["eventTimestamp"] = f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
            #     # Construct the SQL query
            #     values = f"({', '.join(str(value) for value in row.values())})"
            #     query = f"INSERT INTO {stream_name} ({', '.join(rows[0].keys())}) VALUES {values};"
            #     print(query)
            #     # Execute the SQL query
            #     try:
            #         self.api_client.ksql(query)
            #     except Exception as e:
            #         pass
            # return None
    #for table
rows =[
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr"},
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr"}]

#for stream networkTraffic
# rows = [
#     {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
#     {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"}
# ]


my_object = MyClass(ksql_url='http://20.171.111.32:8088')
while True:
    response = my_object.insert(stream_name='network', rows=rows)
    time.sleep(60)





