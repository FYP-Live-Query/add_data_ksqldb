from ksql import KSQLAPI
from datetime import datetime
import csv
from random import randrange
class MyClass:
    def __init__(self, ksql_url):
        self.api_client = KSQLAPI(ksql_url)
    
    
    
    def insert(self, stream_name, rows):
        
        browsers = ['chr', 'fox', 'mie', 'opr', 'saf']
        values = []
        unique_id = 10000000
        while True:
            for row in rows:
                # Add the current time as the eventTimestamp attribute
                row["ID"] = unique_id
                row["eventTimestamp"] = f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
                # Add the values of each row to the list
                values.append(f"({', '.join(str(value) for value in row.values())})")
                unique_id-=1
            # Construct the SQL query with all the values
            query = f"INSERT INTO {stream_name} ({', '.join(rows[0].keys())}) VALUES {', '.join(values)};"
            print(query)
            # Execute the SQL query
            try:
                self.api_client.ksql(query)
            except Exception as e:
                pass
        return None
        # unique_id = 1000000
        # with open('/home/nuvidu/fyp/ksqlDB/add_data_ksqldb/log20170630.csv', 'r') as file:
        #     reader = csv.reader(file)
        #     next(reader)
            
        #     for row in reader:
        #         values = [unique_id, "'" + str(row[0]) + "'", "'" + row[1] + "'", "'" + row[2] + "'",  "'" +browsers[randrange(4)] + "'" , "'" + datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + "'"]
        #         print(values)
        #         # row["eventtimestamp"] = f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
        #         # Construct the SQL query
        #         values = f"({', '.join(str(value) for value in values)})"
        #         query = f"INSERT INTO {stream_name} (id, ip, date, time, browser, eventTimestamp) VALUES {values};"
        #         unique_id-=1
        #         print(query)
        #         # Execute the SQL query
        #         try:
        #             c = self.api_client.ksql(query)
        #         except Exception as e:
        #             pass
        return None
    #for table
rows = [
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
    {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"}
]

#for stream networkTraffic
# rows = [
#     {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
#     {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"}
# ]


my_object = MyClass(ksql_url='http://20.125.141.28:8088')
response = my_object.insert(stream_name='network', rows=rows)





