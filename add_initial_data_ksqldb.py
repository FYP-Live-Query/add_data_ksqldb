from ksql import KSQLAPI
from datetime import datetime
import csv
from random import randrange
class MyClass:
    def __init__(self, ksql_url):
        self.api_client = KSQLAPI(ksql_url)
    
    
    
    def insert(self, stream_name, rows):
        
        browsers = ['chr', 'fox', 'mie', 'opr', 'saf']
    #     values = []
    #     for row in rows:
    #         # Add the current time as the eventTimestamp attribute
    #         row["eventTimestamp"] = f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
    #         # Add the values of each row to the list
    #         values.append(f"({', '.join(str(value) for value in row.values())})")
    #     # Construct the SQL query with all the values
    #     query = f"INSERT INTO {stream_name} ({', '.join(rows[0].keys())}) VALUES {', '.join(values)};"
    #     print(query)
    #     # Execute the SQL query
    #     try:
    #         self.api_client.ksql(query)
    #     except Exception as e:
    #         pass
    #     return None
        unique_id = 1000000
        with open('/home/nuvidu/fyp/ksqlDB/add_data_ksqldb/log20170630.csv', 'r') as file:
            reader = csv.reader(file)
            next(reader)
            
            for row in reader:
                values = [unique_id, "'" + str(row[0]) + "'", "'" + row[1] + "'", "'" + row[2] + "'",  "'" +browsers[randrange(4)] + "'" , "'" + datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + "'"]
                print(values)
                # row["eventtimestamp"] = f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
                # Construct the SQL query
                values = f"({', '.join(str(value) for value in values)})"
                query = f"INSERT INTO {stream_name} (id, ip, date, time, browser, eventTimestamp) VALUES {values};"
                unique_id-=1
                print(query)
                # Execute the SQL query
                try:
                    c = self.api_client.ksql(query)
                except Exception as e:
                    pass
        return None
    #for table
rows = [
    {"ID":1,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":2,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":3,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":4,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":5,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":6,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":7,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":8,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":9,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":10,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":11,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":12,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":13,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":14,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":15,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":16,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":17,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":18,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":19,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":20,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":31,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":32,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":33,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":34,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":35,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":36,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":37,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":38,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":39,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":40,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":51,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":52,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":53,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":54,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":55,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":56,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":57,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":58,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":59,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":60,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":61,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":62,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":63,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":64,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":65,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":66,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":67,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":68,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":69,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":70,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":71,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":72,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":73,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":74,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":75,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":76,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":77,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":78,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":79,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":80,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":81,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":82,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":83,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":84,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":85,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":86,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":87,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":88,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":89,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":90,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":91,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":92,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":93,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":94,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":95,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":96,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":97,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":98,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":99,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":100,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"}
]

#for stream networkTraffic
# rows = [
#     {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
#     {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"}
# ]


my_object = MyClass(ksql_url='http://20.125.141.28:8088')
response = my_object.insert(stream_name='network', rows=rows)





