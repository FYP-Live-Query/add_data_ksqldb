from ksql import KSQLAPI

class MyClass:
    def __init__(self, ksql_url):
        self.api_client = KSQLAPI(ksql_url)

    def insert_into_stream(self, stream_name, rows):
        
        for row in rows:
        # Construct the SQL query
            values = f"({', '.join(str(value) for value in row.values())})"
            query = f"INSERT INTO {stream_name} ({', '.join(rows[0].keys())}) VALUES {values};"
            print(query)
            # Execute the SQL query
            try:
                self.api_client.ksql(query)
            except Exception as e:
                pass
        return None
#for table
rows = [
    {"ID":1,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
    {"ID":2,"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"}
]

#for stream networkTraffic
# rows = [
#     {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"},
#     {"IP": "\'108.23.85.jfd\'", "DATE": "\'2017-06-30\'", "TIME": "\'00:00:00\'", "BROWSER": "\'chr\'"}
# ]


my_object = MyClass(ksql_url='http://10.8.100.246:8088')
response = my_object.insert_into_stream(stream_name='network', rows=rows)





