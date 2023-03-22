from ksql import KSQLAPI

class MyClass:
    def __init__(self, ksql_url):
        self.api_client = KSQLAPI(ksql_url)

    def insert_into_stream(self, stream_name, rows):
        
        for row in rows:
        # Construct the SQL query
            values = f"({', '.join(str(value) for value in row.values())})"
            print(values)
            query = f"INSERT INTO {stream_name} ({', '.join(rows[0].keys())}) VALUES {values};"
            print(query)
            # Execute the SQL query
            try:
                self.api_client.ksql(query)
            except Exception as e:
                pass
        return None

rows = [
    {"PROFILEID": "\'m2309eec\'", "LATITUDE": 37.7877, "LONGITUDE": -122.4011},
    {"PROFILEID": "\'k2309eec\'", "LATITUDE": 37.7877, "LONGITUDE": -122.4011},
    {"PROFILEID": "\'k2309eec\'", "LATITUDE": 37.7877, "LONGITUDE": -122.4011}
]

my_object = MyClass(ksql_url='http://10.8.100.246:8088')
response = my_object.insert_into_stream(stream_name='riderLocations', rows=rows)

