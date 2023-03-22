from ksql import KSQLAPI
from ksql.errors import KSQLAPIError

class MyClass:
    def __init__(self, ksql_url):
        self.api_client = KSQLAPI(ksql_url)

    def insert_into_stream(self, stream_name, rows):
        try:
            # Construct the SQL query
            values = ', '.join(f"({', '.join(str(value) for value in row.values())})" for row in rows)
            query = f"INSERT INTO {stream_name} ({', '.join(rows[0].keys())}) VALUES {values}"

            # Execute the SQL query
            response = self.api_client.ksql(query)
            return response

        except KSQLAPIError as e:
            print(f"Error inserting data into stream: {e}")
            return None


rows = [
    {"PROFILEID": 'm2309eec', "LATITUDE": 37.7877, "LONGITUDE": -122.4011},
    {"PROFILEID": 'k2309eec', "LATITUDE": 37.7877, "LONGITUDE": -122.4011}
]

my_object = MyClass(ksql_url='http://localhost:8088')
response = my_object.insert_into_stream(stream_name='riderLocations', rows=rows)
