from ksql import KSQLAPI
from datetime import datetime
import csv
from random import randrange
import time
class MyClass:
    def __init__(self, ksql_url):
        self.api_client = KSQLAPI(ksql_url)
       
    def insert(self, stream_name):
        unique_id = 4000000
        while True: 
            values = [unique_id, "\'Baby Food1\'", "\'H\'", 10, 1500, 1000, 500, "\'Australia and Oceania\'", "\'Tuvalu\'" ]
            # row["eventtimestamp"] = f"'{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
            # Construct the SQL query
            values = f"({', '.join(str(value) for value in values)})"
            query = f"INSERT INTO {stream_name} (orderID, itemType, orderPriority, unitsSold , totalRevenue, totalCost, totalProfit, region , country) VALUES {values};"
            unique_id-=1
            print(query)
            time.sleep(1)

            # Execute the SQL query
            try:
                c = self.api_client.ksql(query)
            except Exception as e:
                pass

my_object = MyClass(ksql_url='http://172.174.71.151:8088')
response = my_object.insert(stream_name='orderTable')




