from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
# from datetime import date, datetime
import time

starttime = time.time()

# datapath = "/Proj/smalldata.csv"
datapath = "/Proj/yellow_tripdata_1m.csv"

# vendorspath = "/Proj/smallvendors.csv"
vendorspath = "/Proj/yellow_tripvendors_1m.csv"

sc = SparkContext("spark://master:7077", "SQL_Q2")
sqlContext = SQLContext(sc)

Data = sc.textFile(datapath)
Vendors = sc.textFile(vendorspath)

csv_data = Data.map(lambda l: l.split(","))
row_data = csv_data.map(lambda p: Row(
    id=int(p[0]), 
    start_t=p[1],
    end_t=p[2],
    start_long=float(p[3]), # mhkos
    start_lat=float(p[4]), # platos
    end_long=float(p[5]),
    end_lat=float(p[6]),
    price = float(p[7])
    )
)

csv_vendors = Vendors.map(lambda l: l.split(","))
row_vendors = csv_vendors.map(lambda p: Row(
    id=int(p[0]), 
    ven =int(p[1])
    )
)

trips_df = sqlContext.createDataFrame(row_data)
trips_df.registerTempTable("trips")

ven_df = sqlContext.createDataFrame(row_vendors)
ven_df.registerTempTable("vendorsVSid")

ans = sqlContext.sql(""" 
	SELECT max(price) as MaxPrice, ven as Vendor FROM(
    	SELECT trips.price, vendorsVSid.ven FROM trips 
    	INNER JOIN vendorsVSid ON trips.id=vendorsVSid.id )
    GROUP BY ven  
    ORDER BY MaxPrice DESC  
    """)
ans.show()

# print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')


# ans=ans.collect()

# import pandas as pd
# df = pd.DataFrame(ans, columns = ['MaxPrice', 'Vendor']) 
# # print(df)
# df.to_csv("./Q2_SQL_out.csv", sep=',',index=False)