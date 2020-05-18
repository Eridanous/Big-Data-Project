from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time

starttime = time.time()

# datapath = "/Proj/smalldata.csv"
datapath = "/Proj/yellow_tripdata_1m.csv"

# vendorspath = "/Proj/smallvendors.csv"
vendorspath = "/Proj/yellow_tripvendors_1m.csv"

sc = SparkContext("spark://master:7077", "Parquet_Write")
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

# http://blogs.quovantis.com/how-to-convert-csv-to-parquet-files/

# hadoop fs -ls hdfs://master:9000/Proj/
t0 = time.time()
# trips_df.write.parquet('/Proj/smalldata.parquet')
trips_df.write.parquet('/Proj/data.parquet')
print('')
print('Data Write Time: ' +str(time.time() - t0) +' sec')
print('')

t1 = time.time()
# ven_df.write.parquet('/Proj/smallvendors.parquet')
ven_df.write.parquet('/Proj/vendors.parquet')
print('')
print('Vendors Write Time: ' +str(time.time() - t1) +' sec')
print('')

print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')
