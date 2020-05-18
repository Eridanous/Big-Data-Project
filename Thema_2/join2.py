from pyspark import SparkContext
from pyspark.sql import SQLContext
import time

starttime = time.time()

sc = SparkContext("spark://master:7077", "Join2")
sqlContext = SQLContext(sc)

# data_parqDF = sqlContext.read.parquet('/Proj/smalldata.parquet')
data_parqDF = sqlContext.read.parquet('/Proj/data.parquet')

# ven_parqDF = sqlContext.read.parquet('/Proj/smallvendors.parquet')
ven_parqDF= sqlContext.read.parquet('/Proj/vendors.parquet')

data_parqDF.registerTempTable("trips")

ven_parqDF = ven_parqDF.limit(100)
ven_parqDF.registerTempTable("vendorsVSid")

sqlContext.sql("SET spark.sql.autoBroadcastJoinThreshold = -1") 

ans = sqlContext.sql("""
	SELECT * FROM trips
	INNER JOIN vendorsVSid ON trips.id = vendorsVSid.id
	""")


ans.explain()

ans.show(100)
# ans.collect()

print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')