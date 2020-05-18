from pyspark import SparkContext
from pyspark.sql import SQLContext, Row , SparkSession
import time
# from pyspark.sql.session import SparkSession


starttime = time.time()

sc = SparkContext("spark://master:7077", "Q2Parquet")
sqlContext = SQLContext(sc)

# data_parqDF = sqlContext.read.parquet('/Proj/smalldata.parquet')
data_parqDF = sqlContext.read.parquet('/Proj/data.parquet')

# ven_parqDF = sqlContext.read.parquet('/Proj/smallvendors.parquet')
ven_parqDF= sqlContext.read.parquet('/Proj/vendors.parquet')

data_parqDF.registerTempTable("trips")
ven_parqDF.registerTempTable("vendorsVSid")

ans = sqlContext.sql(""" 
    SELECT max(price) as MaxPrice, ven as Vendor FROM(
    	SELECT trips.price, vendorsVSid.ven FROM trips 
    	INNER JOIN vendorsVSid ON trips.id=vendorsVSid.id )
    GROUP BY ven  
    ORDER BY MaxPrice DESC  
    """)
ans.show(24)

print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')


# ans=ans.collect()

# import pandas as pd
# df = pd.DataFrame(ans, columns = ['MaxPrice', 'Vendor']) 
# # print(df)
# df.to_csv("./Q2_ParquetSQL_out.csv", sep=',',index=False)