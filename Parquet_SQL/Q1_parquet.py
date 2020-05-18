from pyspark import SparkContext
from pyspark.sql import SQLContext, Row , SparkSession
import time
# from pyspark.sql.session import SparkSession


starttime = time.time()

sc = SparkContext("spark://master:7077", "Q1Parquet")
sqlContext = SQLContext(sc)

# data_parqDF = sqlContext.read.parquet('/Proj/smalldata.parquet')
data_parqDF = sqlContext.read.parquet('/Proj/data.parquet')

data_parqDF.registerTempTable("trips")

ans = sqlContext.sql(""" 
    SELECT  int(date_format(start_t,'H')) as HourOfDay, 
    AVG(datediff(end_t,start_t)*24*60 + (date_format(end_t,'H') - date_format(start_t,'H'))*60
     +(date_format(end_t,'m') - date_format(start_t,'m')) +(date_format(end_t,'s') - date_format(start_t,'s'))/60 ) 
     as AverageTripDurationInMinutes 
     FROM trips GROUP BY HourOfDay ORDER BY HourOfDay ASC
    """)
ans.show(24)

print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')


# ans=ans.collect()

# import pandas as pd
# df = pd.DataFrame(ans, columns = ['HourOfDay', 'AverageTripDurationInMinutes']) 
# # print(df)
# df.to_csv("./Q1_ParquetSQL_out.csv", sep=',',index=False)