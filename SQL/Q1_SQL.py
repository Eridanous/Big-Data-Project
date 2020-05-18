from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
# from datetime import date, datetime
import time

starttime = time.time()

# datapath = "/Proj/smalldata.csv"
datapath = "/Proj/yellow_tripdata_1m.csv"

sc = SparkContext("spark://master:7077", "SQL_Q1")
sqlContext = SQLContext(sc)

Data = sc.textFile(datapath)

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


trips_df = sqlContext.createDataFrame(row_data)
trips_df.registerTempTable("trips")

# FMT ='%y-%M-%d %H:%m:%s'

# PIO SWSTO ALLA ARGEI PERISSOTERO (synypologizei kai ta seconds)
ans = sqlContext.sql(""" 
    SELECT  int(date_format(start_t,'H')) as HourOfDay, AVG(datediff(end_t,start_t)*24*60 + (date_format(end_t,'H') - date_format(start_t,'H'))*60 +(date_format(end_t,'m') - date_format(start_t,'m')) +(date_format(end_t,'s') - date_format(start_t,'s'))/60.0 ) as AverageTripDurationInMinutes FROM trips GROUP BY HourOfDay ORDER BY HourOfDay ASC
    """)
ans.show(24)

# ans = sqlContext.sql(""" 
#     SELECT  int(date_format(start_t,'H')) as HourOfDay, AVG(datediff(end_t,start_t)*24*60 + (date_format(end_t,'H') - date_format(start_t,'H'))*60 +(date_format(end_t,'m') - date_format(start_t,'m')) ) as AverageTripDurationInMinutes FROM trips GROUP BY HourOfDay ORDER BY HourOfDay ASC
#     """)
# ans.show(24)

# trips_df.printSchema()

print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')


# ans=ans.collect()

# import pandas as pd
# df = pd.DataFrame(ans, columns = ['HourOfDay', 'AverageTripDurationInMinutes']) 
# print(df)
# df.to_csv("./Q1_SQL_out.csv", sep=',',index=False)