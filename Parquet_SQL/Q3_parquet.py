from pyspark import SparkContext
from pyspark.sql import SQLContext, Row , SparkSession
import time
import pyspark.sql.functions

# from pyspark.sql.session import SparkSession


starttime = time.time()

sc = SparkContext("spark://master:7077", "Q3Parquet")
sqlContext = SQLContext(sc)

# data_parqDF = sqlContext.read.parquet('/Proj/smalldata.parquet')
data_parqDF = sqlContext.read.parquet('/Proj/data.parquet')

# ven_parqDF = sqlContext.read.parquet('/Proj/smallvendors.parquet')
ven_parqDF= sqlContext.read.parquet('/Proj/vendors.parquet')

data_parqDF.registerTempTable("trips")
ven_parqDF.registerTempTable("vendorsVSid")


ans = sqlContext.sql("""
SELECT AverageSpeed, ven AS Vendor ,temp_query.id
FROM(
	SELECT id , 2*atan2( sqrt(a) , sqrt(1-a) ) * 6371 /  (tot_hours) AS AverageSpeed
	FROM (
		SELECT id, tot_hours, pow(sin((radians(start_lat) - radians(end_lat))/2.0),2) + 
			cos(radians(start_lat))*cos(radians(end_lat)) * pow(sin((radians(start_long) - radians(end_long))/2.0) ,2) AS a 
		FROM (
			SELECT *, datediff(end_t,start_t)*24 + (date_format(end_t,'H')-date_format(start_t,'H')) +
				(date_format(end_t,'m')-date_format(start_t,'m'))/60.0 + (date_format(end_t,'s')-date_format(start_t,'s'))/3600.0  
				as tot_hours FROM trips 
			WHERE (start_long>=-74.27 and start_long<=-73.68 and end_long>=-74.27 and end_long<=-73.68 and
				start_lat >= 40.48 and start_lat<=40.93 and end_lat >= 40.48 and end_lat<=40.93 and
				start_t > '2015-03-10' and start_t < end_t)
			)
		WHERE tot_hours>0
		)
	ORDER BY AverageSpeed DESC
	LIMIT 5
	) AS temp_query
INNER JOIN vendorsVSid ON temp_query.id=vendorsVSid.id
ORDER BY AverageSpeed DESC
""")
# '2015-03-10' !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
ans.show()




print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')


# ans=ans.collect()

# import pandas as pd
# df = pd.DataFrame(ans, columns = ['AverageSpeed', 'Vendor','TripID']) 
# # print(df)
# df.to_csv("./Q3_ParquetSQL_out.csv", sep=',',index=False)