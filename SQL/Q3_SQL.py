from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
import time
import pyspark.sql.functions

starttime = time.time()

# datapath = "/Proj/smalldata.csv"
datapath = "/Proj/yellow_tripdata_1m.csv"

# vendorspath = "/Proj/smallvendors.csv"
vendorspath = "/Proj/yellow_tripvendors_1m.csv"

sc = SparkContext("spark://master:7077", "SQL_Q3")
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


# atan2(y, x) = atan(y / x)

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
# INNER JOIN vendorsVSid ON __auto_generated_subquery_name.id=vendorsVSid.id


print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')


# ans=ans.collect()

# import pandas as pd
# df = pd.DataFrame(ans, columns = ['AverageSpeed', 'Vendor','TripID']) 
# # print(df)
# df.to_csv("./Q3_SQL_out.csv", sep=',',index=False)