from pyspark import SparkContext
from datetime import date, datetime
import pandas as pd
import numpy as np
import math
import time


starttime = time.time()

# datapath = "/Proj/smalldata.csv"
datapath = "/Proj/yellow_tripdata_1m.csv"

# vendorspath = "/Proj/smallvendors.csv"
vendorspath = "/Proj/yellow_tripvendors_1m.csv"

sc = SparkContext("spark://master:7077", "Q3")

Data = sc.textFile(datapath)
Vendors = sc.textFile(vendorspath)

data = Data.flatMap(lambda x: x.split('\n')) \
.map(lambda y:y.split(','))

vendors = Vendors.flatMap(lambda x: x.split('\n')) \
.map(lambda y:y.split(','))

# data= data.collect()
# print(data[0])

def f(row):
	tripID = row[0] 
	start, end = row[1], row[2]
	FMT ='%Y-%m-%d %H:%M:%S'
	
	if start >'2015-03-10' and start<end: # we care about trips after 10 March 2015
		tdelta = datetime.strptime(end, FMT) - datetime.strptime(start, FMT)
		total_hours = (tdelta.total_seconds()/3600.0)

		# Distance
		long1, lat1 = float(row[3]) , float(row[4])
		long2, lat2 = float(row[5]) , float(row[6])
		
		OK = False
		if long1 !=0 and long2 !=0 and lat1 !=0 and lat2 !=0:
			# if long1 >=-180.0 and long1 <=180.0 and long2 >=-180.0 and long2 <=180.0:
			# 	if lat1 >=-90.0 and lat1 <=90.0 and lat2 >=-90.0 and lat2 <=90.0:
			if long1 >=-74.27 and long1 <=-73.68 and long2 >=-74.27 and long2 <=-73.68: #New York Coordinates
				if lat1 >=40.48 and lat1 <=40.93 and lat2 >=40.48 and lat2 <=40.93:
					if total_hours >0:
						OK = True
		if OK ==True:
			phi1 = math.radians(lat1)
			phi2 = math.radians(lat2)
			lambda1 = math.radians(long1)
			lambda2 = math.radians(long2)

			Dphi =  phi2 - phi1 #math.radians(lat2-lat1)
			Dlambda = lambda2 - lambda1 #math.radians(long2-long1)

			a = np.sin(Dphi/2.0)**2 + np.cos(phi2) * np.cos(phi1) * np.sin(Dlambda/2.0)**2
			c = 2* math.atan2( np.sqrt(a), np.sqrt(1-a) )
			R = 6371 #km
			d = R*c

			speed = d/total_hours # average speed (km/hour)
			return(speed, tripID)
		else:
			return(0,None)			
	else:return(0,None)



speedslist = data.map(f).top(5, key = lambda x: x[0]) # we want the top 5 speeds
speeds= dict() # key = tripID, value = average speed
for i in speedslist:
	tripID = i[1]
	speeds[tripID] = i[0]


speeds = sc.broadcast(speeds)  # we broadcast the top 5 speeds to every node
speeds=speeds.value # broadcast_object.value == the data of the object
# print(speeds)
# print()


def findVendors(pair):
	global speeds, res

	tripID = pair[0]
	vendor = pair[1]
	if tripID in speeds:
		sp = speeds[tripID]
		return(sp, vendor,tripID)
	# else:
	# 	return(0,None)


res = vendors.map(findVendors).filter(lambda x: x is not None).sortBy(lambda x: x[0], ascending = False ).collect()
print(res)

print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')

# Create the pandas DataFrame 
df = pd.DataFrame(res, columns = ['Average Speed (km/h)', 'Vendor', 'tripID']) 
### df = df.dropna()
### df = df.reset_index(drop=True)

print(df)
# df.to_csv("./Q3broadcast_out.csv", sep=',',index=False)

