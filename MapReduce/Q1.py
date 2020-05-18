from pyspark import SparkContext
from datetime import date, datetime
import pandas as pd
import time

starttime = time.time()

# datapath = "/Proj/smalldata.csv"
datapath = "/Proj/yellow_tripdata_1m.csv"

sc = SparkContext("spark://master:7077", "Q1")
Data = sc.textFile(datapath)

data = Data.flatMap(lambda x: x.split('\n')) \
.map(lambda y:y.split(','))


def TotMinDif(row):
	start, end = row[1], row[2]
	FMT ='%Y-%m-%d %H:%M:%S'
	tdelta = datetime.strptime(end, FMT) - datetime.strptime(start, FMT)
	
	t = start.split(' ')[1]
	hour_of_day = t[0:2]
	return( [hour_of_day , tdelta.total_seconds()/60.0])


# emit1 =data.map(totalminutedif)
emit1 =data.map(TotMinDif)

res = emit1 \
.groupByKey() \
.mapValues(lambda x: sum(x) / len(x)) \
.sortBy(lambda x: x[0])



res = res.collect()
# Create the pandas DataFrame 
df = pd.DataFrame(res, columns = ['HourOfDay', 'AverageTripDuration']) 

print(df)
# df.to_csv("./Q1out.csv", sep=',',index=False)


print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')


## print('')
## print('Hour of Day \t Average Trip Duration')
## for row in res:
## 	print(str(row[0])+'\t \t '+str(row[1]))











# def daydifference(startdate, enddate):
# 	startdate = startdate.split('-')
# 	enddate = enddate.split('-')
# 	d0 = date(int(startdate[0]), int(startdate[1]), int(startdate[2]))
# 	d1 = date(int(enddate[0]), int(enddate[1]), int(enddate[2]))
# 	delta = d1 - d0
# 	daydif =  delta.days
# 	return(daydif)

# def minutedifference(t1, t2):
# 	FMT = '%H:%M:%S'
# 	tdelta = datetime.strptime(t2, FMT) - datetime.strptime(t1, FMT)
# 	tot_minutes = tdelta.total_seconds()/60.0
# 	return(tot_minutes)

# def totalminutedif(row):
# 	startdatetime, enddatetime = row[1], row[2]
# 	start = startdatetime.split(' ')
# 	end = enddatetime.split(' ')
# 	startdate, starttime = start[0], start[1]
# 	enddate, endtime = end[0], end[1]

# 	daydif = daydifference(startdate,enddate)
# 	timedif = minutedifference(starttime,endtime)
# 	tot_minutes = timedif + daydif*24*60

# 	hour_of_day = starttime[0:2]

# 	return([hour_of_day, tot_minutes])