from pyspark import SparkContext
from datetime import date, datetime
import pandas as pd
import time

starttime = time.time()

# datapath = "/Proj/smalldata.csv"
datapath = "/Proj/yellow_tripdata_1m.csv"

# vendorspath = "/Proj/smallvendors.csv"
vendorspath = "/Proj/yellow_tripvendors_1m.csv"

sc = SparkContext("spark://master:7077", "Q2")
# sc = SparkContext("local", "Q2")

Data = sc.textFile(datapath)
Vendors = sc.textFile(vendorspath)

data = Data.flatMap(lambda x: x.split('\n')) \
.map(lambda y:y.split(','))

vendors = Vendors.flatMap(lambda x: x.split('\n')) \
.map(lambda y:y.split(','))

# x[0] = id , x[1:] ola ta ypoloipa tou data (an einai to data dataset) 'h o vendor (an einai to vendor dataset)
# to reduce ginetai mia fora gia ka8e id kai kollaei (+) ola ta data me ton vendor poy antistoixei s auto to id.  

data = data.map(lambda x: (x[0],x[-1])) # we keep only id and price
vendors = vendors.map(lambda x: (x[0],x[1])) 

# inner join sto id, kratame vendor (list(x[1])[1]) kai price (list(x[1])[0]), reduce by key (vendor)
un = data.join(vendors).map(lambda x: (list(x[1])[1], list(x[1])[0])).reduceByKey(lambda a,b: max(float(a),float(b))).sortBy(lambda x: x[1], ascending = False)


# # Create the pandas DataFrame 
un = un.collect()
# print(un)
df = pd.DataFrame(un, columns = ['Vendor', 'MaxTripPrice']) 
df = df.dropna()
df = df.reset_index(drop=True)

print(df)
# df.to_csv("./Q2out.csv", sep=',',index=False)





print('')
print('Total Time: ' +str(time.time() - starttime) +' sec')
print('')
