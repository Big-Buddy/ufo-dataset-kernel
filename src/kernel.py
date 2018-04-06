import csv
import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row

def avg_duration(dataRDD):
	temporalRDD = dataRDD.map(lambda x: ('time_elapsed',x['duration']))
	temporalRDD = temporalRDD.filter(lambda x: x[1])
	count = temporalRDD.count()
	sum_durations = temporalRDD.reduceByKey(lambda a,b: a+b).collect()
	return sum_durations[0][1]/count

def hemispheres(dataRDD):
	geographicalRDD = dataRDD.map(lambda x: (x['latitude'], x['longitude']))
	no_soRDD = geographicalRDD.map(lambda x: ('north', 1) if x[0] >= 0 else ('south', 1))
	ea_weRDD = geographicalRDD.map(lambda x: ('east', 1) if x[1] >= 0 else ('west', 1))

	no_so = no_soRDD.reduceByKey(lambda a,b: a+b).collect()
	ea_we = ea_weRDD.reduceByKey(lambda a,b: a+b).collect()
	return [no_so, ea_we]

def rank_shapes(dataRDD):
	shapeRDD = dataRDD.map(lambda x: (x['shape'], 1))
	shapes = shapeRDD.reduceByKey(lambda a,b: a+b).sort(ascending=False).collect()
	return shapes

def population_centers(dataRDD):


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("kernelBOYS")\
        .getOrCreate()

data_file = sys.argv[1]

###READ
lines = spark.read.text(data_file).rdd
parts = lines.map(lambda row: row.value.split(","))
ufoRDD = parts.map(lambda x: Row(datetime=x[0], state=x[2], country=x[3], shape=x[4], duration=x[5], comment=x[7], latitude=x[9], longitude=x[10]))

###MAP REDUCE SPECS
	#1 avg duration of sighting
	#2 geography
	#3 shape rankings
	#4 alcohol consumption
###FREQUENT PATTERNS
	#5 words used in descriptions
	#6 locations of sightings
	#7 time of sightings (season, month, day/night)
###CLUSTERING
	#8 reliable sightings vs unreliable (look at possible features)
	#9 cluster coordinates
###MAPS
	#10 impose lat-lon coordinate map on world?!??!