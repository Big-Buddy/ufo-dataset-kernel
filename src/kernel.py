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

def rank_seasons(dataRDD):
	seasonRDD = dataRDD.map(lambda x: (x['datetime']))
	seasonRDD = seasonRDD.map(lambda x: x.split(' '))
	seasonRDD = seasonRDD.map(lambda x: x[0].split('/'))
	seasonRDD = seasonRDD.map(lambda x: x[0])
	seasonRDD = seasonRDD.map((get_season, 1))
	seasons = seasonRDD.reduceByKey(lambda a,b: a+b).sort(ascending=False).collect()
	return seasons

def get_season(data):
	return {
		1 : 'Winter',
		2 : 'Winter',
		3 : 'Spring',
		4 : 'Spring',
		5 : 'Spring',
		6 : 'Summer',
		7 : 'Summer',
		8 : 'Summer',
		9 : 'Fall',
		10 : 'Fall',
		11 : 'Fall',
		12 : 'Winter'
	}[data]

def ufo_beer_run(dataRDD):


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

###MAP REDUCE
	#1 avg duration of sighting
	#2 geography
	#3 shape rankings
	#4 seasonal rankings
	#5 alcohol consumption
###FREQUENT PATTERNS
	#6 words used in descriptions
	#7 locations of sightings
	#8 time of sightings (season, month, day/night)
###CLUSTERING
	#9 reliable sightings vs unreliable (look at possible features)
	#10 cluster coordinates
###MAPS
	#11 impose lat-lon coordinates on world map?!??!