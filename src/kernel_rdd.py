import csv
import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("").setMaster("local")
sc = SparkContext(conf=conf)

data_file = sys.argv[1]

###MAP REDUCE SPECS
	#avg duration of sighting
	#north vs south hemisphere
	#population centers
###FREQUENT PATTERNS
	#words used in descriptions
	#locations of sightings
	#time of sightings (season, month, day/night)
###CLUSTERING
	#reliable sightings vs unreliable (look at possible features)
###MAPS
	#impose x-y coordinate map on world?!??!
