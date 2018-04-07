import csv
import sys
import string
from pyspark.sql import SparkSession
from pyspark.sql import Row

def avg_duration(dataRDD):
	temporalRDD = dataRDD.map(lambda x: x['duration'])
	temporalRDD = temporalRDD.map(detect_int_or_float)
	temporalRDD = temporalRDD.filter(lambda x: x[1] and x[1] != 'broken')
	count = temporalRDD.count()
	sum_durations = temporalRDD.reduceByKey(lambda a,b: a+b).collect()
	return sum_durations[0][1]/count

def detect_int_or_float(data):
	try:
		return ('time elapsed', int(data))
	except ValueError:
		try:
			return ('time elapsed', float(data))
		except ValueError:
			return ('time elapsed', 'broken')

def hemispheres(dataRDD):
	geographicalRDD = dataRDD.map(lambda x: (float(x['latitude']), float(x['longitude'])))
	geographicalRDD = geographicalRDD.map()
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
	print()
def rank_words(dataRDD):
	wordRDD = dataRDD.map(lambda x: x['comments'])
	wordRDD = wordRDD.flatMap(remove_punc_garbage_stopwords)
	wordRDD = wordRDD.map(lambda x: (x, 1))
	top_10 = wordRDD.reduceByKey(lambda a,b: a+b).sort(ascending=False).take(10)
	return top_10

def remove_punc_garbage_stopwords(data):
	output = data.replace('&amp', ' ')
	output = output.replace('&#44', ' ')
	output = output.replace('&#44000', ' ')
	output = output.replace('&#39', ' ')
	output = output.replace('&quote;', ' ')
	output = output.replace('&#8230', ' ')
	output = output.replace('&#33', ' ')
	output = output.translate(None, string.punctuation)
	output = output.split(' ')

	target_buffer = []

	for word in output:
		if (word not in stopwords):
			target_buffer.append()

	return target_buffer

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("kernelBOYS")\
        .getOrCreate()

data_file = sys.argv[1]

###LOAD STOPWORDS
global stopwords
with open("./data/stopwords.txt", 'r') as f:
	stopwords = f.readlines()

###READ
lines = spark.read.text(data_file).rdd
header = lines.first()
lines = lines.filter(lambda x: x != header)
parts = lines.map(lambda row: row.value.split(","))
ufoRDD = parts.map(lambda x: Row(datetime=x[0], state=x[2], country=x[3], shape=x[4], duration=x[5], comment=x[7], latitude=x[9], longitude=x[10]))

print(avg_duration(ufoRDD))
print(hemispheres(ufoRDD))
print(rank_shapes(ufoRDD))
print(rank_seasons(ufoRDD))
print(rank_words(ufoRDD))

###MAP REDUCE
	#1 avg duration of sighting
	#2 geography
	#3 shape rankings
	#4 seasonal rankings
	#5 alcohol consumption
	#6 words used in descriptions
	#7 locations of sightings
	#8 time of sightings (season)
###FREQUENT PATTERNS
	#common word patterns
###CLUSTERING
	#9 reliable sightings vs unreliable (look at possible features)
	#10 cluster coordinates
	#11 time of sightings (cluster then find range)
###MAPS
	#12 impose lat-lng coordinates on world map?!??!
