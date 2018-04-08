import csv
import sys
import string
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.mllib.clustering import KMeans, KMeansModel
from math import sqrt

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
	geographicalRDD = dataRDD.map(lambda x: (x['latitude'], x['longitude']))
	geographicalRDD = geographicalRDD.map(detect_float)
	geographicalRDD = geographicalRDD.filter(lambda x: x != 'broken')
	no_soRDD = geographicalRDD.map(lambda x: ('north', 1) if x[0] >= 0 else ('south', 1))
	ea_weRDD = geographicalRDD.map(lambda x: ('east', 1) if x[1] >= 0 else ('west', 1))
	no_so = no_soRDD.reduceByKey(lambda a,b: a+b).collect()
	ea_we = ea_weRDD.reduceByKey(lambda a,b: a+b).collect()
	return [no_so, ea_we]

def detect_float(data):
	try:
		return (float(data[0]), float(data[1]))
	except ValueError:
		return ('broken')

def rank_shapes(dataRDD):
	shapeRDD = dataRDD.map(lambda x: (x['shape'], 1))
	shapes = shapeRDD.reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], ascending=False).collect()
	return shapes

def rank_seasons(dataRDD):
	seasonRDD = dataRDD.map(lambda x: (x['datetime']))
	seasonRDD = seasonRDD.map(lambda x: x.split(' '))
	seasonRDD = seasonRDD.map(lambda x: x[0].split('/'))
	seasonRDD = seasonRDD.map(lambda x: x[0])
	seasonRDD = seasonRDD.filter(lambda x: int(x) < 13)
	seasonRDD = seasonRDD.map(get_season)
	seasons = seasonRDD.reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], ascending=False).collect()
	return seasons

def get_season(data):
	return {
		'1' : ('Winter', 1),
		'2' : ('Winter', 1),
		'3' : ('Spring', 1),
		'4' : ('Spring', 1),
		'5' : ('Spring', 1),
		'6' : ('Summer', 1),
		'7' : ('Summer', 1),
		'8' : ('Summer', 1),
		'9' : ('Fall', 1),
		'10' : ('Fall', 1),
		'11' : ('Fall', 1),
		'12' : ('Winter', 1)
	}[data]

def ufo_beer_run(dataRDD):
	print()
def rank_words(dataRDD):
	wordRDD = dataRDD.map(lambda x: x['comment'])
	wordRDD = wordRDD.flatMap(remove_punc_garbage_stopwords)
	wordRDD = wordRDD.map(lambda x: (x, 1))
	top_10 = wordRDD.reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], ascending=False).take(10)
	return top_10

def remove_punc_garbage_stopwords(data):
	output = data.replace('&amp', ' ')
	output = output.replace('&#44', ' ')
	output = output.replace('&#44000', ' ')
	output = output.replace('&#39', ' ')
	output = output.replace('&quote;', ' ')
	output = output.replace('&#8230', ' ')
	output = output.replace('&#33', ' ')

	translator = str.maketrans('', '', string.punctuation)
	output = output.translate(translator)
	output = output.split(' ')

	target_buffer = []

	for word in output:
		if (word.lower() not in stopwords and not ''):
			target_buffer.append(word)

	return target_buffer

def cluster_coords(dataRDD):
	###CHOOSE K
	k = 3

	coordRDD = dataRDD.map(lambda x: (x['latitude'], x['longitude']))
	coordRDD = coordRDD.map(detect_float)
	clusters = KMeans.train(coordRDD, k, maxIterations=100, initializationMode="kmeans||")
	coordRDD.map(lambda x: "{0} {1} {2}".format(clusters.predict(x), x[0], x[1])).saveAsTextFile("coord_cluster.txt")
	write_centroids(clusters.centers, os.path.join("coord_cluster.txt","centroids_final.txt"))

	###Within Set Sum of Squared Errors
	wssse = points.map(lambda point: error(point)).reduce(lambda x, y: x + y)
	print("Final cost: " + str(wsse))

def write_centroids(centroids, file_name):
    with open(file_name, 'w') as f:
        for c in centroids:
            f.write("{0} {1}\n".format(str(c[0]), str(c[1])))

def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))

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
	stopwords = [x.strip() for x in stopwords]

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
cluster_coords(ufoRDD)

###MAP REDUCE
	#alcohol consumption
###FREQUENT PATTERNS
	#common word patterns
###CLUSTERING
	#cluster coordinates
	#time of sightings (cluster then find range)
###MAPS
	#impose lat-lng coordinates on world map?!??!
