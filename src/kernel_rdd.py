import csv
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("").setMaster("local")
sc = SparkContext(conf=conf)