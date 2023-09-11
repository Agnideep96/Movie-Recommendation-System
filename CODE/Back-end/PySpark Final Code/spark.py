from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName('PySpark DataFrame From RDD').getOrCreate()
from decimal import *
import pandas as pd
import glob

#input: (m_i, m_j), (sim, avgi,avgj)
#output: (m_i, avg_i), (m_j, avg_j)
def check(data, keys):
	if str(data[0][0]) in keys:
		return [(data[0][0],data[1][1]), (data[0][1], data[1][2])]
	return [(data[0][1], data[1][2]), (data[0][0],data[1][1])]

#input: (m_j, array[(m_i, avg_i, sim, avg_j)], r_j)
#output: (m_i, avg_i), (m_j, avg_j, sim, r_j)
def build(data):
	res = []
	for m_i, avgi, sim, avgj in data[1][0]:
		res.append(((m_i, avgi),(data[0], avgj, sim, data[1][1])))
	return res

#input: (m_i, avgi), llista de (m_j, avgj, sim, r_j)
def pred(data):
	res = 0
	up = 0
	down = 0
	for m_j, avgj, sim, r_j in data[1]:
		up += sim*(r_j - avgj)
		down += sim
	res = (up/down) + data[0][1]
	return res

#df = (m1,m2),(sim,avg1,avg2)
#df2 = (m, r)
#df: input from the dataset
#structure: ((m1,m1), (sim, avg_movie1, avg_movie2))
df = sc.textFile('/Users/nuria/Desktop/CA675/Assignment 2 CA675/pyspark/out1/*')
df1 = df.map(lambda x: x.strip('(')).map(lambda x: x.strip(')')).map(lambda x: x.split(','))

#df2: input from the front-end
#structure: (movie, rating) separated by commas
df2 = sc.textFile('/Users/nuria/Desktop/CA675/Assignment 2 CA675/pyspark/input1.txt')

#df3: transform front-end input to integer and float for movieID and rating respectively
df3 = df2.map(lambda x: x.split(',')).map(lambda x: (int(x[0]), float(x[1])))

#df4: transform to integer and float where necessary
df4 = df1.map(lambda x: ((int(x[0]), int(x[1])), (float(x[2]), float(x[3]), float(x[4]))))

#find movies rated by the new user
keys = df2.keys().collect()

#filter_movies: get similarities for the movies rated by the new user
#being movie_i the one rated by the new user:
#finds (movie_i, movie_j) pairs and (movie_j, movie_i)
#returns (movie_i),(movie_j, avg_j, sim, avg_i)
filter_movies = df4.filter(lambda x: str(x[0][0]) in keys or str(x[0][1]) in keys).map(lambda x: (check(x, keys)[0][0], (check(x, keys)[1][0], check(x, keys)[1][1], x[1][0], check(x, keys)[0][1])))

#add_all: groups by movie rated by the new user and joins with the front-end input to get the ratings
#returns (m_i, avg_i), array [(m_j, avg_j, sim, r_j)]
add_all = filter_movies.groupByKey().mapValues(list).leftOuterJoin(df3).flatMap(lambda x: [((key[0], key[1])) for key in build(x)]).groupByKey().mapValues(list)

#res: computes the prediction of rating and sorts to create the rating
#Finally takes the top 10 movies with the highest prediction for the given input
#returns only movieID
res = add_all.map(lambda x: (x[0][0], pred(x))).sortBy(lambda x: -x[1]).map(lambda x: x[0]).take(10)

#saves the output to folder out2
sc.parallelize(res).saveAsTextFile('/Users/nuria/Desktop/CA675/Assignment 2 CA675/pyspark/out2')

#due to integration, it is stored in a single .txt file
file_list = glob.glob("/Users/nuria/Desktop/CA675/Assignment 2 CA675/pyspark/out2/part*")
main_dataframe = pd.read_csv(file_list[0], header = None)

for i in range(1,len(file_list)):
    data = pd.read_csv(file_list[i], header = None)
    main_dataframe = pd.concat([main_dataframe, data], axis = 0)
main_dataframe.to_csv('final.txt', header = None, index = None, sep = ',')
