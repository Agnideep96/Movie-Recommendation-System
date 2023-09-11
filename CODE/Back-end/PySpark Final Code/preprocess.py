from pyspark.sql import functions as F
from pyspark import SparkContext
from pyspark.sql import SparkSession
#create Spark session
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName('PySpark DataFrame From RDD').getOrCreate()
from decimal import *

#input: array of ratings for a certain movie
#returns the average of an array of ratings
def average(data):
	n = len(data)
	total = sum(data)
	return total/n

#input: array with (movie, rating, avg_movie)
#returns an array with pairs of movies, their ratings and averages
def make_pairs(data):
	res = []
	#movies will store the appended movies
	#so that they are not repeated, i.e.: (m_i, m_j) and (m_j, m_i) are the same
	movies = []
	for m1 in data:
		for m2 in data:
			if m1[0] != m2[0] and m2[0] not in movies: #if different movies
				res.append((m1,m2))
				if m1[0] not in movies:
					movies.append(m1[0])
	return res

#input: (m1, avg1, m2, avg2), [(avg_u, r1, r2)]
#returns the cosine similarity value sim(m1, m2)
#sim(m1,m2) = sum((r1-avg_u)*(r2-avg_u))/(sqrt(sum(r1^2))*sqrt(sum(r2^2)))
def sim(data):
	key = data[0]
	value = data[1]
	sim_up = 0
	sim_i = 0
	sim_j = 0
	sim = 0
	for t in value:
		sim_up += (t[1]-t[0])*(t[2]-t[0])
		sim_i += t[1]**2
		sim_j += t[2]**2
	#user of Decimal because the numbers are very big and numpy can't process them
	sim = sim_up/float(Decimal(sim_i).sqrt()*Decimal(sim_j).sqrt())
	return sim

#path from which to read the file
#if used in GCP, add gs:// at the beginning of the bucket path
df = sc.textFile('/Users/nuria/Desktop/CA675/Assignment 2 CA675/pyspark/input_in.txt')
#input file: movie, rating, user

#df2: removes header and possible null values
df2 = df.map(lambda x: x.split(',')).filter(lambda x: x[2] != ' userId' and x[2] != 'userId' and x[0] != '' and x[1] != '' and x[2] != '')

#users: obtains list of users
users = df2.map(lambda x: (x[2],0)).groupByKey().map(lambda x: x[0]).take(2000)

#df21 selects users and converts data to int for IDs and float for rating
df21 = df2.filter(lambda x: x[2] in users).map(lambda x: ((int(x[2])), (int(x[0]), float(x[1]))))
#df21 now is (userID, (movieID, rating))

#df3: computes average for each user
df3 = df21.map(lambda x: (x[0], x[1][1])).groupByKey().mapValues(list).map(lambda x: (x[0], average(x[1])))

#avg_user: joins again with movieID
#returns (movieID, (userID, rating, avg_user))
avg_user = df21.leftOuterJoin(df3).map(lambda x: ((x[1][0][0]), (x[0], x[1][0][1], x[1][1])))

#df4 = computes average for each movie
df4 = avg_user.map(lambda x: (x[0], x[1][1])).groupByKey().mapValues(list).map(lambda x: (x[0], average(x[1])))

#avg_total: joins again with userID
#returns ((userID, avg_user), [(movieID, rating, avg_movie)])
#which is a list of movies rated by each user
avg_total = avg_user.leftOuterJoin(df4).map(lambda x: ((x[1][0][0],x[1][0][2]),(x[0],x[1][0][1],x[1][1]))).groupByKey().mapValues(list)

#pairs: for each user, make pairs of two movies in the user's array with their corresponding ratings and averages
pairs = avg_total.flatMap(lambda x: [(x[0], key) for key in make_pairs(x[1])])

#clean: returns (movie1, avg_movie1, movie2, avg_movie2), [(avg_user, rating_movie1, rating_movie2)]
#which is a list of user's ratings for every pair of movies they have rated
clean = pairs.map(lambda x: ((x[1][0][0], x[1][0][2], x[1][1][0], x[1][1][2]),(x[0][1],x[1][0][1],x[1][1][1]))).groupByKey().mapValues(list)

#group: returns ((movie1, movie2), (similarity(movie1, movie2), avg_movie1, avg_movie2))
group = clean.map(lambda x: (int(x[0][0]), int(x[0][2]), sim(x), x[0][1], x[0][3]))

#store the value in a folder named out1 in the specified path
group.saveAsTextFile('/Users/nuria/Desktop/CA675/Assignment 2 CA675/pyspark/out1')

