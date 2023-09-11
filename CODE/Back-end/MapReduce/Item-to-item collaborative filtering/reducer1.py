#!/usr/bin/python

import os
import sys
import re

#input: (movieID, (userID, rating))
#output: (movieID, (userID, rating, average))
ratings = {}
average = {}
avg = 0
i = 0

for line in sys.stdin:
	line = line.strip()
	movie, value = line.split('\t')
	user, rating = value.split(',')

	rating = float(rating)
	if movie in ratings:
		ratings[movie].append((user, rating))
	else:
		ratings[movie] = [(user, rating)]


for movie in ratings:
	for user, rating in ratings[movie]:
		avg += rating
		i += 1
	average[movie] = avg/i
	avg = 0
	i = 0


for movie in ratings:
	for user, rating in ratings[movie]:
		z = str(user) + ' ' + str(rating) + ' ' + str("{:.2f}").format(average[movie])
		print("%s\t%s" % (str(movie), z))

