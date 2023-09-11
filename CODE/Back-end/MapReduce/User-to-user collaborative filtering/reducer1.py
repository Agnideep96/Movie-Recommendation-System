#!/usr/bin/python

import os
import sys
import re

#input: (userID, (movieID, rating))
#output: (userID, (movieID, rating, average))
ratings = {}
average = {}
avg = 0
i = 0
for line in sys.stdin:
	line = line.strip()
	user, value = line.split('\t')
	movie, rating = value.split(',')
	rating = float(rating)
	if user in ratings:
		ratings[user].append((movie, rating))
	else:
		ratings[user] = [(movie, rating)]

for user in ratings:
	for movie, rating in ratings[user]:
		avg += rating
		i += 1
	average[user] = avg/i
	avg = 0
	i = 0

for user in ratings:
	for movie, rating in ratings[user]:
		z = str(movie) + ' ' + str(rating) + ' ' + str("{:.2f}").format(average[user])
		print("%s\t%s" % (str(user), z))

