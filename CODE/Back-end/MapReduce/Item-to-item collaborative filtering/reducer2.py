#!/usr/bin/python

import os
import sys
import re

#input: (userID, (movieID, rating, avg_m))
#output: (userID, (movieID, rating, avg_m, avg_u))
ratings = {}
average = {}
avg = 0
i = 0
for line in sys.stdin:
	line = line.strip()
	user, value = line.split('\t')
	movie, rating, avg_m = value.split(' ')
	
	if user in ratings:
		ratings[user].append((movie, rating, avg_m))
	else:
		ratings[user] = [(movie, rating, avg_m)]

for user in ratings:
	for movie, rating, avg_m in ratings[user]:
		rating = float(rating)
		avg += rating
		i += 1
	average[user] = avg/i
	avg = 0
	i = 0
	
for user in ratings:
	for movie, rating, avg_m in ratings[user]:
		z = str(movie) + ' ' + str(rating) + ' ' + str(avg_m) + ' ' + str("{:.2f}").format(average[user])
		print("%s\t%s" % (str(user), z))

