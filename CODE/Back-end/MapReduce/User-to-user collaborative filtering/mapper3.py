#!/usr/bin/python

import os
import sys
import re
import numpy as np

#input: ((movieID, user, new_user), (sim, avg, new_avg, rating))
#output: (movieID, (sim, avg, new_avg, r))
#afegir cacheFile

f = open('data.txt', 'r')
i = 0
users = {}
for line in f:
	if i == 0:
		i += 1
	else:
		line = line.strip()
		user, movieID, title, rating = line.split(",")
		if user in users:
			users[user].append((movieID, rating))
		else:
			users[user] = [(movieID, rating)]

values = {}
new_movie = []
for line in sys.stdin:
	line = line.strip()
	key, value = line.split('\t')
	user, new_user, movie = key.split(' ')
	new_movie.append(movie)
	sim, avg, new_avg, r = value.split(' ')
	values[user] = (sim, avg, new_avg)
for user in values:
	for movie, rating in users[user]:
		if movie not in new_movie:
			z = values[user][0] + ' ' + values[user][1] + ' ' + values[user][2] + ' ' + rating
			print("%s\t%s" % (movie, z))




