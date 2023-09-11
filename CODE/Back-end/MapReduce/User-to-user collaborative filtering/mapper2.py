#!/usr/bin/python

import os
import sys
import re

#input: (userID, (movieID, rating, average))
#output: ((userID, new_userID), (movieID, avg, new_avg, rating, new_rating))
new_user = 0
new_movie = []
new_rating = []
new_avg = 0

for line in sys.stdin:
	line = line.strip()
	user, values = line.split('\t')
	movie, rating, avg = values.split(' ')
	if int(user) == 0:
		new_user = user
		new_movie.append(movie)
		new_rating.append(rating)
		new_avg = avg
	else:
		if movie in new_movie:
			index = new_movie.index(movie)
			key = user + ' ' + new_user
			value = movie + ' ' + avg + ' ' + new_avg + ' ' + rating + ' ' + new_rating[index]
			print("%s\t%s" % (key, value))

