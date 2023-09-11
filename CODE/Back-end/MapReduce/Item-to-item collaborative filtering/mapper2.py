#!/usr/bin/python

import os
import sys
import re

#input: (movieID, (userID, rating, average))
#output: (userID, (movieID, rating, average))

for line in sys.stdin:
	line = line.strip()
	movie, value = line.split('\t')
	user, rating, avg = value.split(' ')
	z = str(movie) + ' ' + str(rating) + ' ' + str(avg)
	print("%s\t%s" % (str(user), z))