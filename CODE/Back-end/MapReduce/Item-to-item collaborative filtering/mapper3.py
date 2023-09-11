#!/usr/bin/python

import os
import sys
import re
import numpy as np
#input: (userID, (movieID, rating, avg_m, avg_u))
#output: (movieID, (userID, rating, avg_m, avg_u))

for line in sys.stdin:
	line = line.strip()
	user, value = line.split('\t')
	movie, rating, avg_m, avg_u = value.split(' ')
	z = str(user) + ' ' + str(rating) + ' ' + str(avg_m) + ' ' + str(avg_u)
	print("%s\t%s" % (str(movie), z))