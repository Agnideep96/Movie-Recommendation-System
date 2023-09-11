#!/usr/bin/python

import os
import sys
import re
import numpy as np

#input: ((userID, new_userID), (movieID, avg, new_avg, rating, new_rating))
#output: ((user, new_user, movieID), (sim, avg, new_avg, rating))

previous = None

sum_up = 0
sum_a = 0
sum_b = 0

for line in sys.stdin:
	line = line.strip()
	key, values = line.split('\t')
	user, new_user = key.split(' ')
	movie, avg, new_avg, r, new_r = values.split(' ')
	avg = float(avg)
	new_avg = float(new_avg)
	r = float(r)
	new_r = float(new_r)
	if previous == user:
		sum_up += (new_r - new_avg)*(r - avg)
		sum_a += (new_r - new_avg)**2
		sum_b += (r - avg)**2
	else:
		if previous:
			sim = sum_up/(np.sqrt(sum_a)*np.sqrt(sum_b))
			k = movie + ' ' + user + ' ' + new_user
			v = str(sim) + ' ' + str(avg) + ' ' + str(new_avg) + ' ' + str(r)
			print("%s\t%s" % (k, v))
		previous = user


