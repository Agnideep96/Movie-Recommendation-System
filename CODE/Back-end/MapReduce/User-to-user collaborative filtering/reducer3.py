#!/usr/bin/python

import os
import sys
import re
import numpy as np

#input: (movieID, (sim, avg, new_avg, r))
#output: (movie, pred)

prev_movie = None
sum_up = 0
sum_down = 0
movie = None
pred = 0

for line in sys.stdin:
	line = line.strip()
	movie, values = line.split('\t')
	sim, avg, new_avg, r = values.split(' ')
	sim = float(sim)
	avg = float(avg)
	new_avg = float(new_avg)
	r = float(r)
	if prev_movie == movie:
		sum_up += sim*(r - avg)
		sum_down += sim
	else:
		if prev_movie:
			pred = new_avg + (sum_up/sum_down)
			print("%s\t%s" % (prev_movie, pred))
		prev_movie = movie
		sum_up = 0
		sum_down = 0
		pred = 0
if prev_movie == movie and prev_movie is not None:
	print("%s\t%s" % (prev_movie, pred))

