#!/usr/bin/python

import os
import sys
import re
import numpy as np
from decimal import *

#input: (movieID, (userID, rating, avg_m, avg_u))
#output: ((m_i, m_j), (sim, avg_i, avg_j))

ratings = {}
average_m = {}
for line in sys.stdin:
	line = line.strip()
	movie, value = line.split('\t')
	user, rating, avg_m, avg_u = line.split(' ')
	if movie in ratings:
		ratings[movie].append((user, rating, avg_u))
	else:
		ratings[movie] = [(user, rating, avg_u)]
	average_m[movie] = avg_m

sim_up = 0
sim_i = 0
sim_j = 0
eq_users = 0
previous = []
for i in ratings:
	previous.append(i)
	for j in ratings:
		#evitem que hi hagi i,j i j,i que és el mateix
		if i != j and j not in previous:
			for u_i, r_i, avg_i in ratings[i]:
				for u_j, r_j, avg_j in ratings[j]:
					if u_i == u_j:
						eq_users += 1
						sim_up += (r_i-avg_i)*(r_j-avg_j)
						sim_i += r_i**2
						sim_j += r_j**2
			if eq_users > 0:
				sim = sim_up/float(Decimal(sim_i).sqrt()*Decimal(sim_j).sqrt())
				k = str(i) + ' ' + str(j)
				v = str("{:.2f}").format(sim) + ' ' + str(average_m[i]) + ' ' + str(average_m[j])
				eq_users = 0
				print('%s\t%s' % (k,v))



