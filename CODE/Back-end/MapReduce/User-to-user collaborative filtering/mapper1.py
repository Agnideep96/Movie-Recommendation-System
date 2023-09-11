#!/usr/bin/python

import os
import sys
import re

#input: userID, movieID, rating
#output: (userID, (movieID, rating))

f = open('data.txt', 'r')

for line in sys.stdin:
	line = line.strip();
	movieID, rating = line.split(",")
	user = 0
	z = movieID + ',' + rating
	print("%s\t%s" % (str(user), z))
i = 0
for line in f:
	if i == 0:
		i += 1
	else:
		line = line.strip()
		user, movieID, title, rating = line.split(",")
		if user != '':
			user = int(user)
			z = movieID + ',' + rating
			print("%s\t%s" % (str(user), z))