#!/usr/bin/python

import os
import sys
import re

#input: userID, movieID, title, rating
#output: (movieID, (userID, rating))
i = 0
for line in sys.stdin:
	if i == 0:
		i += 1
	else:
		line = line.strip();
		user, movieID, title, rating = line.split(",")
		z = user + ',' + rating
		print("%s\t%s" % (str(movieID), z))
