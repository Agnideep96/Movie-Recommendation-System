#!/usr/bin/python

import os
import sys
import re
import numpy as np

#input: (movie, pred)
#output: (pred, movie) but sorted

#1	22 5
#1	23 3
#2	40 4
#2	50 1

for line in sys.stdin:
	line = line.strip()
	movie, pred = line.split('\t')
	pred = -float(pred)
	print("%s\t%s" % (str(pred), movie))