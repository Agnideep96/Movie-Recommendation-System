#!/usr/bin/python

import os
import sys
import re
import numpy as np

#input: ((m_i, m_j), (sim, avg_i, avg_j))
#output: ((m_i, m_j), (sim, avg_i, avg_j))


for line in sys.stdin:
	line = line.strip()
	movies, value = line.split('\t')
	m_i, m_j = movies.split(' ')
	sim, avg_i, avg_j = value.split(' ')
	k = m_i + ' ' + m_j
	v = sim + ' ' + avg_i + ' ' + avg_j
	print("%s\t%s" % (k, v))