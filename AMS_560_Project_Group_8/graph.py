from abc import ABCMeta, abstractmethod
from random import random
import numpy as np
import sys
import re
import time

if __name__ == "__main__":
	
	print "\n-------------------------------------\n"
	l1 = [(' Start', 1525851352.712909), ('MR1 Start', 1525851353.035068), ('MR1 Stop', 1525851354.863394), ('MR2 Start', 1525851354.863412), ('MR2 Stop', 1525851360.18841), ('MR3 Start', 1525851363.145086), ('MR3 Stop', 1525851366.150539), ('Stop', 1525851367.315466)]
	l2 = [('Start', 1525851507.660789), ('MR1-T1 Start', 1525851507.893014), ('MR1-T2 Start', 1525851507.894223), ('MR1-T1 Stop', 1525851510.258999), ('MR1-T2 Stop', 1525851513.555469), ('MR2 Start', 1525851515.389094), ('MR2 Stop', 1525851518.588742), ('Stop', 1525851519.476613)]
	
	print "Results Exported to this file!!!!\n"
	# l2 = [('a',0.2),('b',0.35),('c',3.55)]
	print l1,l2
	print "\n-------------------------------------\n"
	print "Graphical Analysis\n"
	import matplotlib.pyplot as plt


	t, start_time = l1[0]
	print "1.", start_time
	p1 = plt.plot([(x[1] - start_time) for x in l1], range(len(l1)), 'k')
	plt.plot([(x[1] - start_time) for x in l1], range(len(l1)), 'bo', label='Previous Approach')
	t, stop_time = l1[len(l1) - 1]
	temp1 = stop_time - start_time
	for i in range(len(l1)):
		x = l1[i][0]
		y = l1[i][1] - start_time
		print x,y
		plt.text(y * (1 + 0.01), i * (1 + 0.01) , x, {'ha': 'center', 'va': 'top'}, fontsize=7, rotation=30, color='blue')
	plt.setp(p1, linewidth=1)

	t, start_time = l2[0]
	print "2.", start_time
	p2 = plt.plot([(x[1] - start_time) for x in l2], range(len(l2)), 'k')
	plt.plot([(x[1] - start_time) for x in l2], range(len(l2)), 'ro',label='Optimised Approach')
	t, stop_time = l1[len(l2) - 1]
	temp2 = stop_time - start_time
	for i in range(len(l2)):
		x = l2[i][0]
		y = l2[i][1] - start_time
		print x,y, "hello ", i
		plt.text(y * (1 + 0.01),  i* (1 + 0.01) , x, {'ha': 'center', 'va': 'bottom'}, fontsize=7, rotation=30, color='red')
		
	plt.setp(p2, linewidth=1)	
	plt.xlim(xmax=max(temp1, temp2)+5)
	plt.xlabel("Time (sec)")
	plt.ylabel("Level of Execution")
	plt.title("Normal MapReduce vs Optimised MapReduce  (400k)")
	plt.legend(loc=2)
	plt.show()