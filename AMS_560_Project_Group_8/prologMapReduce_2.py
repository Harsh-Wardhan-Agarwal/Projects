from multiprocessing import Process, Manager, Queue
import multiprocessing
from abc import ABCMeta, abstractmethod
from random import random
import numpy as np
import sys
import re
import threading
import time
list_time = []
class MapReduce:
	__metaclass__ = ABCMeta
	
	def __init__(self, data, number_of_mapper_tasks=1, number_of_reducer_tasks=1):
		self.data = data
		self.number_of_reducer_tasks=number_of_reducer_tasks
		self.number_of_mapper_tasks=number_of_mapper_tasks


	# Allocating data blocks to each map job.
	def mapperTask(self, block, reducer_input):

		for (k, v) in block:
			mapper_k_v_pairs = self.map(k, v)
			for k,v in mapper_k_v_pairs:
				reducer_input.append((self.partition(k), (k, v)))

	# Accumulating the results of the mapper ans hashing them by keys to pass each set of hashed values to unique reducer.
	def partition(self,k):
			
		try:
			reducer = hash(k) % self.number_of_reducer_tasks
		except: 
			print("hash value not found")
		return reducer

	# Allocating data blocks to each reduce job.
	def reducerTask(self, key_value_pairs, reducer_output):
		  
		key_dict = {}
		for (k,v) in key_value_pairs:
			if k in key_dict:
				temp = key_dict.get(k)
				temp.append(v)              
				key_dict[k] = temp
			else:
				key_dict[k] =  [v]
		for key, countList in key_dict.iteritems():
			reduceValue = self.reduce(key,countList)
			if reduceValue is not None:         
				reducer_output.append(reduceValue)      
				 

	# Execution of MapReduce jobs start here
	def execute(self):

		# Defining input and output streams.
		reducer_input = Manager().list()
		reducer_output = Manager().list()
		jobs = []

		# Finding the block size.
		block_size = int(len(self.data)/self.number_of_mapper_tasks)
		mapper_jobs = []

		if block_size == 0:
			block_size = 1

		# Assigning facts to each mapper job based on the block size.
		for i in range(0,len(self.data),block_size):
			mapper_jobs.append(self.data[i:i+block_size])
		
		# Parallely executing the map jobs based on allocated facts.
		for mapjob in mapper_jobs:       
			jobs.append(Process(target=self.mapperTask, args=(mapjob,reducer_input)))
			jobs[len(jobs)-1].start()     
 
		for job in jobs:
			job.join()


		print("Mapper Executed.......\n")

		# Initiating reducer jobs.
		reducejob = [[] for i in range(self.number_of_reducer_tasks)] 

		for (task_num,node) in reducer_input:
			reducejob[task_num].append(node)

		# Parallely executing the reducer jobs based on allocated mapper and partition results.
		reducerTasks = []
		for i in range(0,self.number_of_reducer_tasks):
		   reducerTasks.append(Process(target=self.reducerTask, args =(reducejob[i],reducer_output)))
		   reducerTasks[i].start() 

		for i in reducerTasks:
			i.join()       


		print("Reducer Executed.......\n")

		return reducer_output

class MapReduceProlog(MapReduce):
	
	# The map function (with join).
	def map(self, k, v):
		predicate_regex = re.compile("(.*?)\s*\((.*?)\)")
		predicate = predicate_regex.match(v).group(1)
		args = re.search(r'\((.*?)\)',v).group(1)
		args = args.replace(" ", "")
		arg = args.split(',')
		kvp = {}

		if predicate == 'purchased':
			kvp[arg[0]] = str('{'+predicate+','+arg[1]+'}')
		elif predicate == 'sold':
			kvp[arg[0]] = str('{'+predicate+','+arg[1]+'}')
		elif predicate == 'buyer':
			kvp[arg[0]] = str('{'+predicate+'}')
		elif predicate == 'seller':
			kvp[arg[0]] = str('{'+predicate+'}')

		elif predicate == 'BuyerWhoPurchased':
			kvp[arg[1]] = str('{'+predicate+','+arg[0]+'}')
		elif predicate == 'SellerWhoSold':
			kvp[arg[1]] = str('{'+predicate+','+arg[0]+'}')

		return kvp.items()
	
	# The reduce function (with anti-join).
	def reduce(self, k, vs):
		key_value_pairs = {}
		vs.sort()
		#print("VS: ",vs)
		value = ''
		for i in range(len(vs)):
			if "BuyerWhoPurchased" in vs[i] or "SellerWhoSold" in vs[i]:
				value+=vs[i]
				value+=','
				
			elif "seller" in vs[0] or "buyer" in vs[0]:
				#return
				value+=vs[i]
				value+=','
		value = value[:-1]
		key_value_pairs[k] = value
		return key_value_pairs.items()

def worker1(I, que, num):
	# print '\n worker1 starting \n'
	list_time2 = []
	join_result1 = []
	obj1 = MapReduceProlog(I, 5,5)
	tup = ("MR1-T"+str(num)+" Start", time.time())
	list_time2.append(tup)
	temp = obj1.execute()
	tup = ("MR1-T"+str(num)+" Stop", time.time())
	list_time2.append(tup)
	# print '\nI = ',temp,'\n worker1 ending \n'
	join_result1.extend(temp)
	# print '\n aaja: ', join_result1, id(join_result1)
	que.put(join_result1)
	que.put(list_time2)
	
		



if __name__ == "__main__":


	# Reading the contents of the file.
	start_time = time.time()
	tup = ("Start", start_time)
	list_time.append(tup)
	with open(sys.argv[1]) as f:
		content = f.readlines()
		content = [x.strip() for x in content]
		content = [x.strip('.') for x in content]

	## print "File Contents: \n",content
	
	# Seperating facts and queries.
	facts = content[:-1]
	facts = list(set(facts))
	query = content[-1].replace(':-', ',')
	qurey_literals = re.split(r',\s*(?![^()]*\))', query)

	
	negative_sub_goals = []
	positive_sub_goals = []
	atomic_negative_sub_goals = []
	atomic_positive_sub_goals = []

	# Finding negative and positive sub goals
	for literal in qurey_literals:
		if "buyer" in literal or "purchased" in literal:
			#literal = literal.replace("not ",'')
			negative_sub_goals.append(literal)
		else:
			positive_sub_goals.append(literal)
			
	

	# Collecting the final goal.
	final_goal = positive_sub_goals[0]
	positive_sub_goals = positive_sub_goals[1:]

	# Collecting the atomic form of sub goals.... 'parent', 'sibling', etc
	regex = re.compile("(.*?)\s*\((.*?)\)")
	for sub_goal in negative_sub_goals:
		x = regex.match(sub_goal)
		atomic_negative_sub_goals.append(x.group(1))
	for sub_goal in positive_sub_goals:
		x = regex.match(sub_goal)
		atomic_positive_sub_goals.append(x.group(1))

	atomic_final_goal = regex.match(final_goal).group(1)


	# Creating I and J sets.
	I = [i for e in atomic_positive_sub_goals for i in facts if e in i]
	J = [i for e in atomic_negative_sub_goals for i in facts if e in i]

	I = zip(range(1,len(I)+1), I)
	J = zip(range(1,len(J)+1), J)

	# print("\n")
	# print "Set I: ",I,"\n"
	# print "Set J: ",J,"\n"

	print "First MapReduce Cycle:\n\n"
	# Executing the first MapReduce cycle on set I.
	join_result1 = []
	join_result2 = []
	t = [I,J]
	temp = []
	procs = []
	queue1 = Queue() #create a queue object
	n = 1
	for x in t:
		#print x
		
		p = Process(target= worker1, args= (x,queue1,n)) #we're setting 3rd argument to queue1
		p.start()
		
		procs.append(p)
		n = 2
		
	# for p in procs:
		# p.join()
		# print("got it")
	
	# print("hereeee")
	join_result1 = queue1.get()
	list_time.extend(queue1.get())
	join_result2 = queue1.get()
	list_time.extend(queue1.get())

	
	#print '\n I= ', join_result1
	#print '\n J= ', join_result2
	# print("here--- %s seconds ---" % (time.time() - start_time))
	
	# Creating the new sub goal 'ParentOfSiblings' from the results obtained from the first MapReduce cycle.
	new_sub_goal1 = []
	for item in join_result2:
		tup = item[0]
		result=re.compile("{(.*?)}",re.M|re.DOTALL).findall(tup[1])
		for res in result:
		    res = res.split(',')
		    if len(res) > 1:
		    	new_sub_goal1_str = 'BuyerWhoPurchased('+tup[0]+','
		    	new_sub_goal1_str+=res[1]
		    	new_sub_goal1_str+=')'
		    	new_sub_goal1.append(new_sub_goal1_str)
	#print("JHVkjhvkhvk: ",new_sub_goal1)


	new_sub_goal2 = []
	for item in join_result1:
		tup = item[0]
		result=re.compile("{(.*?)}",re.M|re.DOTALL).findall(tup[1])
		for res in result:
		    res = res.split(',')
		    if len(res) > 1:
		    	new_sub_goal2_str = 'SellerWhoSold('+tup[0]+','
		    	new_sub_goal2_str+=res[1]
		    	new_sub_goal2_str+=')'
		    	new_sub_goal2.append(new_sub_goal2_str)

	
	processed_new_sub_goal = new_sub_goal1 + new_sub_goal2

	for i in range(len(processed_new_sub_goal)):
		processed_new_sub_goal[i] = (i+1,processed_new_sub_goal[i])

	print "\n-------------------------------------\n"
	print "Second MapReduce Cycle:\n\n"
	# Executing the second MapReduce cycle on updated set J.
	tup = ("MR2 Start", time.time())
	list_time.append(tup)
	obj3 = MapReduceProlog(processed_new_sub_goal, 5,5)
	join_result3 = obj3.execute()
	tup = ("MR2 Stop", time.time())
	list_time.append(tup)
	print("--- %s seconds ---" % (time.time() - start_time))
	# print join_result3

	print "\n-------------------------------------\n"
	print "Generated Results:\n\n"

	# Fetching the results from the second MapReduce cycle and loading the results into a text file.
	f= open("output.txt","w+")
	for item in join_result3:
		tup = item[0]
		result=re.compile("{(.*?)}",re.M|re.DOTALL).findall(tup[1])
		#ans = 'buy_sell('
		ans = atomic_final_goal+'('
		buyers = []
		sellers = []
		for res in result:
		    res = res.split(',')
		    if res[0] == 'BuyerWhoPurchased':
		    	buyers.append(res[1])
		    elif res[0] == 'SellerWhoSold':
		    	sellers.append(res[1])
		ans+=item[0][0]+','+str(buyers)+','+str(sellers)+').'
		# print ans
		f.write(ans+"\n")
	f.close()
	print "\n-------------------------------------\n"
	print "Results Exported to output.txt file!!!!\n"
	stop_time = time.time()
	tup = ("Stop", stop_time)
	list_time.append(tup)
	print list_time
	print "\n-------------------------------------\n"
	print "Graphical Analysis\n"
	import matplotlib.pyplot as plt
	
			
	result = sorted(list_time, key=lambda x: x[1])
	print("HERE: ",result)
	plt.plot(range(len(list_time)), [(x[1] - start_time) for x in result], 'k')
	plt.plot(range(len(list_time)), [(x[1] - start_time) for x in result], 'bo')
	temp = stop_time - start_time
	for i in range(len(result)):
		x = result[i][0]
		y = result[i][1] - start_time
		print x,y
		plt.text(i * (1 + 0.01), y * (1 + 0.01) , x, {'ha': 'center', 'va': 'bottom'}, fontsize=8, rotation=45)
	plt.ylim(ymax=temp+2)
	plt.show()