from multiprocessing import Process, Manager
from abc import ABCMeta, abstractmethod
from random import random
import numpy as np
import sys
import re

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

		if predicate == 'parent':
			kvp[arg[1]] = str('{'+predicate+','+arg[0]+'}')

		elif predicate == 'sibling':
			kvp[arg[0]] = str('{'+predicate+','+arg[1]+'}')

		elif predicate == 'ParentOfSiblings':
			kvp[arg[1]] = str('{'+predicate+','+arg[0]+','+arg[2]+'}')

		elif predicate == 'female':
			kvp[arg[0]] = str('{'+predicate+'}')

		return kvp.items()
	
	# The reduce function (with anti-join).
	def reduce(self, k, vs):
		key_value_pairs = {}
		vs.sort()
		value = ''
		for i in range(len(vs)):
			if "female" in vs[i]:
				return
			value+=vs[i]
			value+=','
		value = value[:-1]
		key_value_pairs[k] = value
		return key_value_pairs.items()


###### The Main function

if __name__ == "__main__":


	# Reading the contents of the file.
	with open('randomsample.txt') as f:
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
		if "not" in literal:
			literal = literal.replace("not ",'')
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

	## print("\n")
	## print "Set I: ",I,"\n"
	## print "Set J: ",J,"\n"

	print "First MapReduce Cycle:\n\n"


	# Executing the first MapReduce cycle on set I.
	obj1 = MapReduceProlog(I, 5,5)
	join_result = obj1.execute()

	# Creating the new sub goal 'ParentOfSiblings' from the results obtained from the first MapReduce cycle.
	new_sub_goal = []
	for item in join_result:
		tup = item[0]
		new_sub_goal_str = 'ParentOfSiblings('
		result=re.compile("{(.*?)}",re.M|re.DOTALL).findall(tup[1])
		for res in result:
		    res = res.split(',')
		    new_sub_goal_str+=res[1]+','
		new_sub_goal_str+=tup[0]
		new_sub_goal_str+=')'
		new_sub_goal.append(new_sub_goal_str)

	# Some string processing using regular expressions required.
	processed_new_sub_goal = []
	predicate_regex = re.compile("(.*?)\s*\((.*?)\)")
	for item in new_sub_goal:
		predicate = predicate_regex.match(item).group(1)
		args = re.search(r'\((.*?)\)',item).group(1)
		args = args.replace(" ", "")
		arg = args.split(',')
		if len(arg)==3:
			processed_new_sub_goal.append(item)

	## print "Generated facts after First MapReduce Cycle: \n\n",processed_new_sub_goal,"\n"

	# Appending the new facts with set J.
	for i in J:
		processed_new_sub_goal.append(i[1])
	processed_new_sub_goal = zip(range(1,len(processed_new_sub_goal)+1), processed_new_sub_goal)
	
	## print "Appended newly generated facts to set J: \n\n", processed_new_sub_goal

	print "\n-------------------------------------\n"
	print "Second MapReduce Cycle:\n\n"

	# Executing the second MapReduce cycle on updated set J.
	obj2 = MapReduceProlog(processed_new_sub_goal, 5,5)
	anti_join_result = obj2.execute()

	print "\n-------------------------------------\n"
	print "Generated Results:\n\n"

	# Fetching the results from the second MapReduce cycle and loading the results into a text file.
	f= open("output.txt","w+")
	for item in anti_join_result:
		tup = item[0]
		result=re.compile("{(.*?)}",re.M|re.DOTALL).findall(tup[1])

		for res in result:
		    res = res.split(',')
		    ans = 'Son('+item[0][0]+','+res[1]+').'
		    print ans
		    f.write(ans+"\n")
	f.close()

	print "\n-------------------------------------\n"
	print "Results Exported to output.txt file!!!!\n"