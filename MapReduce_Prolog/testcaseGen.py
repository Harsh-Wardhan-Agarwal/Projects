import pandas as pd
import random


data = pd.read_csv('NationalNames.csv')
names = list(data.Name)

parent_key = []
for i in range(25000):
    parent_key.append(random.choice(names))


s1 = pd.Series(data.Name.unique())
s2 = pd.Series(s1.index.values, index=s1)
remaining_names1 = s2.drop(parent_key)

x = list(remaining_names1.index)
parent_value = []
for i in range(25000):
    parent_value.append(random.choice(x))

remaining_names2 = remaining_names1.drop(parent_value)

sibling_key = []
for i in range(int(len(parent_value)/2)):
    sibling_key.append(parent_value[i])

x = list(remaining_names2.index)
for i in range(25000):
    sibling_key.append(random.choice(x))

remaining_names3 = s2.drop(sibling_key)

x = list(remaining_names3.index)
sibling_value = []
for i in range(25000):
    sibling_value.append(random.choice(x))

female = []
for i in range(15000):
    female.append(random.choice(sibling_value))

parent = dict(zip(parent_key, parent_value))
sibling = dict(zip(sibling_key, sibling_value))

f = open("randomsample.txt","w+")
for k,v in parent.items():
    string = 'parent('+k+','+v+').'
    f.write(string+"\n")

for k,v in sibling.items():
    string = 'sibling('+k+','+v+').'
    f.write(string+"\n")
    
for i in female:
    string = 'female('+i+').'
    f.write(string+"\n")

f.write('son(X,Y) :- parent(Y,Z), sibling(Z,X), not female(X).')


f.close()