Tourism (file name: tourism.pl) 

Connor intends to plan a tour of Melbourne, accompanied by a group of colleagues. In order to visit the most desirable locations, he has asked the group members to rank the possible locations in order of preference. However, upon collating the data, he finds each colleague has stated preferences for only some of the locations.

Given a set L of locations, and partial rankings R = [R1, ..., Rn], we want to find the ordering of L which is most consistent with R. That is, the ordering of L which minimizes the sum of the number times each partial ranking is violated. Note that if several people have a violated preference, it is counted for each person. Similarly, preferences are transitive: so given a ranking [1, 2, 3], the ordering [3, 2, 1] has 3 violations.

INPUT FORMAT
An input file contains the following facts:

One fact people(N), specifying the number of people.
One fact locations(M), specifying the number of locations.
One fact preferences(K), giving the number of stated preferences.
A relation consisting of facts of the form order(P, X, Y), that person P ranked location X above location Y.
Output format

The output should contain exactly one fact of the form violations(V), where V is the number of preference violations of the optimal ranking.

EXAMPLES

Input:

people(2). 
locations(4). 
preferences(4).  
order(1, 1, 2). 
order(1, 2, 4). 
order(2, 1, 3). 
order(2, 3, 4).

Output:

violations(0).
