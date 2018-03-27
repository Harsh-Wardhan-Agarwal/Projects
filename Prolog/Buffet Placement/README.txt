Buffet placement (file name: buffet.pl)
-----------------------------------------
Task:
Leigh is sorting out the placement of dishes for a conference buffet, and must decide how many tables are needed to place the dishes. Each dish is either hot or cold, and occupies a fixed width of the table. Additionally, a hot and a cold dish may not be placed immediately next to each other; they must be separated by a minimum distance. The venue has only provides one size of table.

Given the number of each dish type to be served, the objective is to find the minimum number of tables necessary to serve all dishes.

INPUT FORMAT
An input file contains the following:

One fact of the form dishes(N), specifying that there are N kinds of dishes.
One fact hot(H), specifying that the first H dishes are hot. Thus dishes 1..H are hot, and dishes H+1..N are cold.
One fact of the form separation(S), specifying the minimum distance D between hot and cold foods.
One fact table_width(L) indicating the width of a buffet table.
A relation consisting of facts of the form dish_width(D, W), which specifies that a dish of kind D has width W.
A relation consisting of facts of the form demand(D, Q), specifying the number Q of dishes of kind D that we require.
Output format

The output should contain exactly one fact of the form tables(K). where K is the minimum number of tables required to serve the required dishes.

EXAMPLE

Input:

dishes(3). 
separation(1). 
hot(1). 
table_width(4).  
dish_width(1, 1). 
dish_width(2, 1). 
dish_width(3, 2).  
demand(1, 1). 
demand(2, 1). 
demand(3, 1).

Output:

tables(2).
