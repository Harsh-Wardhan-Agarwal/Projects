
#  # Naive Bayes Classifier using Laplace Smoothing

# ## Loading the csv file into a DataFrame :
import pandas as pd
import numpy as np
df = pd.read_csv('Q2-tennis.csv')
print(df)

# ## Forming a DataFrame containing Row (intersection) Column values from 'df'
df.rename(columns={'Temp.': 'Temp'}, inplace=True)
values = {}
play = df.Play.unique()

#Forming a Dictionary containing 'keys' as unique values in categories, 'values' as number of unique values in the key's category
features = [df.Outlook.unique(),df.Temp.unique(),df.Humidity.unique(),df.Windy.unique()]
for i in features:
    for j in i:
        values[j] = len(i)
        
# Forming a DataFrame containing Row<intersection>Column values from 'df'
df2 = pd.DataFrame(index = values, columns = ['yes','no'])
for i in df.Outlook.unique():
    for j in play:
        df2.at[i,j] = df[(df.Outlook==i) & (df.Play==j)].Play.count()
for i in df.Temp.unique():
    for j in play:
        df2.at[i,j] = df[(df.Temp==i) & (df.Play==j)].Play.count()
for i in df.Humidity.unique():
    for j in play:
        df2.at[i,j] = df[(df.Humidity==i) & (df.Play==j)].Play.count()
for i in df.Windy.unique():
    for j in play:
        df2.at[i,j] = df[(df.Windy==i) & (df.Play==j)].Play.count()
print(df2)

# ## Forming the Probability table:
p_yes = df[df.Play=='yes'].Play.count() # Probability of 'yes' in Play category
p_no = df[df.Play=='no'].Play.count()   # Probability of 'no' in Play category

# Forming the Probability DataFrame (Using Laplace Smoothing)
prob = pd.DataFrame(index = values, columns = ['yes','no'])
for column,row in prob.iterrows():
    prob.at[column,'no'] = (df2.at[column,'no']+1)/(p_no+values[column])    # Laplace Smoothing Formula
    prob.at[column,'yes'] = (df2.at[column,'yes']+1)/(p_yes+values[column])
print(prob)

# ## Testing the results based on User Input:
print("Choose the scenerio:\n")
outlook_input = input("OUTLOOK (sunny,overcast,rainy):")
temp_input = input("TEMPERATURE (hot,mild,cool):")
humidity_input = input("HUMIDITY (normal,high):")
windy_input = input("WINDY (true,false):")

# Calculating the probabilities based on user input
prob_yes = prob.at[outlook_input,'yes']*prob.at[temp_input,'yes']*prob.at[humidity_input,'yes']*prob.at[windy_input+' ','yes']
prob_no = prob.at[outlook_input,'no']*prob.at[temp_input,'no']*prob.at[humidity_input,'no']*prob.at[windy_input+' ','no']
print("\n-------------------------------------------\n")

# Comparing the calculated probabilities
if prob_yes > prob_no:
    print("Yes")           # Printing the final Decision
else:
    print("No")

