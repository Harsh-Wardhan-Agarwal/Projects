from __future__ import division
import pandas as pd
import numpy as np
import string
import math
import sys
import os


"""TRAINING MODEL"""

# Reading Training data from the 'train' file.
with open(sys.argv[2]) as f:
    content = f.readlines()
content = [x.strip() for x in content]

# Splitting data into emailID, label, string of words and their frequency by " " delimiter.
content1 = []
for i in content:
    content1.append(i.split(" ",2))

# Loading the training data into pandas dataframe.
train_data = pd.DataFrame(content1,columns = ['email_id','label','words'])

#Calculating prior probabilities for Spam and Ham.
spam_prob = train_data.label.value_counts()[0]/train_data.shape[0]
ham_prob = train_data.label.value_counts()[1]/train_data.shape[0]

# Separating Spam emails and Ham emails into two DataFrames.
spam_data = train_data.loc[train_data['label'] == 'spam']
ham_data = train_data.loc[train_data['label'] == 'ham']

# Spam and Ham lists containing only the string of words and their frequency content of every email.
spam_list = list(spam_data.words)
ham_list = list(ham_data.words)

# Creating a dictionary for Spam emails as: spam_words[word] = frequency_of_the_word_in_spam_emails.
spam_words = {}
for email in spam_list:
    email = email.split(' ')
    words, count = email[::2], email[1::2]
    for i in range(len(words)):
        if words[i] in spam_words:
            spam_words[words[i]] += int(count[i])
        else:
            spam_words[words[i]] = 0
            spam_words[words[i]] += int(count[i])

# Creating a dictionary for Ham emails as: ham_words[word] = frequency_of_the_word_in_ham_emails.
ham_words = {}
for email in ham_list:
    email = email.split(' ')
    words, count = email[::2], email[1::2]
    for i in range(len(words)):
        if words[i] in ham_words:
            ham_words[words[i]] += int(count[i])
        else:
            ham_words[words[i]] = 0
            ham_words[words[i]] += int(count[i])

# Calculating all the distinct words in the training dataset.
distinct = len(set(list(spam_words) + list(ham_words)))

# Total number of distinct words in Spam and Ham emails.
total_spam_words = sum(spam_words.values())
total_ham_words = sum(ham_words.values())



""" TESTING MODEL """

# Reading testing data from the 'test' file.
with open(sys.argv[4]) as f:
    content_test = f.readlines()
content_test = [x.strip() for x in content_test]

# Splitting data into emailID, label, string of words and their frequency by " " delimiter.
content2 = []
for i in content_test:
    content2.append(i.split(" ",2))

# Loading the testing data into pandas dataframe.
test_data = pd.DataFrame(content2,columns = ['email_id','label','words'])

# List containing only the string of words and their frequency content of every email.
email_list = list(test_data.words)

prediction = []
for email in email_list:
    # Creating a dictionary for emails as: email_pairs[word] = frequency_of_the_word_in_emails.
    email_pairs = {}
    test1 = email.split(' ')
    test_words = test1[::2]
    email_pairs = dict(zip(test_words, [int(i) for i in test1[1::2]]))

    # Calculating Conditional Probability P(Word/Spam)
    # Since, multiplying probabilities become zero after a point of time,
    # we apply log and add the probabilities to get a proper value.
    # [P(Word1/Spam)*P(Word2/Spam)*....] = [log(P(Word1/Spam))+log(P(Word2/Spam))+....]

    probspam = math.log(spam_prob)
    for word in test_words:                         # For each word
        for i in range(email_pairs[word]):          # Number of times the word occurred
            probspam += math.log((spam_words.get(word,0)+10)/float((total_spam_words)+distinct*10))
            # The number '10' in the above formula epresents the Laplacian smoothing paramter alpha.

    probham = math.log(ham_prob)
    for word in test_words:
        for i in range(email_pairs[word]):
            probham += math.log((ham_words.get(word,0)+10)/float((total_ham_words)+distinct*10))

    # Classifying prediction as Spam or Ham.
    if probspam > probham:
        prediction.append('spam')
    else:
        prediction.append('ham')

# Writing the predicted output to 'output.csv' file.
prediction_collect = pd.Series(prediction)
id = pd.Series(test_data.email_id)
output = pd.concat([id, prediction_collect], axis=1)
output.to_csv(sys.argv[6],index=False, header=False)