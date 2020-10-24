import pandas as pd 
import numpy as np 
import glob

path = r"/Users/Daues/Documents/School/Columbia/Coursework/MSYear2/hackathon_stanford/social_mobility_Data"
filenames = glob.glob(path + "/*.csv")


dfs = []
for filename in filenames:
    dfs.append(pd.read_csv(filename))

# Concatenate all data into one DataFrame
big_frame = pd.concat(dfs, ignore_index=True)

big_frame.to_csv('mobility_summer.csv')