import glob

path = r"/Users/Daues/Documents/School/Columbia/Coursework/MSYear2/hackathon_stanford/social_mobility_Data"
filenames = glob.glob(path + "/*.csv")
count = 0
with open("social_mobility_Data/combined/combined.csv","a+") as targetfile:
    for filename in filenames:
        count+=1
        print(count)
        with open(filename,'r') as f:
            next(f)
            for line in f:
                targetfile.write(line)

