
# coding: utf-8

# In[1]:

from csv import reader
import time
import datetime


# In[2]:

data = sc.textFile('/Users/Selene/1004/NYPD_Complaint_Data_Historic.csv').mapPartitions(lambda x:reader(x))
header = data.first()
lines = data.filter(lambda line: line != header)

num_samples = lines.count()
num_features = len(header)


# In[3]:

col_7 = lines.map(lambda x : x[7]).distinct().collect()
col_9 = lines.map(lambda x : x[9]).distinct().collect()
offense_desc= list(set().union(col_7,col_9))

crime_ind = ['COMPLETED', 'ATTEMPTED']

offense_lev = ['FELONY', 'VIOLATION', 'MISDEMEANOR']

jurisdiction = lines.map(lambda x : x[12]).distinct().collect()

col_13 = lines.map(lambda x : x[13]).distinct().collect()
col_15 = lines.map(lambda x : x[15]).distinct().collect()
col_16 = lines.map(lambda x : x[16]).distinct().collect()
col_17 = lines.map(lambda x : x[17]).distinct().collect()
col_18 = lines.map(lambda x : x[18]).distinct().collect()
location = list(set().union(col_13,col_15, col_16, col_17, col_18))


# In[4]:

class Checker:
    
    def __init__(self, val):
        self.val = val
    
    def check_int(self):
        try:
            int(self.val)
            return True
        except:
            return False
    
    def check_float(self):
        try:
            float(self.val)
            return True
        except:
            return False
    
    def check_time(self):
        try:
            time.strptime(self.val, '%H:%M:%S'+"\t"+'PM' or '%H:%M:%S'+"\t"+'AM')
            return True
        except:
            return False
    
    def check_date(self):
        try:
            datetime.datetime.strptime(self.val, '%m/%d/%Y')
            return True
        except:
            return False
    
    def check_coordinate(self):
        try:
            temp = self.val.strip("()")
            temp = str.split(temp, ",")
            try:
                for i in range(len(temp)):
                    float(temp[i])
                return True
            except:
                return False
        except:
            return False
            
    
    def base_type(self):
        if self.val == '':
            return("NULL")
        elif self.check_int():
            return("INT")
        elif self.check_float():
            return("DECIMAL")
        elif self.check_time():
            return("TIME")
        elif self.check_date():
            return("DATETIME")
        elif self.check_coordinate():
            return("TUPLE")
        else:
            return ("TEXT")
    
    def semantic_type(self):
        if self.val == '':
            return("NULL")
        elif self.check_int() and len(self.val) == 9:
            return("ID")
        elif self.check_int() and len(self.val) <= 3:
            return("CODE")
        elif self.check_time():
            return("TIME")
        elif self.check_date():
            return("DATETIME")
        elif self.val in offense_desc:
            return("Offense Description")
        elif self.val in crime_ind:
            return("Crime Type")
        elif self.val in offense_lev:
            return("Offense Level")
        elif self.val in location:
            return("Location")
        elif self.check_int() or self.check_float() or self.check_coordinate():
            return("Coordinate")
        else:
            return("Unknown")
    
    def label(self):
        if self.val == '':
            return("NULL")
        elif self.semantic_type() == "Unknown":
            return("INVALID")
        else:
            return("VALID")
        
        
        


# In[7]:

for i in range(num_features):
    result = lines.map(lambda x : x[i])    .map(lambda x : (x, (Checker(x).base_type(), Checker(x).semantic_type(), Checker(x).label())))
    result.saveAsTextFile("/Users/Selene/1004/"+header[i]+".txt")


# In[ ]:



