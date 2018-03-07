
# coding: utf-8

# In[ ]:

from csv import reader
import time
import datetime


# In[ ]:
sc = SparkContext()
data = sc.textFile('/Users/Selene/1004/NYPD_Complaint_Data_Historic.csv').mapPartitions(lambda x:reader(x))
header = data.first()
lines = data.filter(lambda line: line != header)

num_samples = lines.count()
num_features = len(header)


# In[ ]:

class Checker:
    
    def __init__(self, val, col):
        self.val = val
        self.col = col
    
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
            cast = time.strptime(self.val, '%H:%M:%S')
            if cast.tm_sec < 60:
                return True
            else:
                return False
        except:
            return False
    
    

    def check_date(self):
        try:
            cast = datetime.datetime.strptime(self.val, '%m/%d/%Y')
            if 2006 <= cast.year <= 2016:
                return True
            else:
                return False
        except:
            return False
    
    
    def check_tuple(self):
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
        elif self.col in [0, 6, 8, 14, 19, 20]:
            if self.check_int():
                return("INT")
            else:
                return("INVALID")
        elif self.col in [1, 3, 5]:
            if self.check_date():
                return("DATETIME")
            else:
                return("INVALID")
        elif self.col in [2, 4]:
            if self.check_time():
                return("TIME")
            else:
                return("INVALID")
        elif self.col in [7, 9, 10, 11, 12, 13, 15, 16, 17, 18]:
            return("TEXT")
        elif self.col in [21, 22]:
            if self.check_float():
                return("DECIMAL")
            else:
                return("INVALID")
        elif self.col == 23:
            if self.check_tuple():
                return("TUPLE")
            else:
                return("INVALID")
            
            
    def semantic_type(self, aux = None):
        if self.val == '':
            return("NULL")
        elif self.base_type() == "INVALID":
            return("INVALID")
        elif self.col == 0:
            if len(self.val) == 9:
                return("ID")
            else:
                return("INVALID")
        elif self.col == 1:
            if aux == '':
                return("Exact Date")
            else:
                return("Start Date")
        elif self.col == 2:
            if aux == '':
                return("Exact Time")
            else:
                return("Start Time")
        elif self.col == 3:
            return ("Ending Date")
        elif self.col == 4:
            return ("Ending Time")
        elif self.col == 5:
            return ("Report Date")
        elif self.col == 6 :
            if len(self.val) == 3:
                return("Key code")
            else:
                return("INVALID")
        elif self.col == 7:
            return("Key code desc")
        elif self.col == 8 :
            if len(self.val) == 3:
                return("PD code")
            else:
                return("INVALID")
        elif self.col == 9:
            return("PD code desc")
        elif self.col ==10:
            if self.val in ['COMPLETED', 'ATTEMPTED']:
                return("Crime Ind")
            else:
                return("INVALID")
        elif self.col ==11:
            if self.val in ['FELONY', 'VIOLATION', 'MISDEMEANOR']:
                return("Offense Lev")
            else:
                return("INVALID")
        elif self.col == 12:
            return("Jurisdiction")
        elif self.col == 13:
            if self.val in ['STATEN ISLAND', 'BROOKLYN', 'QUEENS', 'MANHATTAN', 'BRONX']:
                return("Borough")
            else:
                return("INVALID")
        elif self.col == 14 :
            if len(self.val) <= 3:
                return("Precinct code")
            else:
                return("INVALID")
        elif self.col == 15:
            if self.val in ['OUTSIDE', 'FRONT OF', 'REAR OF', 'OPPOSITE OF', 'INSIDE']:
                return("Location")
            else:
                return("INVALID")
        elif self.col == 16:
            return("Location Desc")
        elif self.col == 17:
            return("Park")
        elif self.col == 18:
            return("Housing Dev")
        elif self.col in [19, 20]:
            return("Coordinate")
        elif self.col == 21:
            temp = aux.strip("()")
            temp = str.split(temp, ",")
            if float(self.val) - float(temp[0]) < 1e-3:
                return("Coordinate")
            else:
                return("INVALID")
        elif self.col == 22:
            temp = aux.strip("()")
            temp = str.split(temp, ",")
            if float(self.val) - float(temp[1]) < 1e-4:
                return("Coordinate")
            else:
                return("INVALID")
        elif self.col == 23:
            temp = self.val.strip("()")
            temp = str.split(temp, ",")
            if 40.477399 < float(temp[0]) < 40.917577 and -74.259090 < float(temp[1]) < -73.700272:
                return("Coordinate")
            else:
                return("INVALID")
        


# In[ ]:

def label(tp):
    if tp == "INVALID" or tp =="NULL":
        return(tp)
    else:
        return("VALID")


# In[ ]:

for i in range(num_features):
    if i == 1 or i == 2:
        result = lines.map(lambda x : (x[i], (Checker(x[i], i).base_type(), Checker(x[i], i).semantic_type(x[i+2]), label(Checker(x[i], i).semantic_type(x[i+2])))))
    elif i == 21 or i == 22:
        result = lines.map(lambda x : (x[i], (Checker(x[i], i).base_type(), Checker(x[i], i).semantic_type(x[23]), label(Checker(x[i], i).semantic_type(x[23])))))
    else:
        result = lines.map(lambda x : (x[i], (Checker(x[i], i).base_type(), Checker(x[i], i).semantic_type(), label(Checker(x[i], i).semantic_type()))))
    result.saveAsTextFile("/Users/Selene/1004/"+str(i)+" "+header[i]+".txt")


# In[ ]:



