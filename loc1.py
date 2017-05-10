from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

def check(location,sub_loc):
    if ((location[0]-sub_loc[0])**2+(location[1]-sub_loc[1])**2)**(0/5)<0.01:
        return "yes"
    else:
        return "no"


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    loc1 = sc.textFile(sys.argv[1], 1)
    loc1 = loc1.mapPartitions(lambda x: reader(x))\
		.map(lambda x: (x[3][7:-1].split()[0], x[3][7:-1].split()[1]))

    crime = sc.textFile(sys.argv[2], 1)
    crime = crime.mapPartitions(lambda x: reader(x))\
		 .map(lambda x: (x[24][1:-1].split()[0], x[24][1:-1].split()[1])) 
 
    result={}
    for i in range(loc1.count()): 
	result[i] = crime.map(lambda x: (check(x,loc1[i]),1)) \
			 .reduceByKey(lambda x,y: x+y)\
			 .map(lambda x: "%s\t%d" % (x[0], x[1]))

    out = sc.parallelize(result) 
    
    out.saveAsTextFile("location.out")
    sc.stop()
