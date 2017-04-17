from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

# felony mis vio na
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    parking = sc.textFile(sys.argv[1], 1)
    parking = parking.mapPartitions(lambda x: reader(x))
    header = parking.first()
    lines = parking.filter(lambda line: line != header) \
                       .map(lambda x: (x[13],(1,0,0,0)) if x[11]=='FELONY' else (x[13],(0,1,0,0)) if x[11]=='MISDEMEANOR' else (x[13],(0,0,1,0)) if x[11]=='VIOLATION' else (x[13],(0,0,0,1)) ) \
                       .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3]))

    out = lines.map(lambda x: "%s\t%d, %d, %d, %d" %(x[0],x[1][0],x[1][1],x[1][2],x[1][3]))
    out.saveAsTextFile("location_CD.out")
    sc.stop()
