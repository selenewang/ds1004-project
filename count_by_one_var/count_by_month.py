from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    parkings = sc.textFile(sys.argv[1], 1)
    parking = parking.mapPartitions(lambda x: reader(x))
    header = parking.first()
    lines = parking.filter(lambda line: line != header) \
                       .map(lambda x: (x[1][:2], 1)) \
		       .reduceByKey(lambda x,y: x+y) \
		       .sortBy(lambda x: x[0]) \
                       .map(lambda x: "%s\t%s" % (x[0],x[1]))
    lines.saveAsTextFile("countbymonth.out")
    sc.stop()
