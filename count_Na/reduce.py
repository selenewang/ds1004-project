#!/usr/bin/env python
import sys
import string

count_na = [0]*24
count_correct = [0]*24

for line in sys.stdin:
    state = line.strip()
    entry = line.split("\t")
    var0 = entry[0]
    entry_value = entry[1].split(";")

    if var0 == "":
        count_na[0] += 1
    else:
        count_correct[0] += 1


    for i in range(len(entry_value)):
        if entry_value[i] == "":
            count_na[i+1] += 1
        else:
            count_correct[i+1] += 1

print count_na
print count_correct
    
