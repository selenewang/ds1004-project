## This is the project for DS1004 Big Data
Count_Na: check the number of na in each column, write with MapReduce
- Note: This program give a roughly idea on how many na each column have, please run the Map.ipynb for detail count

count_by_one_var: Prepared data for plotting, containing count by crime type, count by big crime type, count by date, count by month, count by year. 
- to run the program in dumbo, type:

```python
spark-submit program_name.py /user/zl1732/nypd.csv
```

>count_by_two_var: Prepared data for plotting, containing count of location vs crime type, count of year vs crime type, count of month vs crime. 
- output format: key  count of felony count of mis, count of violation, count of na. 
To run the program in dumo, type:
```python
spark-submit program_name.py /user/zl1732.nypd
```

