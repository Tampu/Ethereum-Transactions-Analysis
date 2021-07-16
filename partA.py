import pyspark
import re
import time
sc = pyspark.SparkContext()

def clean_transactions(line):#we will use this function later in our filter transformation
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False
        float(fields[6])
        return True
    except:
        return False

def get_monthYear(line):

    fields=line.split(',')
    timestamp = int(fields[6])
    monthYear = time.strftime("%m-%Y",time.gmtime(timestamp))
    return(monthYear,1)


transactions = sc.textFile("/data/ethereum/transactions")


clean_lines = transactions.filter(clean_transactions)
keyMY=clean_lines.map(get_monthYear).persist()
output=keyMY.reduceByKey(lambda a,b: a+b).sortByKey()

inmem=output.persist()
inmem.saveAsTextFile("/user/rkt31/partAoutput1")
