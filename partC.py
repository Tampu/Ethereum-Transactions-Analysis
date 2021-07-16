import pyspark
import time
sc=pyspark.SparkContext()

def clean_transactions(line):
    try:
        fields = line.split(',')
        if len(fields)!=7:
            return False

        int(fields[6])
        int(fields[3])

        return True

    except:
        return False

transactions = sc.textFile('/data/ethereum/transactions')
trans_filtered = transactions.filter(clean_transactions)
transactions_join = trans_filtered.map(lambda l: (l.split(',')[2] , (int(l.split(',')[6]), int(l.split(',')[3])))).persist()

scams = sc.textFile('/user/rkt31/scams.csv')
join_scams = scams.map(lambda f: (f.split(',')[0],f.split(',')[5]))
join_trans_scams = transactions_join.join(join_scams)


scam_category = join_trans_scams.map(lambda a: (a[1][1], a[1][0][1]))
scam_category_count = scam_category.reduceByKey(lambda a,b: (a+b)).sortByKey()
scam_category_count.saveAsTextFile('most_lucrative_scam')

time_analysis = join_trans_scams.map(lambda b: ((b[1][1], time.strftime("%m-%Y",time.gmtime(b[1][0][0]))), b[1][0][1]))
output = time_analysis.reduceByKey(lambda a,b: (a+b)).sortByKey()
output.saveAsTextFile('timeanalysis')
