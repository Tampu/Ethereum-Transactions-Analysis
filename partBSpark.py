import pyspark

sc = pyspark.SparkContext()

def clean_transactions(trans):
    try:
        fields = trans.split(',')
        if len(fields)!=7:
            return False
        int(fields[3])
        return True
    except:
        return False


def clean_contracts(contract):
    try:
        fields = contract.split(',')
        if len(fields)!=5:
            return False
        return True
    except:
        return False

transactions = sc.textFile("/data/ethereum/transactions")
trans_filtered = transactions.filter(clean_transactions)
address=trans_filtered.map(lambda l: (l.split(',')[2], int(l.split(',')[3]))).persist()
job1output = address.reduceByKey(lambda a,b:(a+b))
job1output_join=job1output.map(lambda f:(f[0], f[1]))

contracts = sc.textFile("/data/ethereum/contracts")
contracts_filtered = contracts.filter(clean_contracts)
contracts_join = contracts_filtered.map(lambda f: (f.split(',')[0],f.split(',')[3]))

job2output =job1output_join.join(contracts_join)

top10=job2output.takeOrdered(10, key = lambda x:-x[1][0])
for record in top10:
    print("{}: {}".format(record[0],record[1][0]))
