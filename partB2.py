from mrjob.job import MRJob

class PartB2(MRJob):

    def mapper(self, _, line):
        try:
            if len(line.split(',')) == 5 :
                fields=line.split(',')
                joinkey=fields[0]
                joinvalue=fields[3]
                yield(joinkey,(joinvalue,1))

            if len(line.split('\t'))==2:
                fields=line.split('\t')
                joinkey=fields[0]
                joinkey=joinkey[1:-1]
                joinvalue=fields[1]
                yield(joinkey, (joinvalue,2))

        except:
            pass

    def reducer(self,address,values):
        block_number=0
        counts=0
        for value in values:
            if value[1]==1:
                block_number=value[0]
            if value[1]==2:
                counts=value[0]
        if block_number>0 and counts>0:
            yield(address,counts)

if __name__ =='__main__':
    PartB2.run()
