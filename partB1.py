from mrjob.job import MRJob
class PartB1(MRJob):


    def mapper (self, _,line):
        try:
            fields = line.split(',')
            if len(fields) == 7:
                toaddress = (fields[2])
                value = int(fields[3])
                yield(toaddress,value)

        except:
            pass

    def combiner(self, toaddress, value):
        yield(toaddress,sum(value))


    def reducer(self, toaddress, value):
        yield(toaddress,sum(value))

if __name__ == '__main__':
    PartB1.run()
