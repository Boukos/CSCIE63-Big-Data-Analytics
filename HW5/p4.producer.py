from itertools import islice

filename="test.txt"
N=100

with open(filename, 'r') as infile:
    for lines in grouper(f, 100, ''):
        assert len(lines) == 100


#        lines_gen = islice(infile, 100)
#        for line in lines_gen:
#            print line
#            break
