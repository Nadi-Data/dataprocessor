import readcsv

obj1 = readcsv.Readcsv('/Users/sriyan/Downloads/sales.csv','r',1024,'\n')

for record in obj1.process_chunks():
    print(record[0]['Country'])