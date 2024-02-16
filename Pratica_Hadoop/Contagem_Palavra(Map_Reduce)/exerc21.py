## Funcionando na versao do python2.7
import mincemeat
import glob
import csv

text_files = glob.glob('textos/*')

def file_contents(file_name):
    f=open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name,file_contents(file_name))for file_name in text_files) 

## implantando o  metodo map
def mapfn(k, v):
    print('map', k)
    from stopwords import allStopWords
    for line in v.splitlines():
        for word in line.split():
            if word not in allStopWords:
                yield word, 1

## implantando o metodo reduce
def reducefn(k, v):
    print('reduce', k)
    return sum(v)  

## Utilizando o mincemeat para simular o papel da DFS e do name node

s = mincemeat.Server()

#A fonte de dados pode ser qualquer objeto do tipo dicionario
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open("exerc\\RESULTS.csv", "w"))
for k, v in results.items():
    w.writerow([k, v])    
