## Funcionando na versao do python2.7
import mincemeat
import glob
import csv

text_files = glob.glob('Join/*')

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
    for line in v.splitlines():
        if k== 'Join/2.2-vendas.csv':
            yield line.split(';')[0],'Vendas'+':'+ line.split(';')[5]
        if k== 'Join/2.2-filiais.csv':
            yield line.split(';')[0],'Filial'+':'+ line.split(';')[1]

## implantando o metodo reduce
def reducefn(k, v):
    print('reduce', k)
    total = 0
    for index, item in enumerate(v):
        if item.split(':')[0]=='Vendas':
            total += int(item.split(':')[1]) 
        elif item.split(':')[0]=='Filial':   
            NomeFilial = item.split(':')[1]
    L=list()
    L.append(NomeFilial + ', '+str(total))
    return L          

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
