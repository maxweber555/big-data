import mincemeat
import glob
import csv

text_files = glob.glob('exerc\\textos\\*')

def file_contents(file_name):
    f=open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name,file_contents(file_name))for file_name in text_files) 

for t in source:
    print(t)
