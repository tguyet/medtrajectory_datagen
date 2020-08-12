import wget
import re
import os

import pandas as pd

data=pd.read_csv('cim_codes.csv')

data = data[1000:2000]

cimoccs=[]
cims=[]

for d in data.to_numpy():
#for d in [['A020']]:
    cim=d[0]
    print("process code: '"+cim+"'")

    url = 'https://www.scansante.fr/applications/statistiques-activite-MCO-par-diagnostique-et-actes/outilExcel?_program=mcoprog.affiche_cataghm.sas&base=deux&typt=cim&annee=2015&code='+cim
    print(url)
    wget.download(url, 'data.xml')

    f=open('data.xml',"r")
    found=0
    for line in f.readlines(): 
        if found:
            linetotal = line.strip()
            break
        if "Total" in line:
            found=1
        elif "secret statistique" in line:
            cimoccs.append(0)
            cims.append(cim)
            print("\t-> secret statistique (0)")
            found=2
            break
    f.close()
    os.remove("data.xml")
    if found==0:
        print("\t-> no observation")
    elif found==1:
        m = re.search('(.*)>(\s*[ 0-9]+)</td>', linetotal)
        if not m is None:
            nb=int(m.group(2).replace(" ",""))
            cimoccs.append(nb)
            cims.append(cim)
            print("\t-> "+str(nb))
        else:
            print("\t-> parsing error ('"+linetotal+"')")

cimcounts=pd.DataFrame({"cim":cims,"count":cimoccs})

cimcounts.to_csv("cimcounts.csv")
