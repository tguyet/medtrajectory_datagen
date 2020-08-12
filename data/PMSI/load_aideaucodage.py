import wget
import re
import os

import pandas as pd
import wget
import re
import os

import pandas as pd

import json

data=pd.read_csv('../cim_codes.csv')
data = data

i=1
outdata={}
for d in data.to_numpy():
    try:
        os.remove("data.html")
    except FileNotFoundError:
        pass
    cim=d[0].lower()
    print("process code ("+str(i)+"): '"+cim+"'")
    
    url = 'https://www.aideaucodage.fr/cim-'+cim
    print(url)
    try:
        wget.download(url, out="data.html")
    except:
        continue
        
    f=open('data.html',"r", encoding="latin-1")
    content = f.read()
    f.close()

    content=content.replace("\n","")
    content=content.replace("\t","")
    content=content.replace("\r","")
    
    #print("---- ACTES CCAM Liés ----")
    m=re.search("<h2>Actes CCAM(.*)</h2><table style=\"font-size:11px\">(.*)</table>(.*)<h2>",content)
    try:
        tabledata=m.group(2)
        m=re.findall("<td><b><a href=\"ccam-(\w+)\">(\w+?)</a></b></td><td>(['()\-,\w\s]+?)</td><td><div style=\"width:(\d+[.]{0,1}\d*)px; background-color:#(\w+);\">&nbsp;</div></td></tr>",tabledata, re.UNICODE)
        ccam={e[1]:(float(e[3])-2) for e in m}
        #print(ccam)
    except AttributeError:
        ccam={}

    #print("---- Diagnostics CIM Liés ----")
    
    try:
        m=re.search("<h2>Diagnostics CIM\-10 \(DAS\) associés à (\w*)</h2><table style=\"font-size:11px\">(.*)</table>",content, re.UNICODE)
        tabledata=m.group(2)
        m=re.findall("<td><b><a href=\"cim-(\w+)\">(\w+[.]{0,1}\w*)</a></b></td><td><span title=\"Niveau Sévérité\">(\d+)</span></td><td>(['()\-,\w\s]+?)</td><td><div style=\"width:(\d+[.]{0,1}\d*)px; background-color:#(\w+);\">&nbsp;</div></td></tr>",tabledata, re.UNICODE)
        cimd={e[1]:(float(e[4])-2) for e in m}
        #print(cimd)
    except AttributeError:
        cimd={}

    #print("---- GHM associés ----")
    try:
        m=re.search("<h2>GHM associés avec (\w*)</h2><table style=\"font-size:11px\">(.*)</table>",content, re.UNICODE)
        tabledata=m.group(2)
        m=re.findall("<td><b><a href=\"ghm-(\w+)\">(\w+)</a></b></td><td>(['()\-,\w\s]+?)</td><td><div style=\"width:(\d+[.]{0,1}\d*)px; background-color:#(\w+);\">&nbsp;</div></td></tr>",tabledata, re.UNICODE)
        ghm={e[1]:(float(e[3])-2) for e in m}
        #print(ghm)
    except AttributeError:
        ghm={}
    
    outdata[cim.upper()]={"ccam":ccam,"cim":cimd,"ghm":ghm}

    if i%100==0:
        with open('result.json', 'w') as fp:
            json.dump(outdata, fp)
    i+=1