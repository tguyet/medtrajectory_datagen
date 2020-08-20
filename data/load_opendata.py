#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script for (partly) automatic downloads of open datasets required for the simulator

@author: Thomas Guyet, Institut Agro/IRISA
@date: 08/2020
"""

import wget
import zipfile
import gzip
import sys
import os
import warnings

sources={
    "https://www.insee.fr/fr/statistiques/fichier/1893204/pop-sexe-age-quinquennal6816.zip": {
        "description":"Population dataset",
        "compress":"zip",
        "filename":"pop-sexe-age-quinquennal6816.xls"
        },
    "https://www.data.gouv.fr/fr/datasets/r/3dc9b1d5-0157-440d-a7b5-c894fcfdfd45":{
        "description":"Données FINESS sur les etablissements de soins",
        "compress":None,
        "filename":"finess-clean.csv"
        },
    "https://www.scansante.fr/sites/default/files/content/445/open_ccam_16.zip": {
        "description":"Open CCAM dataset",
        "compress":"zip",
        "filename":"Open_ccam_16.csv"
        },
    "https://www.ameli.fr/fileadmin/user_upload/documents/Actes_techniques_de_la_CCAM_en_2016.xls":{
        "description":"Open CCAM dataset",
        "compress":None,
        "filename":"Actes_techniques_de_la_CCAM_en_2016.xls"
        },
    "https://www.data.gouv.fr/fr/datasets/r/296394b6-d539-4cc7-a440-2698eec06c18":{
        "description":"Liste des praticiens de Santé",
        "compress":None,
        "filename":"ps-infospratiques.csv"
        },
    "https://www.data.gouv.fr/fr/datasets/r/88d48234-4330-4eed-835d-ef83220ea145":{
        "description":"Open DAMIR : Fichier A",
        "compress":"gz",
        "filename":"A201601.CSV"
        },
    "https://www.data.gouv.fr/fr/datasets/r/554590ab-ae62-40ac-8353-ee75162c05ee":{
        "description":"Base officielle des codes postaux",
        "compress":None,
        "filename":"laposte_hexasmal.csv"
        },
    "https://www.data.gouv.fr/fr/datasets/r/914c34c6-22bb-484b-b4a4-e2262ea10d65":{
        "description":"Open DAMIR : Fichier R",
        "compress":"zip",
        "filename":"R201601.CSV"
        },
    "http://open-data-assurance-maladie.ameli.fr/medicaments/download_file.php?file=Open_MEDIC_Base_Complete/OPEN_MEDIC_2019.zip":{
        "description":"Open MEDIC dataset",
        "compress":None, #keep it in zip!
        "filename":"OPEN_MEDIC_2019.zip"
        },
    }

manual_downloads=[] 

for source,infos in sources.items():
    print("Data source: "+infos["description"])
    
    if os.path.exists( infos["filename"] ):
        print("\t data file already exists: skip it!")
        continue
    
    if infos["compress"]=="zip":
        try:
            try:
                os.remove("data.zip")
            except FileNotFoundError:
                pass
            
            print("\tDownload")
            wget.download(source, out="data.zip")
            print("\n\tUnzip file (ZIP)")
            with zipfile.ZipFile("data.zip", 'r') as zip_ref:
                zip_ref.extract(infos["filename"])
        except:
            warnings.warn("ERROR")
            manual_downloads.append(source)
    elif infos["compress"]=="gz":
        try:
            try:
                os.remove("data.gz")
            except FileNotFoundError:
                pass
            
            print("\tDownload")
            wget.download(source, out="data.gz")
            print("\n\tUnzip file (GZ)")
            with zipfile.ZipFile("data.gz", 'r') as zip_ref:
                zip_ref.extract(infos["filename"])
        except:
            warnings.warn("ERROR")
            manual_downloads.append(source)
    else:
        try:
            print("\tDownload")
            wget.download(source, out=infos["filename"])
            print("\n")
        except:
            warnings.warn("ERROR")
            manual_downloads.append(source)

print("---- FINISHED ----")
if len(manual_downloads)>0:
    print("remaining downloads (try them manually):")
    for d in manual_downloads:
        print("\t* "+str(d))
        

try:
    os.remove("data.zip")
    os.remove("data.gz")
except FileNotFoundError:
    pass
