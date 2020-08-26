
import os
import pandas as pd
import json

rootsnomencl="../external/schema-snds-master/nomenclatures"

#nomenclature

#Suppression de la date vide !
dataset=pd.read_csv( os.path.join(rootsnomencl,"ORAVAL/IR_DTE_V.csv"), sep=";")
dataset.dropna(inplace=True)
dataset.to_csv( os.path.join(rootsnomencl,"ORAVAL/IR_DTE_V.csv"), sep=";", index=False )


#HACK: on vire cette partie de la base
# suppression de IR_MIR_D.json et de IR_MIR_V.json -> problème avec des entrées avec des entiers trop longs !
#dataset=pd.read_csv( os.path.join(rootsnomencl,"ORAVAL/IR_DTE_V.csv"), sep=";")
#dataset.dropna(inplace=True)
#dataset.to_csv( os.path.join(rootsnomencl,"ORAVAL/IR_DTE_V.csv"), sep=";", index=False )

with open("../external/schema-snds-master/schemas/DCIR/ER_PRS_F.json", "r") as read_file:
    data = json.load(read_file)

for f in data["fields"]:
	if f['name']=="PFS_EXE_NUM" or f['name']=="PFS_PRE_NUM" or f['name']=="PRS_MTT_NUM" or f['name']=="PSP_PPS_NUM" or f['name']=="ETB_PRE_FIN":
		try:
			del(f["constraints"])
		except:
			continue

with open("../external/schema-snds-master/schemas/DCIR/ER_PRS_F.json", "w") as write_file:
    json.dump(data, write_file, indent=2)

