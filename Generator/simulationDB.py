# -*- coding: utf-8 -*-

from simulation import simulation
from database_model import Patient, GP, Specialist, Provider, DrugDelivery

import sqlalchemy as sa
from tableschema import Table
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy import inspect
from datetime import date, datetime
from shutil import copyfile

class simDB(simulation):
    def __init__(self):
        super().__init__()
    


    def generate(self):
        rootschemas="/home/tguyet/Progs/SNDS/schemas"
        #rootsnomencl="/home/tguyet/Tools/SNDS/synthetic-snds/nomenclatures"
        
        copyfile("/home/tguyet/Progs/SNDS/snds_nomenclature.db", "/home/tguyet/Progs/SNDS/snds_testgen.db")
        db = sa.create_engine('sqlite:////home/tguyet/Progs/SNDS/snds_testgen.db')
        
        
        ########  Physicians #############
        # Get table information
        inspector = inspect(db)
        tables=inspector.get_table_names()
        
        # Create the table if necessary
        if "DA_PRA_R" not in tables:
            fjson=rootschemas+"/DCIR_DCIRS/DA_PRA_R.json"
            table=Table([], schema=fjson)
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
        
        Base = automap_base()
        Base.prepare(db, reflect=True)
        
        session = sessionmaker()
        session.configure(bind=db)
        s = session()
        for p in self.GPs:
            s.add( self.createInsert_PS(Base, p)[0] )

        for p in self.specialists:
            s.add( self.createInsert_PS(Base, p)[0] )
            
        for p in self.pharms:
            s.add( self.createInsert_PS(Base, p)[0] )
        s.commit()
        
        # Create the table if necessary
        modified=False
        if "IR_BEN_R" not in tables:
            fjson=rootschemas+"/BENEFICIAIRE/IR_BEN_R.json"
            table=Table([], schema=fjson)
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "IR_IBA_R" not in tables:
            fjson=rootschemas+"/BENEFICIAIRE/IR_IBA_R.json"
            table=Table([], schema=fjson)
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "CT_IDE_AAAA_GN" not in tables:
            fjson=rootschemas+"/CARTOGRAPHIE_PATHOLOGIES/CT_IDE_AAAA_GN.json"
            table=Table([], schema=fjson)
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True

        if modified:
            Base = automap_base()
            Base.prepare(db, reflect=True)
            #redefinition of the foreign key ... I remove the "foreign(...)" to have a read-only relation to Table IR_BEN_R
            Base.classes.CT_IDE_AAAA_GN.ir_ben_r = relationship("IR_BEN_R", primaryjoin="and_(IR_BEN_R.BEN_NIR_PSA == foreign(CT_IDE_AAAA_GN.ben_nir_psa), IR_BEN_R.BEN_RNG_GEM == CT_IDE_AAAA_GN.ben_rng_gem)")

        
        s = session()
        for p in self.patients:
            for pi in self.createInsert_ben(Base, p):
                s.add( pi )
        s.commit()
        
        
    def createInsert_PS(self, Base, p):
        professional = Base.classes.DA_PRA_R(
                        DTE_MOI_FIN = "",#"s" ,
                        DTE_ANN_TRT = "",#"haHoKysholbSI" ,
                        PFS_PFS_NUM = p.id,
                        TRT_SNI_COD = "",#"cz" ,
                        IPP_IDV_NUM = "",#"KvGzDJyBNqe" ,
                        IPP_SEX_COD = "",#"TiItWYuZgJBjmkTGQ" ,
                        IPP_ANN_NAI = "",#"Aw" ,
                        CAI_NUM = "",#"oXIvSJrfpuRxEOf" ,
                        ACT_CAB_COD = "",#"GJkJopq" ,
                        T733_CTR_NUM = "",#"AdxiUEcG" ,
                        PFS_PRA_SPE = p.speciality,#"HDinVkEUqUGcwxTbVTD"    ,
                        CES_CES_COD = "",#"tCJE" ,
                        PRA_MEP_COD = "",#"mj" ,
                        CNV_CNV_COD = "",#"tgAvExXqPTePHoNaI" ,
                        PRA_IDP_DDP = "",#"x" ,
                        PRA_IDP_SAL = "",#"mZsVZwAcmzCnqCsNmP"	,
                        EXC_EXC_NAT = p.catpro,#"bDPTitf"    , 
                        STA_PFS_NUM = "",#"ojvqEDZKs" ,
                        STA_CAI_NUM = "",#"uZubwzwJSlFensZ" ,
                        STA_CTR_NUM = "",#"kkwEGtaeDpklQOqlbh" ,
                        T733_STA_URC = "",#"fmfsysVqECeQQe" ,
                        FIS_PFS_NUM = "",#"mzrIhRTnbONZZWO" ,
                        FIS_CAI_NUM = "",#"oBHGsDvOoCCJs" ,
                        FIS_URC_COD = "",#"Bt" ,
                        PFS_ACP_DSD = "",#"hX" ,
                        EXC_EFF_DSD = "",#"qhvMpRUBSVCGLE" ,
                        EXC_FIN_MTF = "",#"g" ,
                        PFS_INS_DSD = "",#"DsS" ,
                        PRA_SAL_SPE1 = "",#"fjZmWNqGqa" ,
                        PRA_SAL_SPE2 = "",#"MY" ,
                        PRA_SAL_SPE3 = "",#"EhDLltKG" ,
                        PRA_SAL_SPE4 = "",#"CZtuZwCDlhrnhHuhrGk" ,
                        PRA_SAL_SPE5 = "",#"LQRcDGSREUwXRB",
                        PRA_SAL_SPE6 = "",#"dDWCtmpAyts" ,
                        PRA_DIP_NBR = "",#"pJEushmm" ,
                        PRA_SAL_NBR = "",#"qVQtgU" ,
                        PFS_SPE_ANT = "",#"MkCw" ,
                        PFS_AMB_NBR = "",#"zITIAxzx" ,
                        PFS_VSL_NBR = "",#"jEDsMbkauvCJRyIW" ,
                        PFS_TXI_NBR = "",#"GsXF" ,
                        PRA_TOP_REF = "",#"AdfljPDitfnymcd" ,
                        CAB_REF_DSD = "",#"XDaFucHQm" ,
                        CAB_REF_DSF = "",#"SomNgu" ,
                        LAB_CAT_COD = "" ,
                        PRA_CIV_COD = "",#"VgmI" ,
                        PFS_EXC_COM = "",#"avmHSXRaDvOl" ,
                        PFS_COD_POS = "",#"XM" ,
                        PFS_LIB_COM = "",#"TfKQLE" ,
                        PFS_FIN_NUM = "",#"lL" ,
                        PFS_MAJ_DAT = "",#"JYgZWrHLKJZZR" ,
                        PFS_SCN_COD = ""#"KhwflLkNcjwhYUz"
                    )
        return [professional]


    def createInsert_ben(self, Base, p):
        beneficiaire = Base.classes.IR_BEN_R(
                BEN_NIR_PSA = p.NIR, 	#	chaîne de caractères	Identifiant anonyme du patient dans le SNIIRAM
                BEN_RNG_GEM = p.RNG_GEM,	#	nombre entier	rang de naissance du bénéficiaire
                BEN_NIR_ANO = p.NIR,#"",
                BEN_IDT_ANO = p.NIR,#"",
                BEN_IDT_TOP = 5,#0,
                ASS_NIR_ANO = "",
                BEN_IDT_MAJ = date(1900,1,1),
                BEN_CDI_NIR = "00",# voir IR_NIR_V: 00 NIR Normal
                BEN_NAI_ANN = p.BD.year,
                BEN_NAI_MOI = p.BD.month,
                BEN_SEX_COD = int(p.Sex),
                BEN_DCD_DTE = datetime(2100,1,1,0,0,0),
                BEN_DCD_AME = "",
                ORG_AFF_BEN = "01C731029", #Organisme beneficiaire (clé étrangère)
                BEN_RES_DPT = p.Dpt,
                BEN_RES_COM = p.City,
                BEN_TOP_CNS = 0,#0,
                MAX_TRT_DTD = date(1900,1,1),
                ORG_CLE_NEW = "01C682674",
                BEN_DTE_INS = datetime(1900,1,1,0,0,0),
                BEN_DTE_MAJ = datetime(1900,1,1,0,0,0)
        )

        beneficiaire_dcir=Base.classes.IR_IBA_R(
                BEN_IDT_ANO = beneficiaire.BEN_IDT_ANO, #foreign key, primary_key
                BEN_IDT_TOP = beneficiaire.BEN_IDT_TOP,
                ASS_NIR_ANO = beneficiaire.ASS_NIR_ANO,
                BEN_CDI_NIR = beneficiaire.BEN_CDI_NIR,
                BEN_NAI_ANN = beneficiaire.BEN_NAI_ANN,
                BEN_NAI_MOI = beneficiaire.BEN_NAI_MOI,
                BEN_SEX_COD = beneficiaire.BEN_SEX_COD,
                BEN_DCD_DTE = beneficiaire.BEN_DCD_DTE,
                BEN_DCD_AME = beneficiaire.BEN_DCD_AME,
                ORG_AFF_BEN = beneficiaire.ORG_AFF_BEN,
                BEN_RES_DPT = beneficiaire.BEN_RES_DPT,
                BEN_RES_COM = beneficiaire.BEN_RES_COM,
                BEN_TOP_CNS = beneficiaire.BEN_TOP_CNS,
                MAX_TRT_DTD = beneficiaire.MAX_TRT_DTD,
                ORG_CLE_NEW = beneficiaire.ORG_CLE_NEW,
                BEN_DTE_INS = beneficiaire.BEN_DTE_INS,
                BEN_DTE_MAJ = beneficiaire.BEN_DTE_MAJ
            )
        
        beneficiaire_carto=Base.classes.CT_IDE_AAAA_GN(
                id_carto = beneficiaire.ASS_NIR_ANO,
                ben_nir_psa = beneficiaire.BEN_NIR_PSA,
                ben_rng_gem = beneficiaire.BEN_RNG_GEM,
                version ="1"
            )
        return [beneficiaire, beneficiaire_dcir, beneficiaire_carto]

if __name__ == "__main__":
    sim = simDB()
    sim.run()
    sim.generate()