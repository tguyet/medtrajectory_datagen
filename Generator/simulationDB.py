# -*- coding: utf-8 -*-

from simulation import simulation
from database_model import Patient, GP, Specialist, Provider, DrugDelivery

import sqlalchemy as sa
from tableschema import Table
import numpy as np
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy import inspect
from datetime import date, datetime
from shutil import copyfile
import sqlite3

from dateutil.relativedelta import *
from datetime import date

class simDB(simulation):
    def __init__(self):
        super().__init__()
    


    def generate(self):
        rootschemas="/home/tguyet/Progs/medtrajectory_datagen/schemas"
        #rootsnomencl="/home/tguyet/Tools/synthetic-snds/nomenclatures"
        
        copyfile("/home/tguyet/Progs/medtrajectory_datagen/Generator/snds_nomenclature.db", "/home/tguyet/Progs/medtrajectory_datagen/Generator/snds_testgen.db")
        db = sa.create_engine('sqlite:////home/tguyet/Progs/medtrajectory_datagen/Generator/snds_testgen.db')
        
        
        ########  Physicians and Pharmacists #############
        # Get table information
        inspector = inspect(db)
        tables=inspector.get_table_names()
        
        # Create the table if necessary
        modified=False
        if "DA_PRA_R" not in tables:
            table=Table([], schema=rootschemas+"/DCIR_DCIRS/DA_PRA_R.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "BE_IDE_R" not in tables:
            table=Table([], schema=rootschemas+"/DCIR_DCIRS/BE_IDE_R.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "T_MCOaa_nnE" not in tables:
            table=Table([], schema=rootschemas+"/PMSI/PMSI_MCO/T_MCOaa_nnE.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "T_SSRaa_nnE" not in tables:
            table=Table([], schema=rootschemas+"/PMSI/PMSI_SSR/T_SSRaa_nnE.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        
        if modified:
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
            
        for e in self.createInsert_Etablissement(Base, self.etablissement):
            s.add( e )
        s.commit()
        
        ########  Patients and their care trajectories #############
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
        if "ER_PRS_F" not in tables:
            table=Table([], schema=rootschemas+"/DCIR/ER_PRS_F.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "ER_PHA_F" not in tables:
            table=Table([], schema=rootschemas+"/DCIR/ER_PHA_F.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "ER_CAM_F" not in tables:
            table=Table([], schema=rootschemas+"/DCIR/ER_CAM_F.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
            
        #Table for hospital stays
        if "T_MCOaa_nnB" not in tables:
            table=Table([], schema=rootschemas+"/PMSI/PMSI_MCO/T_MCOaa_nnB.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "T_MCOaa_nnC" not in tables:
            table=Table([], schema=rootschemas+"/PMSI/PMSI_MCO/T_MCOaa_nnC.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "T_MCOaa_nnD" not in tables:
            table=Table([], schema=rootschemas+"/PMSI/PMSI_MCO/T_MCOaa_nnD.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
            
        if modified:
            Base = automap_base()
            Base.prepare(db, reflect=True)
            #redefinition of the foreign key ... I remove the "foreign(...)" to have a read-only relation to Table IR_BEN_R
            Base.classes.CT_IDE_AAAA_GN.ir_ben_r = relationship("IR_BEN_R", primaryjoin="and_(IR_BEN_R.BEN_NIR_PSA == foreign(CT_IDE_AAAA_GN.ben_nir_psa), IR_BEN_R.BEN_RNG_GEM == CT_IDE_AAAA_GN.ben_rng_gem)")

        
        try:
            s = session()
            for p in self.patients:
                for pi in self.createInsert_ben(Base, p):
                    s.add( pi )
            s.commit()
        except sqlite3.IntegrityError as err:
            print(err)
            raise
        
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
        
        ret = [beneficiaire, beneficiaire_dcir, beneficiaire_carto]
        for d in p.drugdeliveries:
            ret += self.createInsert_DrugDelivery(Base, p, d)
        
        for d in p.visits:
            ret.append( self.createPRS(Base, p, d) )
        
        for d in p.medicalacts:
            ret += self.createInsert_MedicalAct(Base, p, d)
            
            
        for d in p.hospitalStays:
            ret += self.createInsert_SejourMCO(Base, p, d)
            
        return ret


    def createInsert_SejourMCO(self, Base, patient, stay):
        """
        return list of database objects
        """
        
        #increment the RSA_Num of the hospital
        stay.hospital.current_RSA_NUM += 1
        
        age = relativedelta(date.today(), stay.patient.BD)
        aged = (date.today()-stay.patient.BD).days
        
        sejour = Base.classes.T_MCOaa_nnB(
                ##### Clé #####
                ETA_NUM	=	stay.hospital.id,	#	nombre entier	Numéro FINESS e-PMSI
                RSA_NUM	=	stay.hospital.current_RSA_NUM,	                        #	nombre entier	N° d'index du RSA
                
                
                ##### Attributs importants (utilisés) ####
                DGN_PAL	=	stay.DP,	                #	chaîne de caractères	Diagnostic principal (DP)
                DGN_REL	=	"",	                    #	chaîne de caractères	Diagnostic relié (DR)
                
                RSS_NUM	=	"",	                #	chaîne de caractères	Numéro de version du format du RSA
                
                GRC_GHM	=	"",	        #	chaîne de caractères	GHM calculé par la clinique
                
                SOR_ANN	=	stay.finish_date.year,	            #	année	Année de sortie
                SOR_MOI	=	stay.finish_date.month,	            #	date	Mois de sortie
                
                ENT_PRV	=	"",	                    #	chaîne de caractères	Provenance
                SOR_DES	=	"",	                    #	chaîne de caractères	Destination
                
                SEJ_NBJ	=	1,	                    #	nombre entier	Durée totale du séjour dans le champ du PMSI (vide si séances)
                
                ####### Autres variables ##############
                AGE_ANN	=	age.year,	                        #	nombre entier	Age en années
                AGE_GES	=	0,	                        #	nombre entier	Age gestationnel
                AGE_JOU	=	aged,	                    #	nombre entier	Age en jours
                
                BDI_COD	=	patient.City,	#	chaîne de caractères	Code géographique de résidence !!! je reprends le code commune !!
                BDI_DEP	=	patient.Dpt,	#	chaîne de caractères	Code département de résidence
                COD_SEX	=	int(patient.Sex),	#	chaîne de caractères	Sexe
                
                ANT_SUP_NBR	=	0,	                #	nombre entier	Nombre de suppléments antepartum
                AUT_PGV_NBR	=	0,	                #	nombre entier	Nombre d'autorisations d'unités médicales à portée globale valides (Nb_AutPGV)
                BEB_SEJ	=	"",	        #	chaîne de caractères	Type de séjour inférieur à la borne extrême basse
                BEH_NBJ	=	0,	                    #	nombre entier	Nombre de journées au-delà de la borne extrême haute
                CAI_SUP_NBR	=	0,	                #	nombre entier	Nombre de suppléments caisson hyperbare
                DEL_REG_ENT	=	0,	                    #	nombre entier	Délai de la date des dernières règles par rapport à la date d'entrée
                DOS_TYP	=	"",	                    #	chaîne de caractères	Type de dosimétrie
                ENT_MOD	=	"",	                    #	chaîne de caractères	Mode d'entrée dans le champ du PMSI-MCO
                
                ETE_GHS_NUM	=	0,	                    #	nombre entier	Numéro de GHS (du GHM GENRSA)
                EXB_NBJ	=	0,	                    #	nombre entier	Nb journées EXB
                GHS_9615_ACT	=	0,	        #	nombre entier	Nombre d'actes menant dans le GHS 9615
                GHS_HS_INNOV	=	0,	                #	nombre entier	GHS si non prise en compte de l'innovation
                GHS_NUM	=	0,	                        #	nombre entier	Numéro de GHS (du GHM GENRSA)
                GRC_RET	=	0,	                        #	booléen	Groupage établissement Code Retour
                GRC_VER	=	0,	                        #	nombre entier	Groupage établissement Version classification
                GRG_GHM	=	"",	                #	chaîne de caractères	GHM calculé par le GENRSA
                GRG_RET	=	1,	                        #	booléen	Code retour obtenu par GENRSA
                GRG_VER	=	"",	        #	chaîne de caractères	Groupage GENRSA :Version de la classification
                INNOV_NUM	=	"",	            #	chaîne de caractères	Numéro d'innovation
                MACH_TYP_RAD	=	"",	#	chaîne de caractères	Type de machine en radiothérapie
                NBR_ACT	=	1,	                    #	nombre entier	Nombre de zones d'actes (nA) dans ce RSA
                NBR_DGN	=	len(stay.cim_das),	                    #	nombre entier	Nombre de diagnostics associés significatifs (nDAS) dans ce RSA
                NBR_RUM	=	0,	                    #	nombre entier	Nombre de RUM composant le RSS d'origine (NbRUM)
                NBR_SEA	=	0,	                    #	nombre entier	Nombre de séances
                NBR_SUP_NN1	=	0,	                #	nombre entier	Nombre de suppléments NN1
                NBR_SUP_NN2	=	0,	                #	nombre entier	Nombre de suppléments NN2
                NBR_SUP_NN3	=	0,	                #	nombre entier	Nombre de suppléments NN3
                NBR_SUP_REA	=	0,	                #	nombre entier	Nombre de suppléments pour REA (réanimation)
                NBR_SUP_REP	=	0,	                #	nombre entier	Nombre de suppléments REP (réanimation pédiatrique)
                NBR_SUP_SOI	=	0,	                #	nombre entier	Nombre de suppléments soins intensifs provenant de la réanimation
                NBR_SUP_SRC	=	0,	                #	nombre entier	Nombre de suppléments pour SRC (surveillance continue)
                NBR_SUP_STF	=	0,	                #	nombre entier	Nombre de suppléments pour STF (soins intensifs)
                PAS_LIT_DED	=	0,	                    #	booléen	Passage dans un lit dédié de soins palliatifs
                PLO_ACT	=	0,	                        #	nombre entier	Type de prestation de prélèvement d'organe
                POI_NAI	=	0,	                    #	nombre entier	Poids d'entrée (en grammes)
                
                RTH_SUP_NBR	=	0,	                #	nombre entier	Nombre de zones de suppléments de radiothérapie (Nb_Rdth)
                SEJ_COD_CONF	=	"",	            #	chaîne de caractères	Confirmation du codage du séjour
                SEJ_TYP	=	"",	        #	chaîne de caractères	Type de séjour
                SEQ_RUM	=	0,	                    #	nombre entier	N° séquentiel du RUM ayant fourni le DP (RUM:Résumé d'Unité Médicale)
                SOR_MOD	=	"",	                    #	chaîne de caractères	Mode de sortie du champ PMSI-MCO
                SUP_ENT_DPA	=	0,	            #	nombre entier	Nombre de suppléments pour les entraînements à la dialyse péritonéale automatisée hors séances
                SUP_ENT_DPC	=	0,	            #	nombre entier	Nombre de suppléments pour les entraînements à la dialyse péritonéale continue ambulatoire hors séances
                SUP_ENT_HEM	=	0,	            #	nombre entier	Nombre de suppléments pour les entraînements à l'hémodialyse hors séances
                SUP_HEM_HS	=	0,	            #	nombre entier	Nombre de suppléments pour hémodialyse hors séances
                SUP_RAD_PED	=	0,	            #	nombre entier	Nombre de suppléments radiothérapie pédiatrique
                TAR_SEQ_NUM	=	"",	    #	chaîne de caractères	Numéro séquentiel de tarifs
                TOP_AVASTIN	=	0,	                    #	booléen	Top Radiation partielle Avastin
                TOP_DEF_CARD	=	0,	                #	booléen	Supplément défibrillateur cardiaque
                TOP_GHS_MIN_SUS	=	0,	                #	booléen	Top GHS minoré
                TOP_VLV_AOR	=	0,	                    #	booléen	Top valves aortiques percutanées
                TYP_GEN_RSA	=	0,	                    #	nombre entier	Type de génération automatique du RSA
                UHCD_TOP	=	0	                    #	booléen	Top UHCD
        )
        
        
        sejourB = Base.classes.T_MCOaa_nnC(
            ## 
            ETA_NUM	=	sejour.ETA_NUM,	                #	nombre entier	Numéro FINESS e-PMSI
            RSA_NUM	=	sejour.RSA_NUM,	                #	nombre entier	N° séquentiel dans fichier PMSI
            
            ## Variables importantes -> le lien au bénéficiare + dates
            NIR_ANO_17	=	patient.NIR,	#	chaîne de caractères	N° anonyme
            EXE_SOI_DTD	=	datetime(stay.start_date.year,stay.start_date.month,stay.start_date.day,12,0,0),	#	date et heure	Date d'entrée (date)
            EXE_SOI_DTF	=	datetime(stay.finish_date.year,stay.finish_date.month,stay.finish_date.day,12,0,0),	    #	date et heure	Date de sortie (date)
            
            SEJ_NUM	=	1,	#	nombre entier	N° de séjour
            
            RNG_BEN	=	str(patient.RNG_GEM),	#	chaîne de caractères	Rang du bénéficiaire
            RNG_NAI	=	patient.RNG_GEM,	    #	nombre entier	Rang de naissance
            
            ENT_DAT	=	stay.start_date,	    #	date	date d'entrée
            EXE_SOI_AMD	=	stay.start_date,	#	année et mois	Date d'entrée au format année + mois
            
            SOR_DAT	=	stay.finish_date,	    #	date	date de sortie
            EXE_SOI_AMF	=	stay.finish_date,	#	année et mois	Date de sortie au format année + mois
            SOR_ANN	=	sejour.SOR_ANN,	    #	année	Année de sortie  //-> utilisé comme clé de jointure avec B, donc je mets les mêmes ici
            SOR_MOI	=	sejour.SOR_ANN,	    #	date	Mois de sortie   //-> utilisé comme clé de jointure avec B, donc je mets les mêmes ici
            
            NIR_ANO_MAM	=	"",	#	chaîne de caractères	N° anonyme mère-enfant ???
            HOS_NN_MAM	=	0,	        #	booléen	Hospitalisation d'un nouveau-né auprès de la mère
            HOS_PLO	=	0,	            #	booléen	Hospitalisation pour prélèvement d'organe
            
            FOR_NUM	=	"",	#	chaîne de caractères	N° format
            VID_HOSP_FOR	=	1,      	#	nombre entier	N° format VID-HOSP
            ORG_CPL_NUM	=	"",	#	chaîne de caractères	N° d’organisme complémentaire
            ORG_CPL_NUM_RET	=	1,	        #	booléen	Code retour contrôle " N° d’organisme complémentaire"
            
            NUM_DAT_AT	=	"",	#	nombre entier	Numéro accident du travail ou date d’accident de droit commun
            
            COH_NAI_RET	=	1,	#	booléen	Code retour contrôle « Cohérence date naissance »
            COH_SEX_RET	=	1,	#	booléen	Code retour contrôle « Cohérence sexe »
            DAT_RET	=	"",	#	chaîne de caractères	Code retour contrôle « date de référence» (date d'entrée)
            ETA_NUM_RET	=	1,	#	booléen	Code retour contrôle "N° FINESS d’inscription e-PMSI"
            FHO_RET	=	1,	    #	booléen	Code retour « fusion ANO HOSP et HOSP PMSI »
            HOS_NNE_RET	=	1,	#	booléen	Code retour contrôle « Hospitalisation d'un nouveau-né auprès de la mère »
            HOS_ORG_RET	=	1,	#	booléen	Code retour contrôle « Hospitalisation pour prélèvement d'organe »
            NAI_RET	= "",	    #	chaîne de caractères	Code retour contrôle « date de naissance »
            NIR_RET	= 1,	    #	booléen	Code retour contrôle « n° sécurité sociale »
            NUM_DAT_AT_RET = 1,	#	booléen	Code retour contrôle " Numéro accident du travail ou date d’accident de droit commun"
            PMS_RET	=	1,	    #	booléen	Code retour « fusion ANO PMSI et fichier PMSI »
            RNG_BEN_RET	= "",	#	booléen	Code retour contrôle « Rang du bénéficiaire »
            RNG_NAI_RET	= 1,	#	booléen	Code retour contrôle « Rang de naissance »
            SEJ_MER_RET	= 1,	#	booléen	Code retour contrôle « N° administratif de séjour de la mère »
            SEJ_RET	= 1,	    #	booléen	Code retour contrôle « n° d’identification administratif de séjour »
            SEX_RET	= 1 	    #	booléen	Code retour contrôle « sexe »
        )
        
        ret=[sejour, sejourB]
        
        #add entries for the associated diagnosis
        diag_ass_num=0
        for cim in stay.cim_das:
            ## ajout d'un diagnostic associé :
            #       -> incrémenter le DGN_ASS_NUM pour chaque couple ETA_NUM,RSA_NUM
            sejourD = Base.classes.T_MCOaa_nnD(
                ETA_NUM	=	sejour.ETA_NUM,	#	nombre entier	Numéro FINESS e-PMSI
                RSA_NUM	=	sejour.RSA_NUM,	#	nombre entier	N° d'index du RSA
                DGN_ASS_NUM	= diag_ass_num,   	        #	nombre entier	Numero de diag par couple eta_num rsa_num
                
                ASS_DGN	= cim,   	    #	chaîne de caractères	Diagnostic associé (dans table MS_CIM_V)
                RSS_NUM	= ""                #	chaîne de caractères	Numéro de version du format du RSA
            )
            ret.append( sejourD )
            diag_ass_num += 1
        
        return ret

    def createInsert_Etablissement(self, Base, eta):
        ## Création d'un établissement
        etablissement = Base.classes.BE_IDE_R(
            IDE_ETA_NUM	=	eta.id,
            IDE_ETA_NU8	=	eta.id,
            IDE_ETA_NOM	=	"EhvAeahPbftsZtVqF",
            IDE_IDE_CPL	=	"LzLc",
            IDE_VOI_NUM	=	"eB",
            IDE_VOI_CNU	=	"",
            IDE_VOI_TYP_LRG	=	"",
            IDE_VOI_LIB	=	"oXIvSJrfpuRxEOf",
            IDE_ADR_CPL	=	"pJHkSzJGJoqOUDJ",
            IDE_RSD_LIB	=	"AcZaAckxCilGUEdOw",
            IDE_BDI_COD	=	"nDGT",
            IDE_TEL_NU1	=	"CJ",
            IDE_TEL_NU2	=	"m",
            IDE_INT_ADR	=	"biEHAWnTdbovlgayPKpjJXyqILPKtgnxReN",
            IDE_FAX_NUM	=	"",
            IDE_CPT_RSC	=	"EWqlstqmwLqQGJEQWmiicgnmQsUUrNFsYelCpQCtPLLZgVRmizCMvRQNagUxciLnZoyebCdXAk",
            IDE_INS_COM	=	"f",
            IDE_CPA_NUM	=	"Ks",
            IDE_CRA_NUM	=	"l",
            IDE_NUM_NOU	=	"QtDweklGp",
            IDE_NUM_DTE	=	datetime(1972,6,1,0,0,0),
            IDE_PSH_CAT	=	"NhO",
            IDE_CCL_DTE	=	datetime(1928,6,4,0,0,0),
            IDE_PSH_STJ	=	"",
            IDE_STJ_DTE	=	datetime(1970,12,18,0,0,0),#
            IDE_PSH_MFT	=	"SD",
            IDE_MFT_DTE	=	datetime(1938,7,7,0,0,0),#"1938-07-07",
            IDE_HON_TYP	=	"",
            IDE_HON_DTE	=	datetime(1924,2,16,0,0,0),#"1924-02-16",
            IDE_GES_NUM	=	"Y",
            IDE_GES_DTE	=	datetime(1983,7,21,0,0,0),#"1983-07-21",
            IDE_SRT_NUM	=	"GutNwhCZhZhnl",
            IDE_ETA_DTE	=	datetime(1973,12,18,0,0,0),#"1973-12-18",
            IDE_ATI_COD	=	"D",
            IDE_PSP_NAT	=	"u",
            IDE_PSP_DTE	=	datetime(1906,4,7,0,0,0),#"1906-04-07",
            IDE_BGL_PEX	=	"C",
            IDE_ETA_TYP	=	"z",
            IDE_COD_A24	=	"vIJ",
            IDE_SAN_PUB	=	"",
            IDE_IDE_EAM	=	"3583766311",
            IDE_IDE_DAM	=	datetime(1905,12,27,0,0,0),#"1905-12-27",
            IDE_IDE_FAM	=	datetime(1980,8,26,0,0,0),#"1980-08-26",
            IDE_IDE_EAS	=	"4265207648",
            IDE_IDE_DAS	=	datetime(1985,4,26,0,0,0),#"1985-04-26",
            IDE_IDE_FAS	=	datetime(1984,3,8,0,0,0),#"1984-03-08",
            IDE_IDE_CON	=	"2912602989",
            IDE_IDE_DON	=	datetime(1919,11,25,0,0,0),#"1919-11-25",
            IDE_IDE_FON	=	datetime(1926,2,18,0,0,0),#"1926-02-18",
            IDE_NUM_ANC	=	"KLYJHJ",
            IDE_ANC_DTE	=	datetime(1978,10,30,0,0,0),#"1978-10-30",
            IDE_IDE_CTR	=	"2",
            IDE_IDE_DTR	=	datetime(1929,9,30,0,0,0),#"1929-09-30",
            IDE_IDE_FTR	=	datetime(1927,11,29,0,0,0),#"1927-11-29",
            IDE_A24_24	=	"B",
            IDE_A24_24B	=	"Kc",
            IDE_A24_24C	=	"mMy",
            IDE_A24_24Q	=	"D",
            IDE_A24_24S	=	"I",
            IDE_A24_24T	=	"R",
            IDE_NUM_PCP	=	"VMSSnUe",
            IDE_CBU_CTR	=	"5",
            IDE_CBU_EFF	=	datetime(1910,11,21,0,0,0),#"1910-11-21",
            IDE_CBU_TAU	=	"HbJ",
            IDE_CBU_DTF	=	datetime(1921,2,25,0,0,0),#"1921-02-25",
            IDE_FIN_IND	=	"",
            IDE_CAI_PIV	=	"oD",
            IDE_NAT_ORI	=	"YpM",
            IDE_MCO_COE	=	"77652",
            IDE_COE_HAD	=	"65768",
            IDE_MCO_DTE	=	datetime(2010,10,5,0,0,0),#"2010-10-05",
            IDE_HAD_DTE	=	datetime(1919,9,18,0,0,0),#"1919-09-18",
            IDE_IMP_DPT	=	"Xm"
        )
        
        #Création de l'etablissement pour les volets MCO et SSR
        etablissementpmsi = Base.classes.T_MCOaa_nnE(
            ###### clés de la table #####
            ETA_NUM	=	etablissement.IDE_ETA_NUM,	    #	chaîne de caractères	Numéro FINESS e-PMSI
            
            ### Autres attributs 
            ANN_TRT	=	"",	        #	chaîne de caractères	N° du trimestre PMSI transmis
            ETB_EXE_FIN	=""	,	                #	chaîne de caractères	N°FINESS sans clé
            REG_ETA	=	"",	        #	chaîne de caractères	Région
            SOC_RAI	=	"",	#	chaîne de caractères	Raison sociale
            STA_ETA	=	"",	        #	chaîne de caractères	Statut de l'établissement
            VAL_ETA	=	"",	#	chaîne de caractères	Validation des données
        )
        
        #recopie du même établissement pour les SSR
        etablissementssr = Base.classes.T_SSRaa_nnE(
            ###### clés de la table #####
            ETA_NUM	=	etablissementpmsi.ETA_NUM,	    #	chaîne de caractères	Numéro FINESS e-PMSI
            
            ### Autres attributs 
            ANN_TRT	=	etablissementpmsi.ANN_TRT,	        #	chaîne de caractères	N° du trimestre PMSI transmis
            ETB_EXE_FIN	= etablissementpmsi.ETB_EXE_FIN	,	                #	chaîne de caractères	N°FINESS sans clé
            REG_ETA	=	etablissementpmsi.REG_ETA,	        #	chaîne de caractères	Région
            SOC_RAI	=	etablissementpmsi.SOC_RAI,	#	chaîne de caractères	Raison sociale
            STA_ETA	=	etablissementpmsi.STA_ETA,	        #	chaîne de caractères	Statut de l'établissement
            VAL_ETA	=	etablissementpmsi.VAL_ETA,	#	chaîne de caractères	Validation des données
        )
        
        
        return [etablissement, etablissementpmsi, etablissementssr]


    def createPRS(self, Base, p, ma):
        
        #Création d'une entrée dans la table prestation
        prestation = Base.classes.ER_PRS_F(
            ###### clés de la table #####
            DCT_ORD_NUM	=	ma.ord_num,	         #	nombre entier // numéro d'ordre du décompte dans l'organisme
            FLX_DIS_DTD	=	datetime(1900,1,1,0,0,0),	    #	date //Date de mise à disposition des données IR_DTE_V[DTE_DTE]
            FLX_EMT_NUM	=	1,	                            #	nombre entier // numéro d'émetteur du flux IR_NEM_T[EMT_NUM_RES], 1:Rouen 1
            FLX_EMT_ORD	=	7724,	                        #	nombre entier // numéro de séquence du flux
            FLX_EMT_TYP	=	3,	                            #	nombre entier // Type d'émetteur IR_TYT_V[TYT_COD]
            FLX_TRT_DTD	=	datetime(1900,1,1,0,0,0),	    #	date // Date d'entrée des données dans le système d'information IR_DTE_V[DTE_DTE]
            ORG_CLE_NUM	=	"01C731220",	                #	chaîne de caractères // organisme de liquidation des prestations (avant fusion des caisses) IR_ORG_V[ORG_NUM]
            PRS_ORD_NUM	=	0,	                        #	nombre entier // Numéro d'ordre de la prestation dans le décompte
            REM_TYP_AFF	=	0,	                    #	nombre entier // type de remboursement affiné (pas de ref)
        
            ##### Identification du bénéficiaire ######
            BEN_NIR_PSA	=	ma.patient.NIR,	            #	chaîne de caractères    # FOREIGN KEY: IR_BEN_R [ BEN_NIR_PSA, BEN_RNG_GEM ]
            BEN_RNG_GEM	=	ma.patient.RNG_GEM,	            #	nombre entier           # FOREIGN KEY: IR_BEN_R [ BEN_NIR_PSA, BEN_RNG_GEM ]
        
            ###### identification des acteurs de santé (EXE: executant, PRE: prescripteur, MTT: medecin traitant) ######
            ETB_PRE_FIN	=	self.etablissement.id,	         #	chaîne de caractères    # FOREIGN KEY:      BE_IDE_R [ IDE_ETA_NU8 ]
            PFS_EXE_NUM	=	ma.provider.id,	     #	chaîne de caractères    # FOREIGN KEY:  DA_PRA_R [ PFS_PFS_NUM ]
            PFS_EXE_NUMC	=	"",	            #	
            PFS_PRE_NUM	=	ma.prescriber.id,	                    #	chaîne de caractères    # FOREIGN KEY:  DA_PRA_R [ PFS_PFS_NUM ]
            PFS_PRE_NUMC	=	"",	                    #	
            PRS_MTT_NUM	=	ma.patient.MTT.id,	                    #	chaîne de caractères    # FOREIGN KEY:  DA_PRA_R [ PFS_PFS_NUM ]
            PRS_MTT_NUMC	=	"",	            #	
        
            PSE_ACT_NAT	=	ma.code_nature,	       #	nombre entier // Nature d'activité du professionnel de santé exécutant  IR_ACT_V[PFS_ACT_NAT], 50: Pharmacie d'officine
        
            BEN_RES_COM	=	ma.patient.City,	#	chaîne de caractères        #Code Commune (Bénéficiaire)
            BEN_RES_DPT	=	ma.patient.Dpt,	#	chaîne de caractères        #Code Département (Bénéficiaire)
            BEN_SEX_COD	=	int(ma.patient.Sex),	    #	nombre entier      #Code Sexe (Bénéficiaire)
            BEN_NAI_ANN	=	ma.patient.BD.year,	#	année                       #Annee de naissance du bénéficiaire
            BEN_AMA_COD	=	1000,	#	nombre entier
            BEN_CDI_NIR	=	"99",	#	chaîne de caractères
            BEN_CMU_CAT	=	7,	#	nombre entier
            BEN_CMU_ORG	=	"01C731032",	#	chaîne de caractères
            BEN_CMU_TOP	=	"0",	#	nombre entier //beneficiaire CMU=89, 0=Non CMU
            BEN_DCD_AME	=	"000101",	#	année et mois de décès (en texte YYYYMM)
            BEN_DCD_DTE	=	datetime(1,1,1,0,0,0),	#	date de décès
            BEN_EHP_TOP	=	1,	#	nombre entier
            BEN_IAT_CAT	=	"06",	#	chaîne de caractères
            BEN_PAI_CMU	=	1,	#	nombre entier           
            BEN_QAF_COD	=	25,	#	nombre entier
        
            ORG_AFF_BEN	=	"01C731221",	#	chaîne de caractères
            PRS_REJ_TOP	=	"zGijEbx",	#	nombre entier
            
            EXE_SOI_DTD	=	ma.date_debut,	#	date            // Date de début d'exécution des soins
            EXE_SOI_DTF	=	ma.date_fin,	#	date            // Date de fin d'exécution des soins
            EXE_SOI_AMD	=	datetime(1,1,1,0,0,0),	#	année et mois
            EXE_SOI_AMF	=	datetime(1,1,1,0,0,0),	#	année et mois
            PRE_PRE_AMD	=	datetime(1,1,1,0,0,0),	#	année et mois
            PRE_PRE_DTD	=	datetime(1,1,1,0,0,0),	#	date de prescription
            PRS_GRS_DTD	=	datetime(1,1,1,0,0,0),	#	date, date présumé de grossesse
            PRS_HOS_AMD	=	datetime(1,1,1,0,0,0),	#	année et mois, date début hospitalisation
            PRS_HOS_DTD	=	datetime(1,1,1,0,0,0),	#	date
            BSE_REM_BSE	=	0.00,	#	nombre réel
            BSE_REM_MNT	=	0.00,	#	nombre réel
            BSE_REM_PRU	=	0.00,	#	nombre réel
            BSE_REM_SGN	=	1,	#	nombre entier Signe du remboursement, IR_SNG_V
            CPL_REM_BSE	=	78090389879.25,	#	nombre réel
            CPL_REM_MNT	=	41950861634.87,	#	nombre réel
            CPL_REM_PRU	=	81140362630858.48,	#	nombre réel
            CPL_REM_SGN	=	0,	#	nombre entier
            PRS_ACT_CFT	=	661328942.45,	#	nombre réel
            PRS_ACT_COG	=	153215936.9,	#	nombre réel
            PRS_ACT_NBR	=	0,	#	nombre réel Dénombrement signé d'actes (pour les indemnités journalières)
            PRS_ACT_QTE	=	0,	#	nombre réel Quantité signé d'actess
            
            PRS_DEP_MNT	=	0.0,	#	nombre réel, montant déplacemnt
            PRS_ETA_RAC	=	74790551649.64,	#	nombre réel
            PRS_PAI_MNT	=	91738548395.16,	#	nombre réel
            RGO_MOD_MNT	=	90206152481.25,	#	nombre réel
            ORB_BSE_NUM	=	"03C021",	#	chaîne de caractères
            ORL_BSE_NUM	=	"03C024",	#	chaîne de caractères
            RGM_COD	=	6,	#	nombre entier
            RGM_GRG_COD	=	677,	#	nombre entier // Regime du bénéficiare IR_RGM_V[RGM_COD]
            ACC_TIE_IND	=	0,	#	nombre entier
            BSE_FJH_TYP	=	9,	#	nombre entier
            BSE_PRS_NAT	=	6013,	#	nombre entier
            CPL_AFF_COD	=	15,	#	nombre entier
            CPL_ANO_COD	=	3,	#	nombre entier
            CPL_FJH_TYP	=	2,	#	nombre entier
            CPL_MAJ_TOP	=	78,	#	nombre entier
            CPL_PRS_NAT	=	5206,	#	nombre entier
            DPN_QLF	=	50,	#	nombre entier mvt liqudation
            DRG_MOD	=	1,	#	nombre entier voir IR_MOD_V, mode de règlement, 1=Virement bancaire
            DRG_NAT	=	10,	#	nombre entier voir IR_DRG_V, 10=Tier Payant
            EXE_LIE_COD	=	8,	#	nombre entier
            EXO_MTF	=	44,	#	nombre entier
            IJR_EMP_NUM	=5866418702621912,	#	nombre entier
            IJR_RVL_NAT	=	"MP",	#	chaîne de caractères
            MTM_NAT	=	3,	#	nombre entier
            ORG_CLE_NEW	=	"01C731220",	#	chaîne de caractères
            PRE_REN_COD	=	3,	#	nombre entier
        
            PRS_CRD_OPT	=	4,	#	nombre entier
            PRS_DPN_QLP	=	12,	#	nombre entier
            PRS_NAT_REF	=	ma.code_pres,	#	nombre entier // Code de la Prestations de référence IR_NAT_V[PRS_NAT]
            PRS_OPS_TRF	=	0,	#	nombre entier
            PRS_PDS_QCP	=	2,	#	nombre entier
            PRS_PDS_QTP	=	99,	#	nombre entier
            PRS_PPF_COD	=	"C",	#	chaîne de caractères
            PRS_PRE_MTT	=	1,	#	nombre entier
            PRS_REF_KIN	=	"II",	#	chaîne de caractères
            PRS_TOP_ENP	=	1,	#	nombre entier
            PRS_TYP_KIN	=	"U",	#	chaîne de caractères
        
            RGO_ASU_NAT	=	10,	#	nombre entier   // Nature d'assurance (régime obligatoire) IR_ASU_V[ASU_NAT]: 10=Assurrance Maladie
            RGO_ENV_TYP	=	0,	#	nombre entier
            RGO_FTA_COD	=	0,	#	nombre entier
            RGO_MIN_TAU	=	0.0,	#	nombre réel
            RGO_REM_TAU	=	0.0,	#	nombre réel
            RGO_THE_TAU	=	0.0,	#	nombre réel
            PSE_CNV_COD	=	9,	#	nombre entier // Code convention du professionnel de santé exécutant
            PSE_REF_ADH	=	"",	#	chaîne de caractères Top prestation exécuté par un professionnel de santé adhérent à l'option référent
            PSE_SPE_COD	=	0,	#	nombre entier Spécialite médicale du professionnel de santé exécutant
            PSE_STJ_COD	=	0,	#	nombre entier Mode d'exercice du professionnel de santé exécutant
        
            PSP_ACT_NAT	=	26,	#	nombre entier
            PSP_CNV_COD	=	9,	#	nombre entier
            PSP_PPS_NUM	=	"bZmhONjhyObf",	#	chaîne de caractères
            PSP_PPS_NUMC	=	"GcqeAZYKBpTJuLM",	#	
            PSP_REF_ADH	=	"GCyFnY",	#	chaîne de caractères
            PSP_SPE_COD	=	10,	#	nombre entier
            PSP_STJ_COD	=	61,	#	nombre entier
            PSP_SVI_PPS	=	15,	#	nombre entier
        
            BEN_DRT_SPF	=	"ACS",	#	chaîne de caractères
            BEN_ACS_TOP	=	"TRmWCbmpFHkN",	#	booléen
            EXE_CTX_PFS	=	"zvG",	#	chaîne de caractères
            PRS_TYP_MAJ	=	"ZZ",	#	chaîne de caractères
            EXE_CTX_BEN	=	"UEoLOkEWndFDSUcJ",	#	chaîne de caractères
            CPL_FTA_COD	=	59,	#	nombre entier
            PRS_PPU_SEC	=	9,	#	nombre entier
            BEN_CTA_TYP	=	865,	#	nombre entier
            PRS_DRA_AME	=	"Vqt",	#	année et mois Date réelle (année et mois) de l'accouchement
            DRG_AFF_NAT	=	0,	#	nombre entier
            PRS_MNT_MAJ	=	2977390.3,	#	nombre réel
            PRE_IND_PEL	=	"uBvUXVf",	#	chaîne de caractères Indicateur Prescription en Ligne
            PRS_DIS_PRE	=	"xyH",	#	chaîne de caractères
            CPL_REM_TAU	=	6695907.79,	#	nombre réel
            PRS_QTT_MAJ	=	7460	#	nombre entier Quantité de majorations
        )
        return prestation
    
    def createInsert_MedicalAct(self, Base, p, ma):
        """
        Insertion in Base of the drug delivery for patient p
        - Base ORM database model
        - d DrugDelivery
        - p Patient
        """
        prestation = self.createPRS(Base,p,ma)
        #Création d'une entrée d'un acte médical en ville
        delivrance_acte = Base.classes.ER_CAM_F(
            CAM_PRS_IDE	=	ma.code_ccam,	#	chaîne de caractères	Code CCAM de l'acte médical
            CAM_TRT_PHA	=	ma.treatmentphase,	#	nombre entier	Phase de traitement
            CAM_ACT_COD	=	ma.activitycode,	#	chaîne de caractères	Code activite
            
            CAM_ASS_COD	=	"4",	#	chaîne de caractères	Code association
            CAM_ACT_PRU	=	0,#931082.87,	#	nombre réel	Prix unitaire CCAM de l'acte médical
            CAM_CAB_IND	=	"",	#	chaîne de caractères	Top supplément de charge en cabinet
            CAM_DOC_EXT	=	"",	#	chaîne de caractères	Extension documentaire
            CAM_MOD_COD	=	"",	#	chaîne de caractères	Codes modificateurs
            CAM_ORD_NUM	=	177,	#	nombre entier	Numéro d'ordre de la prestation affinée CCAM
            CAM_QUA_DEN	=	"",	#	chaîne de caractères	Localisation dentaire
            CAM_REM_BSE	=	0.0,	#	nombre réel	Base de remboursement de la CCAM
            CAM_REM_COD	=	"kkwEGtaeDpklQOqlbh",	#	chaîne de caractères	Code remboursement exceptionnel
            ORG_CLE_NEW	=	"01C682674",	#	chaîne de caractères	Code de l'organisme de liquidation
        
            CAM_MI4_MNT	=	553941022.5,	#	nombre réel	Montant Minoration due à association sur Modificateur4
            CAM_MM4_MNT	=	379098525.43,	#	nombre réel	Montant Majoration Modificateur4
            CAM_GRI_TAR	=	"oBHGsDvOoCCJs",	#	chaîne de caractères	Grille tarifaire
            CAM_SUP_MNT	=	154287578.13,	#	nombre réel	Montant supplément de Charge en Cabinet
            CAM_MI1_MNT	=	545851429.93,	#	nombre réel	Montant Minoration due à association sur Modificateur1
            CAM_NRM_MNT	=	259371469.95,	#	nombre réel	Montant Non Rmb du à annulation du tarif pr acte non rmb
            CAM_MI3_MNT	=	296249161.67,	#	nombre réel	Montant Minoration due à association sur Modificateur3
            CAM_MM2_MNT	=	927480538.62,	#	nombre réel	Montant Majoration Modificateur2
            CAM_MM3_MNT	=	185575274.82,	#	nombre réel	Montant Majoration Modificateur3
            CAM_RED_MNT	=	9,	#	nombre réel	Montant réduction Tarif due à praticien non conventionné
            CAM_MPU_MNT	=	106738202.26,	#	nombre réel	Montant Minoration due à association sur PU de l' Acte
            CAM_MM1_MNT	=	642689915.79,	#	nombre réel	Montant Majoration Modificateur1
            CAM_MI2_MNT	=	202202293.43,	#	nombre réel	Montant Minoration due à association sur Modificateur2
                
            ## Référence à l'enregistrement de la prestation
            DCT_ORD_NUM	=	prestation.DCT_ORD_NUM,
            FLX_DIS_DTD	=	prestation.FLX_DIS_DTD,
            FLX_EMT_NUM	=	prestation.FLX_EMT_NUM,
            FLX_EMT_ORD	=	prestation.FLX_EMT_ORD,
            FLX_EMT_TYP	=	prestation.FLX_EMT_TYP,
            FLX_TRT_DTD	=	prestation.FLX_TRT_DTD,
            ORG_CLE_NUM	=	prestation.ORG_CLE_NUM,
            PRS_ORD_NUM	=	prestation.PRS_ORD_NUM,
            REM_TYP_AFF	=	prestation.REM_TYP_AFF
        )
        return [prestation, delivrance_acte]

    def createInsert_DrugDelivery(self, Base, p, d):
        """
        Insertion in Base of the drug delivery for patient p
        - Base ORM database model
        - d DrugDelivery
        - p Patient
        """
        
        prestation = self.createPRS(Base,p,d)
        
        #Création d'une entrée dans la table ER_PHA pour une entrée en correspondante en pharmacie
        delivrance_medoc = Base.classes.ER_PHA_F(
            PHA_PRS_C13	=	str(d.cip13),	#	nombre entier	Code CIP de la pharmacie de ville (13 Caractères)   IR_PHA_R[PHA_CIP_C13]
            PHA_PRS_IDE	=	str(d.cip13),	#	nombre entier	Code CIP de la pharmacie de ville (ancien code sur 7 Caractères)  IR_PHA_R[PHA_CIP_C13]
            PHA_SEQ_RNV	=	d.sid,	#	nombre entier	Séquence de renouvellement
            PHA_SUB_MTF	=	0,	#	nombre entier	Motif de substitution du médicament IR_SUB_V[PHA_SUB_MTF] 0: sans objet, 2: generique, 6: refus de substitution
            PHA_ACT_QSN	=	d.quantity,	#	nombre entier	Quantité affinée signée (= nombre de boites facturées)
        
            ORG_CLE_NEW	=	"01C731222",	#	chaîne de caractères	Code de l'organisme de liquidation
            PHA_ACT_PRU	=	0.0,	#	nombre réel	Prix unitaire du médicament codé en CIP
            PHA_CPA_PCP	=	"",	#	chaîne de caractères	Condition particulière de prise en charge
            PHA_DEC_PRU	=	0.0,	#	nombre réel	Prix unitaire de l'unité déconditionnée délivrée
            PHA_DEC_QSU	=	0,	#	nombre entier	Quantité complète de déconditionnement signée
            PHA_DEC_TOP	=	"",	#	chaîne de caractères	Top déconditionnement
            PHA_IDE_CPL	=	0,	#	nombre entier	Préfixe du code CIP
            PHA_MOD_PRN	=	"",	#	chaîne de caractères	Mode de prescription
            PHA_ORD_NUM	=	0,	#	nombre entier	Numéro d'ordre de la prestation affinée pharmacie        
            ## Référence à l'enregistrement de la prestation
            DCT_ORD_NUM	=	prestation.DCT_ORD_NUM,
            FLX_DIS_DTD	=	prestation.FLX_DIS_DTD,
            FLX_EMT_NUM	=	prestation.FLX_EMT_NUM,
            FLX_EMT_ORD	=	prestation.FLX_EMT_ORD,
            FLX_EMT_TYP	=	prestation.FLX_EMT_TYP,
            FLX_TRT_DTD	=	prestation.FLX_TRT_DTD,
            ORG_CLE_NUM	=	prestation.ORG_CLE_NUM,
            PRS_ORD_NUM	=	prestation.PRS_ORD_NUM,
            REM_TYP_AFF	=	prestation.REM_TYP_AFF
        )
        
        return [prestation, delivrance_medoc]


if __name__ == "__main__":
    np.random.seed(0)
    sim = simDB()
    sim.run()
    sim.generate()