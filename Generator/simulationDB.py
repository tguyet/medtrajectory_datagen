# -*- coding: utf-8 -*-
"""
Concretizer of SNDS data simulation into a SQLite database (almost compliant with SNDS schema).

In this part of the simulator, the classes transform the isntances of the data model into records in 
the database compliant with SNDS schema.

@author: Thomas Guyet
@date: 08/2020
"""

import os,sys
from simulation import simulation
from simu_open import OpenSimulation

import sqlalchemy as sa
from tableschema import Table
import numpy as np
#import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from datetime import date, datetime
from shutil import copyfile
import sqlite3

from dateutil.relativedelta import relativedelta

class simDB:
    def __init__(self):
        self.output_db_name="snds_testgen.db"

    def generate(self, sim, rootschemas="../schemas", dbbase=None):
        """
        Fullfill the database with records corresponding to the simulation
        
        The simulator must have been run upstream (the function does not run the simulation).

        Parameters
        ----------
        sim : A simulation
        """
        
        if dbbase is None:
            dbbase=sim.nomencl_db
        
        copyfile(dbbase, os.path.join(os.getcwd(),self.output_db_name))
        db = sa.create_engine("sqlite:///"+os.path.join(os.getcwd(),self.output_db_name))
        
        
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
        if "T_MCOaaE" not in tables:
            table=Table([], schema=rootschemas+r"/PMSI/PMSI MCO/T_MCOaaE.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        
        if "T_SSRaaE" not in tables:
            table=Table([], schema=rootschemas+r"/PMSI/PMSI SSR/T_SSRaaE.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        
        
        if modified:
            Base = automap_base()
            Base.prepare(db, reflect=True)
        
        session = sessionmaker()
        session.configure(bind=db)
        s = session()
        for p in sim.GPs:
            s.add( self.createInsert_PS(Base, p)[0] )

        for p in sim.specialists:
            s.add( self.createInsert_PS(Base, p)[0] )
            
        for p in sim.pharms:
            s.add( self.createInsert_PS(Base, p)[0] )

        for etab in sim.etablissements:
            for e in self.createInsert_Etablissement(Base, etab):
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
        if "T_MCOaaB" not in tables:
            table=Table([], schema=rootschemas+r"/PMSI/PMSI MCO/T_MCOaaB.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "T_MCOaaC" not in tables:
            table=Table([], schema=rootschemas+r"/PMSI/PMSI MCO/T_MCOaaC.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        if "T_MCOaaD" not in tables:
            table=Table([], schema=rootschemas+r"/PMSI/PMSI MCO/T_MCOaaD.json")
            table.save(table.schema.descriptor['name'], storage='sql', engine=db)
            modified=True
        
        if modified:
            Base = automap_base()
            Base.prepare(db, reflect=True)
            #redefinition of the foreign key ... I remove the "foreign(...)" to have a read-only relation to Table IR_BEN_R
            Base.classes.CT_IDE_AAAA_GN.ir_ben_r = relationship("IR_BEN_R", primaryjoin="and_(IR_BEN_R.BEN_NIR_PSA == foreign(CT_IDE_AAAA_GN.ben_nir_psa), IR_BEN_R.BEN_RNG_GEM == CT_IDE_AAAA_GN.ben_rng_gem)")
        
        try:
            s = session()
            for p in sim.patients:
                for pi in self.createInsert_ben(Base, sim, p):
                    s.add( pi )
            s.commit()
        except IntegrityError as e: 
            print ( e.orig )
            errorInfo = e.orig.args
            print(errorInfo[0])  #This will give you error code
            print(errorInfo[1])  #This will give you error code
        """
        except sqlite3.IntegrityError as er:
            print('SQLite error: %s' % (' '.join(er.args)))
            print("Exception class is: ", er.__class__)
            print('SQLite traceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            print(traceback.format_exception(exc_type, exc_value, exc_tb))
            #raise
        """
        
        
    def createInsert_PS(self, Base, p):
        professional = Base.classes.DA_PRA_R(
                        PFS_PFS_NUM = p.id, #Numéro du cabinet du praticien à 8 chiffres (2 chiffre dpt+3eme comme categorie professionnelle) !
                        PFS_PRA_SPE = p.speciality,#Spécialité Médicale PS	voir: IR_SPA_D (50: PHARMACIE D OFFICINE)
                        PRA_MEP_COD = 0,#Mode d'exercice particulier	voir IR_MEP_V (0: sans objet, 67: NEPHROLOGIE)
                        CNV_CNV_COD = 1,#Code convention nationale du P.S.	voir IR_CNV_V (1	PRATICIEN CONVENTIONNE)
                        EXC_EXC_NAT = p.catpro,#Nature d exercice	voir IR_EPS_V (1: LIBERAL INTEGRAL, 2: LIBERAL ACTIVITE SALARIEE)
                        PFS_FIN_NUM = p.finess,#NUMERO FINESS
                        IPP_SEX_COD = p.sex,#Sexe du PS
                        PFS_EXC_COM = p.code_commune,#Code commune adr. prof du P.S.
                        PFS_LIB_COM = p.nom_commune,#Libele commune
                        PFS_COD_POS = p.CP,#Code postal adr. prof du P.S.
                        DTE_MOI_FIN = date(1900,1,1),#Mois de traitement
                        DTE_ANN_TRT = "",#
                        TRT_SNI_COD = "",#
                        IPP_IDV_NUM = "",#N.I.R. du P.S
                        IPP_ANN_NAI = "",#Année de naissance du P.S.
                        CAI_NUM = "",# Cpam gestionnaire du P.S.
                        T733_CTR_NUM = "",#
                        CES_CES_COD = "",#Code diplôme du P.S
                        PRA_IDP_DDP = "",#Droit à dépassement
                        PRA_IDP_SAL = "",#Top salarié
                        STA_PFS_NUM = "",#Num. P.S de chainage statistique.
                        STA_CAI_NUM = "",#CPAM resp. relevés stat P.S.
                        STA_CTR_NUM = "",#
                        T733_STA_URC = "",#
                        FIS_PFS_NUM = "",#Numéro chainage fiscal d'un P.S.
                        FIS_CAI_NUM = "",#CPAM resp. relevé fisc. du P.S.
                        FIS_URC_COD = "",#
                        PFS_ACP_DSD = "",#
                        EXC_EFF_DSD = "",#
                        EXC_FIN_MTF = "",#Motif de fin d'exercice libéral	voir EXC_FIN_MTF (3	CESSATION ACTIVITE)
                        PFS_INS_DSD = "",#Date installation
                        PRA_SAL_SPE1 = "",#Spécialité salariés 1
                        PRA_SAL_SPE2 = "",#
                        PRA_SAL_SPE3 = "",#
                        PRA_SAL_SPE4 = "",#
                        PRA_SAL_SPE5 = "",#
                        PRA_SAL_SPE6 = "",#
                        PRA_DIP_NBR = "",#Nb de salariés diplômés
                        PRA_SAL_NBR = "",#Nb de salariés diplômés
                        PFS_SPE_ANT = "",#Spécialité Médicale PS antérieure
                        PFS_AMB_NBR = "",#Effectif du véhicule de type ambulance
                        PFS_VSL_NBR = "",#Effectif du véhicule de type VSL
                        PFS_TXI_NBR = "",#Effectif du véhicule de type Taxi
                        PRA_TOP_REF = "",#Code options conventionnelles
                        ACT_CAB_COD = "",#Code cabinet d'un praticien
                        CAB_REF_DSD = "",#Date début du code options conventionnelles
                        CAB_REF_DSF = "",#Date fin du code options conventionnelles
                        LAB_CAT_COD = "",#Code catégorie laboratoire
                        PRA_CIV_COD = "",#Code civilité du PS
                        
                        PFS_MAJ_DAT = date(1900,1,1),#"Mois de dernière mise à jour durant l'exercice"
                        PFS_SCN_COD = ""#
                    )
        return [professional]


    def createInsert_ben(self, Base, sim, p):
        beneficiaire = Base.classes.IR_BEN_R(
                BEN_NIR_PSA = p.NIR, 	#	chaîne de caractères	Identifiant anonyme du patient dans le SNIIRAM
                BEN_RNG_GEM = p.RNG_GEM,	#	nombre entier	rang de naissance du bénéficiaire
                BEN_NIR_ANO = p.NIR,#,
                BEN_IDT_ANO = p.NIR,#,
                BEN_IDT_TOP = False,#,
                ASS_NIR_ANO = p.NIR,#,
                BEN_IDT_MAJ = date(1900,1,1),
                BEN_CDI_NIR = "00",# voir IR_NIR_V: 00 NIR Normal
                BEN_NAI_ANN = p.BD.year,
                BEN_NAI_MOI = p.BD,
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
            ret += self.createInsert_DrugDelivery(Base, sim, p, d)
        
        for d in p.visits:
            ret.append( self.createPRS(Base, sim, p, d) )
        
        for d in p.medicalacts:
            ret += self.createInsert_MedicalAct(Base, sim, p, d)
            
            
        for d in p.hospitalStays:
            ret += self.createInsert_SejourMCO(Base, sim, p, d)
            
        return ret


    def createInsert_SejourMCO(self, Base, sim, patient, stay):
        """
        return list of database objects
        """
        
        #increment the RSA_Num of the hospital
        stay.hospital.current_RSA_NUM += 1
        
        age = max(1,relativedelta(date(sim.context.year,1,1), stay.patient.BD).years)
        doy_age = int(stay.patient.BD.strftime('%j'))
        if doy_age>365:
            doy_age=365
        
        
        if len(stay.cim_das)>100:
            print("WARNING: stay has a too large number of CIM_DAS to be recorded")
            return 
        if stay.hospital.current_RSA_NUM> 9999999999:
            print("WARNING: stay has a too large RSA_NUM to be recorded")
            return
        
        if len(stay.GHM)<6:
            print("WARNING: the length of the GHM text of a stay is not long enough to be recorded")
            return
        if int(age)<1:
            print("WARNING: the patient age must be >=1 to record a stay")
            return
        if int(doy_age)<0 or int(doy_age)>365:
            print("WARNING: the age (in number of days) must be >=0 and <=365 to record a stay")
            return
        
        if str(patient.Sex) not in ['1', '2', '7', '8', '9']:
            print("WARNING: the sex code is invalid to record a stay")
            return
            
            
        
        sejour = Base.classes.T_MCOaaB(
                ##### Clé #####
                ETA_NUM	=	int(stay.hospital.id),	#	nombre entier	Numéro FINESS e-PMSI
                RSA_NUM	=	stay.hospital.current_RSA_NUM,	  #	nombre entier	N° d'index du RSA
                
                
                ##### Attributs importants (utilisés) ####
                DGN_PAL	=	stay.DP,	                #	chaîne de caractères	Diagnostic principal (DP)
                DGN_REL	=	stay.DRel,	                    #	chaîne de caractères	Diagnostic relié (DR)
                
                RSS_NUM	=	"224",	#enum                #	chaîne de caractères	Numéro de version du format du RSA
                
                GRC_GHM	=	stay.GHM,	        #	chaîne de caractères	GHM calculé par la clinique
                
                SOR_ANN	=	stay.finish_date.year,	            #	année	Année de sortie
                SOR_MOI	=	stay.finish_date,	            #	date	Mois de sortie
                
                ENT_PRV	=	"",	                    #	chaîne de caractères	Provenance
                SOR_DES	=	"",	                    #	chaîne de caractères	Destination
                
                SEJ_NBJ	=	1,	                    #	nombre entier	Durée totale du séjour dans le champ du PMSI (vide si séances)
                
                ####### Autres variables ##############
                AGE_ANN	=	int(age),	          #	nombre entier >=1	Age en années
                AGE_GES	=	1,	                        #	nombre entier <=44	Age gestationnel
                AGE_JOU	=	int(doy_age),	                    #	nombre entier<=365	Day of Year
                
                BDI_COD	=	str(patient.City),	#	chaîne de caractères	Code géographique de résidence !!! je reprends le code commune !!
                BDI_DEP	=	str(patient.Dpt),	#	chaîne de caractères	Code département de résidence
                COD_SEX	=	str(patient.Sex), #enum	#	chaîne de caractères	Sexe
                
                ANT_SUP_NBR	=	0,	                #	nombre entier	Nombre de suppléments antepartum
                AUT_PGV_NBR	=	0,	                #	nombre entier	Nombre d'autorisations d'unités médicales à portée globale valides (Nb_AutPGV)
                BEB_SEJ	=	"",	        #	chaîne de caractères	Type de séjour inférieur à la borne extrême basse
                BEH_NBJ	=	0,	                    #	nombre entier	Nombre de journées au-delà de la borne extrême haute
                CAI_SUP_NBR	=	0,	                #	nombre entier	Nombre de suppléments caisson hyperbare
                DEL_REG_ENT	=	0,	                    #	nombre entier	Délai de la date des dernières règles par rapport à la date d'entrée
                DOS_TYP	=	"1",#enum	                    #	chaîne de caractères	Type de dosimétrie
                ENT_MOD	=	"",	                    #	chaîne de caractères	Mode d'entrée dans le champ du PMSI-MCO
                
                ETE_GHS_NUM	=	1000,	                    #	nombre entier	Numéro de GHS (du GHM GENRSA)
                EXB_NBJ	=	0,	                    #	nombre entier	Nb journées EXB
                GHS_9615_ACT	=	0,	        #	nombre entier	Nombre d'actes menant dans le GHS 9615
                GHS_HS_INNOV	=	1000,	                #	nombre entier	GHS si non prise en compte de l'innovation
                GHS_NUM	=	1000,	                        #	nombre entier	Numéro de GHS (du GHM GENRSA)
                GRC_RET	=	False,	                        #	booléen	Groupage établissement Code Retour
                GRC_VER	=	0,	                        #	nombre entier	Groupage établissement Version classification
                GRG_GHM	=	"xxxxxx",	                #	chaîne de caractères	GHM calculé par le GENRSA
                GRG_RET	=	True,	                        #	booléen	Code retour obtenu par GENRSA
                GRG_VER	=	"11",	 #enum       #	chaîne de caractères	Groupage GENRSA :Version de la classification
                INNOV_NUM	=	"xxxxxxxxxxxxxxx",	            #	chaîne de caractères	Numéro d'innovation
                MACH_TYP_RAD	=	"1",	#enum #	chaîne de caractères	Type de machine en radiothérapie
                NBR_ACT	=	1,	                    #	nombre entier	Nombre de zones d'actes (nA) dans ce RSA
                NBR_DGN	=	len(stay.cim_das),	    #	nombre entier	Nombre de diagnostics associés significatifs (nDAS) dans ce RSA
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
                PAS_LIT_DED	=	False,	                    #	booléen	Passage dans un lit dédié de soins palliatifs
                PLO_ACT	=	0,	                        #	nombre entier	Type de prestation de prélèvement d'organe
                POI_NAI	=	0,	                    #	nombre entier	Poids d'entrée (en grammes)
                
                RTH_SUP_NBR	=	0,	                #	nombre entier	Nombre de zones de suppléments de radiothérapie (Nb_Rdth)
                SEJ_COD_CONF	=	"",	#enum            #	chaîne de caractères	Confirmation du codage du séjour
                SEJ_TYP	=	"A",	#enum        #	chaîne de caractères	Type de séjour
                SEQ_RUM	=	10,	                    #	nombre entier	N° séquentiel du RUM ayant fourni le DP (RUM:Résumé d'Unité Médicale)
                SOR_MOD	=	"",	                    #	chaîne de caractères	Mode de sortie du champ PMSI-MCO
                SUP_ENT_DPA	=	0,	            #	nombre entier	Nombre de suppléments pour les entraînements à la dialyse péritonéale automatisée hors séances
                SUP_ENT_DPC	=	0,	            #	nombre entier	Nombre de suppléments pour les entraînements à la dialyse péritonéale continue ambulatoire hors séances
                SUP_ENT_HEM	=	0,	            #	nombre entier	Nombre de suppléments pour les entraînements à l'hémodialyse hors séances
                SUP_HEM_HS	=	0,	            #	nombre entier	Nombre de suppléments pour hémodialyse hors séances
                SUP_RAD_PED	=	0,	            #	nombre entier	Nombre de suppléments radiothérapie pédiatrique
                TAR_SEQ_NUM	=	"003",	#enum    #	chaîne de caractères	Numéro séquentiel de tarifs
                TOP_AVASTIN	=	0,	                    #	booléen	Top Radiation partielle Avastin
                TOP_DEF_CARD	=	0,	                #	booléen	Supplément défibrillateur cardiaque
                TOP_GHS_MIN_SUS	=	0,	                #	booléen	Top GHS minoré
                TOP_VLV_AOR	=	0,	                    #	booléen	Top valves aortiques percutanées
                TYP_GEN_RSA	=	0,	                    #	nombre entier	Type de génération automatique du RSA
                UHCD_TOP	=	0,	                    #	booléen	Top UHCD
                
                NBR_NAIS_ANT=0, # 	nombre entier 	Nombres de naissances vivantes antérieures 	
                GHS_9512_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9512 	
                GHS_9515_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9515 	
                GHS_9511_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9511 	
                GHS_9619_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9619 	
                GHS_9610_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9610 	
                NBR_IVG_ANT =0, # 	nombre entier 	Nombre d’IVG antérieures 	
                GHS_9620_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9620 	
                NBR_SUP_SSC =0, # 	nombre entier 	Nombre de suppléments pour SSC (surveillance continue) 	
                GHS_9622_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9622 	
                GHM_24707Z_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHM 24Z07Z ou 28Z13Z 	
                GHS_9621_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9621 	
                NBR_SEA_SROS =0, # 	nombre entier 	Nombre de séances avant SROS 	
                GHM_24706Z_ACT=0, #  	nombre entier 	Nombre d'actes menant dans le GHM 24Z06Z ou 28Z12Z 	
                GHM_24705Z_ACT=0, #  	nombre entier 	Nombre d'actes menant dans le GHM 24Z05Z ou 28Z11Z 	
                FAIS_NBR =0, # 	nombre entier 	Nombre de faisceaux 	
                GHS_9611_ACT=0, #  	nombre entier 	Nombre d'actes menant dans le GHS 9611 	
                GHS_9612_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9612 	
                GHS_9510_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9510 	
                GHS_9524_ACT =0, # 	nombre entier 	Nombre d'actes menant dans le GHS 9524 	
                DLY_ACT="", #  	chaîne de caractères 	Forfait dialyse 	
                GHS_6523_ACT=0, #  	nombre entier 	Nombre d'actes menant dans le GHS 6523 	
                NBR_SUP_SRA=0, #  	nombre entier 	Nombre de suppléments pour SRA (réanimation) 	
                COD_IGS=0, #  	chaîne de caractères 	IGS 2 	
                ANN_IVG_PREC=0 #  	année 	Année de l’IVG précédente
        )
        
        
        sejourB = Base.classes.T_MCOaaC(
            ## 
            ETA_NUM	=	sejour.ETA_NUM,	                #	nombre entier	Numéro FINESS e-PMSI
            RSA_NUM	=	sejour.RSA_NUM,	                #	nombre entier	N° séquentiel dans fichier PMSI
            
            ## Variables importantes -> le lien au bénéficiare + dates
            NIR_ANO_17	=	patient.NIR,	#	chaîne de caractères	N° anonyme
            EXE_SOI_DTD	=	datetime(stay.start_date.year,stay.start_date.month,stay.start_date.day,12,0,0),	#	date et heure	Date d'entrée (date)
            EXE_SOI_DTF	=	datetime(stay.finish_date.year,stay.finish_date.month,stay.finish_date.day,12,0,0),	    #	date et heure	Date de sortie (date)
            
            SEJ_NUM	=	10000,	#	nombre entier	N° de séjour
            
            RNG_BEN	=	str(patient.RNG_GEM),	#	chaîne de caractères	Rang du bénéficiaire
            RNG_NAI	=	patient.RNG_GEM,	    #	nombre entier	Rang de naissance
            
            ENT_DAT	=	stay.start_date,	    #	date	date d'entrée
            EXE_SOI_AMD	=	stay.start_date,	#	année et mois	Date d'entrée au format année + mois
            
            SOR_DAT	=	stay.finish_date,	    #	date	date de sortie
            EXE_SOI_AMF	=	stay.finish_date,	#	année et mois	Date de sortie au format année + mois
            SOR_ANN	=	stay.finish_date,	    #	année	Année de sortie  //-> utilisé comme clé de jointure avec B, donc je mets les mêmes ici
            SOR_MOI	=	stay.finish_date,	    #	date	Mois de sortie   //-> utilisé comme clé de jointure avec B, donc je mets les mêmes ici
            
            ENT_AM="", #Date d'entrée au format année + mois (text en sqlite)
            SOR_AM="", #Date d'entrée au format année + mois (text en sqlite)
            
            NIR_ANO_MAM	=	"xxxxxxxxxx_xxxxxxxxxx_xxxxxxxxxx",	#	len>=32 chaîne de caractères	N° anonyme mère-enfant ???
            HOS_NN_MAM	=	0,	        #	booléen	Hospitalisation d'un nouveau-né auprès de la mère
            HOS_PLO	=	0,	            #	booléen	Hospitalisation pour prélèvement d'organe
            
            FOR_NUM	=	"010", #enum,	#	chaîne de caractères	N° format
            VID_HOSP_FOR	=	100,      	#	nombre entier	N° format VID-HOSP
            ORG_CPL_NUM	=	"",	#	chaîne de caractères	N° d’organisme complémentaire
            ORG_CPL_NUM_RET	=	1,	        #	booléen	Code retour contrôle " N° d’organisme complémentaire"
            
            NUM_DAT_AT	=	100000000,	#	nombre entier	Numéro accident du travail ou date d’accident de droit commun
            
            COH_NAI_RET	=	1,	#	booléen	Code retour contrôle « Cohérence date naissance »
            COH_SEX_RET	=	1,	#	booléen	Code retour contrôle « Cohérence sexe »
            DAT_RET	=	"0", #enum	#	chaîne de caractères	Code retour contrôle « date de référence» (date d'entrée)
            ETA_NUM_RET	=	1,	#	booléen	Code retour contrôle "N° FINESS d’inscription e-PMSI"
            FHO_RET	=	1,	    #	booléen	Code retour « fusion ANO HOSP et HOSP PMSI »
            HOS_NNE_RET	=	1,	#	booléen	Code retour contrôle « Hospitalisation d'un nouveau-né auprès de la mère »
            HOS_ORG_RET	=	1,	#	booléen	Code retour contrôle « Hospitalisation pour prélèvement d'organe »
            NAI_RET	= "1",	#enum    #	chaîne de caractères	Code retour contrôle « date de naissance »
            NIR_RET	= 1,	    #	booléen	Code retour contrôle « n° sécurité sociale »
            NUM_DAT_AT_RET = 1,	#	booléen	Code retour contrôle " Numéro accident du travail ou date d’accident de droit commun"
            PMS_RET	=	1,	    #	booléen	Code retour « fusion ANO PMSI et fichier PMSI »
            RNG_BEN_RET	= 1,	#	booléen	Code retour contrôle « Rang du bénéficiaire »
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
            sejourD = Base.classes.T_MCOaaD(
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
            IDE_ETA_NUM	=	eta.id, #Numéro Finess de l'Etablissement
            IDE_ETA_NU8	=	eta.id, #Numéro Finess de l'Etabt sans clé
            IDE_ETA_NOM	=	eta.rs, #Raison Sociale Abrégée
            IDE_IDE_CPL	=	"", #Complément d'identification
            IDE_VOI_NUM	=	eta.numvoie, #Numéro dans la voie
            IDE_VOI_CNU	=	"", #Complément Numéro de voie
            IDE_VOI_TYP_LRG	=	eta.typvoie, #Nature de la voie
            IDE_VOI_LIB	=	eta.voie, #Nom de la voie
            IDE_ADR_CPL	=	"", #Complément d'adresse
            IDE_RSD_LIB	=	eta.nom_commune, #Nom de la localité ou bureau distributeur
            IDE_BDI_COD	=	eta.cp, #Code postal
            IDE_TEL_NU1	=	"", #Numéro téléphone1
            IDE_TEL_NU2	=	"",
            IDE_INT_ADR	=	"", #Adresse internet
            IDE_FAX_NUM	=	"",
            IDE_CPT_RSC	=	"", #Raison sociale complète
            IDE_INS_COM	=	str(eta.dpt)+" "+str(eta.code_commune), #Numéro de Département Numéro de Commune
            IDE_CPA_NUM	=	"", #Code Caisse de rattachement de l'Etablissement
            IDE_CRA_NUM	=	"", #Code CRAM de rattachement de l'Etablissement
            IDE_NUM_NOU	=	"", #Nouveau FINESS
            IDE_NUM_DTE	=	datetime(1972,6,1,0,0,0), #Date nouveau Finess
            IDE_PSH_CAT	=	"", #Code catégorie de l'Etablissement
            IDE_CCL_DTE	=	datetime(1928,6,4,0,0,0), #Date de classement de l'Etablissement
            IDE_PSH_STJ	=	"", #Statut Juridique de l'Etablissement
            IDE_STJ_DTE	=	datetime(1970,12,18,0,0,0),#Date d'effet du Statut Juridique de l'Etablissement
            IDE_PSH_MFT	=	"",#Code MFT de l'Etablissement
            IDE_MFT_DTE	=	datetime(1938,7,7,0,0,0), #Date d'effet du MFT de l'Etablissement
            IDE_HON_TYP	=	"", #Code Type d'Honoraire de l'Etablissement
            IDE_HON_DTE	=	datetime(1924,2,16,0,0,0), #Date d'effet du Type d'Honoraire de l'Etablissement
            IDE_GES_NUM	=	"", #Numéro Finess Gestionnaire
            IDE_GES_DTE	=	datetime(1983,7,21,0,0,0), #Date d'effet du rattachement
            IDE_SRT_NUM	=	"", #SIRET
            IDE_ETA_DTE	=	datetime(1973,12,18,0,0,0), #Date d'effet du Code Activité de l'Etablissement
            IDE_ATI_COD	=	"", #Code Activité de l'Etablissement
            IDE_PSP_NAT	=	"", #Code PSPH de l'Etablissement
            IDE_PSP_DTE	=	datetime(1906,4,7,0,0,0), #Date d'effet du PSPH de l'Etablissement
            IDE_BGL_PEX	=	"",
            IDE_ETA_TYP	=	"",
            IDE_COD_A24	=	"",
            IDE_SAN_PUB	=	"",
            IDE_IDE_EAM	=	"3583766311", #Code AM de l'Etablissement
            IDE_IDE_DAM	=	datetime(1905,12,27,0,0,0), #Date de Début de l'AM de l'Etablissement
            IDE_IDE_FAM	=	datetime(1980,8,26,0,0,0),
            IDE_IDE_EAS	=	"4265207648", #Code AS de l'Etablissement
            IDE_IDE_DAS	=	datetime(1985,4,26,0,0,0),
            IDE_IDE_FAS	=	datetime(1984,3,8,0,0,0),
            IDE_IDE_CON	=	"2912602989", #Code contrat de l'Etablissement
            IDE_IDE_DON	=	datetime(1919,11,25,0,0,0),
            IDE_IDE_FON	=	datetime(1926,2,18,0,0,0),
            IDE_NUM_ANC	=	"KLYJHJ", #No Finess ancien
            IDE_ANC_DTE	=	datetime(1978,10,30,0,0,0), #Date Finess ancien
            IDE_IDE_CTR	=	"2", #Code convention tripartite
            IDE_IDE_DTR	=	datetime(1929,9,30,0,0,0),
            IDE_IDE_FTR	=	datetime(1927,11,29,0,0,0),
            IDE_A24_24	=	"B",
            IDE_A24_24B	=	"Kc",
            IDE_A24_24C	=	"mMy",
            IDE_A24_24Q	=	"D",
            IDE_A24_24S	=	"I",
            IDE_A24_24T	=	"R",
            IDE_NUM_PCP	=	"VMSSnUe", #Code établissement principal
            IDE_CBU_CTR	=	"5", #Code contrat médicaments
            IDE_CBU_EFF	=	datetime(1910,11,21,0,0,0), #Date effet contrat médicaments
            IDE_CBU_TAU	=	"HbJ",
            IDE_CBU_DTF	=	datetime(1921,2,25,0,0,0),
            IDE_FIN_IND	=	"",
            IDE_CAI_PIV	=	"oD",
            IDE_NAT_ORI	=	"YpM", #Code Nature de l'Origine de l'Etablissement (CPAM ou CRAM)
            IDE_MCO_COE	=	"77652", #Coefficient MCO
            IDE_COE_HAD	=	"65768", #Coefficient HDA
            IDE_MCO_DTE	=	datetime(2010,10,5,0,0,0),
            IDE_HAD_DTE	=	datetime(1919,9,18,0,0,0),
            IDE_IMP_DPT	=	"Xm"
        )
        
        #Création de l'etablissement pour les volets MCO et SSR
        etablissementpmsi = Base.classes.T_MCOaaE(
            ###### clés de la table #####
            ETA_NUM	=	etablissement.IDE_ETA_NUM,	    #	chaîne de caractères	Numéro FINESS e-PMSI
            
            ### Autres attributs 
            ANN_TRT	=	"",	        #	chaîne de caractères	N° du trimestre PMSI transmis
            ETB_EXE_FIN	=""	,	    #	chaîne de caractères	N°FINESS sans clé
            REG_ETA	=	"",	        #	chaîne de caractères	Région
            SOC_RAI	=	"",	        #	chaîne de caractères	Raison sociale
            STA_ETA	=	"",	        #	chaîne de caractères	Statut de l'établissement
            VAL_ETA	=	""	        #	chaîne de caractères	Validation des données
        )
        
        #recopie du même établissement pour les SSR
        etablissementssr = Base.classes.T_SSRaaE(
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


    def createPRS(self, Base, sim, p, ma):
        
        
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
            PRS_ORD_NUM	=	1,	                        #	nombre entier // Numéro d'ordre de la prestation dans le décompte
            REM_TYP_AFF	=	1,	                    #	nombre entier // type de remboursement affiné (pas de ref)
        
            ##### Identification du bénéficiaire ######
            BEN_NIR_PSA	=	ma.patient.NIR,	            #	chaîne de caractères    # FOREIGN KEY: IR_BEN_R [ BEN_NIR_PSA, BEN_RNG_GEM ]
            BEN_RNG_GEM	=	ma.patient.RNG_GEM,	            #	nombre entier           # FOREIGN KEY: IR_BEN_R [ BEN_NIR_PSA, BEN_RNG_GEM ]
        
            ###### identification des acteurs de santé (EXE: executant, PRE: prescripteur, MTT: medecin traitant) ######
            ETB_PRE_FIN	=	ma.prescriber.finess,	 #	chaîne de caractères  n°FINESS de l'etablissement prescripteur   # FOREIGN KEY:      BE_IDE_R [ IDE_ETA_NU8 ]
            PFS_EXE_NUM	=	ma.provider.id,	         #	chaîne de caractères    # FOREIGN KEY:  DA_PRA_R [ PFS_PFS_NUM ]
            #PFS_EXE_NUMC	=	"",	            #	
            PFS_PRE_NUM	=	ma.prescriber.id,	                    #	chaîne de caractères    # FOREIGN KEY:  DA_PRA_R [ PFS_PFS_NUM ]
            #PFS_PRE_NUMC	=	"",	                    #	
            PRS_MTT_NUM	=	ma.patient.MTT.id,	                    #	chaîne de caractères    # FOREIGN KEY:  DA_PRA_R [ PFS_PFS_NUM ]
            #PRS_MTT_NUMC	=	"",	            #	
        
            PSE_ACT_NAT	=	ma.code_nature,	       #	nombre entier // Nature d'activité du professionnel de santé exécutant  IR_ACT_V[PFS_ACT_NAT], 50: Pharmacie d'officine
        
            BEN_RES_COM	=	ma.patient.City,	#	chaîne de caractères        #Code Commune (Bénéficiaire)
            BEN_RES_DPT	=	ma.patient.Dpt,	#	chaîne de caractères        #Code Département (Bénéficiaire)
            BEN_SEX_COD	=	int(ma.patient.Sex),	    #	nombre entier      #Code Sexe (Bénéficiaire)
            BEN_NAI_ANN	=	ma.patient.BD.year,	#	année                       #Annee de naissance du bénéficiaire
            BEN_AMA_COD	=	1000,	#	nombre entier
            BEN_CDI_NIR	=	"99",	#	chaîne de caractères
            BEN_CMU_CAT	=	7,	#	nombre entier
            BEN_CMU_ORG	=	"01C731032",	#	chaîne de caractères
            BEN_CMU_TOP	=	0,	#	nombre entier //beneficiaire CMU=89, 0=Non CMU
            BEN_DCD_AME	=	"000101",	#	année et mois de décès (en texte YYYYMM)
            BEN_DCD_DTE	=	datetime(1,1,1,0,0,0),	#	date de décès
            BEN_EHP_TOP	=	1,	#	nombre entier
            BEN_IAT_CAT	=	"06",	#	chaîne de caractères
            BEN_PAI_CMU	=	1,	#	nombre entier           
            BEN_QAF_COD	=	25,	#	nombre entier
        
            ORG_AFF_BEN	=	"01C731221",	#	chaîne de caractères
            PRS_REJ_TOP	=	0,	#	nombre entier
            
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
            CPL_REM_BSE	=	0.0,	#	nombre réel
            CPL_REM_MNT	=	0.0,	#	nombre réel
            CPL_REM_PRU	=	0.0,	#	nombre réel
            CPL_REM_SGN	=	0,	#	nombre entier
            PRS_ACT_CFT	=	0.0,	#	nombre réel
            PRS_ACT_COG	=	0.0,	#	nombre réel
            PRS_ACT_NBR	=	0,	#	nombre réel Dénombrement signé d'actes (pour les indemnités journalières)
            PRS_ACT_QTE	=	0,	#	nombre réel Quantité signé d'actess
            
            PRS_DEP_MNT	=	0.0,	#	nombre réel, montant déplacemnt
            PRS_ETA_RAC	=	0.0,	#	nombre réel
            PRS_PAI_MNT	=	0.0,	#	nombre réel
            RGO_MOD_MNT	=	0.0,	#	nombre réel
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
            IJR_EMP_NUM	= 0,	#	nombre entier
            IJR_RVL_NAT	=	"MP",	#	chaîne de caractères
            MTM_NAT	=	3,	#	nombre entier
            ORG_CLE_NEW	=	"01C731220",	#	chaîne de caractères
            PRE_REN_COD	=	3,	#	nombre entier
        
            PRS_CRD_OPT	=	4,	#	nombre entier
            PRS_DPN_QLP	=	12,	#	nombre entier
            PRS_NAT_REF	=	str(ma.code_pres),	#	nombre entier // Code de la Prestations de référence IR_NAT_V[PRS_NAT]
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
            PSE_REF_ADH	=	"0",	#	chaîne de caractères enum "0" ou "2" Top prestation exécuté par un professionnel de santé adhérent à l'option référent
            PSE_SPE_COD	=	0,	#	nombre entier Spécialite médicale du professionnel de santé exécutant
            PSE_STJ_COD	=	0,	#	nombre entier Mode d'exercice du professionnel de santé exécutant
        
            PSP_ACT_NAT	=	26,	#	nombre entier
            PSP_CNV_COD	=	9,	#	nombre entier
            PSP_PPS_NUM	=	"bZmhONjhyObf",	#	chaîne de caractères
            #PSP_PPS_NUMC	=	"GcqeAZYKBpTJuLM",	#	
            PSP_REF_ADH	=	"0",	#	chaîne de caractères, enum "0" ou "2"
            PSP_SPE_COD	=	10,	#	nombre entier
            PSP_STJ_COD	=	61,	#	nombre entier
            PSP_SVI_PPS	=	15,	#	nombre entier
        
            BEN_DRT_SPF	=	"ACS",	#	chaîne de caractères
            BEN_ACS_TOP	=	False,	#	booléen
            EXE_CTX_PFS	=	"xxxx",	#	chaîne de caractères
            PRS_TYP_MAJ	=	"ZZ",	#	chaîne de caractères
            EXE_CTX_BEN	=	"xxxx",	#	chaîne de caractères
            CPL_FTA_COD	=	59,	#	nombre entier
            PRS_PPU_SEC	=	9,	#	nombre entier
            BEN_CTA_TYP	=	865,	#	nombre entier
            PRS_DRA_AME	=	"Vqt",	#	année et mois Date réelle (année et mois) de l'accouchement
            DRG_AFF_NAT	=	11,	#	nombre entier
            PRS_MNT_MAJ	=	0.0,	#	nombre réel
            PRE_IND_PEL	=	"ZZ",	#	chaîne de caractères Indicateur Prescription en Ligne
            PRS_DIS_PRE	=	"BF",	#	chaîne de caractères
            CPL_REM_TAU	=	0.0,	#	nombre réel
            PRS_QTT_MAJ	=	7460	#	nombre entier Quantité de majorations
        )
        return prestation
    
    def createInsert_MedicalAct(self, Base, sim, p, ma):
        """
        Insertion in Base of the drug delivery for patient p
        - Base ORM database model
        - d DrugDelivery
        - p Patient
        """
        prestation = self.createPRS(Base, sim, p, ma)
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
        
            CAM_MI4_MNT	=	0.0,	#	nombre réel	Montant Minoration due à association sur Modificateur4
            CAM_MM4_MNT	=	0.0,	#	nombre réel	Montant Majoration Modificateur4
            CAM_GRI_TAR	=	"oBHGsDvOoCCJs",	#	chaîne de caractères	Grille tarifaire
            CAM_SUP_MNT	=	0.0,	#	nombre réel	Montant supplément de Charge en Cabinet
            CAM_MI1_MNT	=	0.0,	#	nombre réel	Montant Minoration due à association sur Modificateur1
            CAM_NRM_MNT	=	0.0,	#	nombre réel	Montant Non Rmb du à annulation du tarif pr acte non rmb
            CAM_MI3_MNT	=	0.0,	#	nombre réel	Montant Minoration due à association sur Modificateur3
            CAM_MM2_MNT	=	0.0,	#	nombre réel	Montant Majoration Modificateur2
            CAM_MM3_MNT	=	10.0,	#	nombre réel	Montant Majoration Modificateur3
            CAM_RED_MNT	=	0.0,	#	nombre réel	Montant réduction Tarif due à praticien non conventionné
            CAM_MPU_MNT	=	0.0,	#	nombre réel	Montant Minoration due à association sur PU de l' Acte
            CAM_MM1_MNT	=	0.0,	#	nombre réel	Montant Majoration Modificateur1
            CAM_MI2_MNT	=	0.0,	#	nombre réel	Montant Minoration due à association sur Modificateur2
                
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

    def createInsert_DrugDelivery(self, Base, sim, p, d):
        """
        Insertion in Base of the drug delivery for patient p
        - Base ORM database model
        - d DrugDelivery
        - p Patient
        """
        
        prestation = self.createPRS(Base, sim, p, d)
        
        #Création d'une entrée dans la table ER_PHA pour une entrée en correspondante en pharmacie
        delivrance_medoc = Base.classes.ER_PHA_F(
            PHA_PRS_C13	=	str(d.cip13),	#	nombre entier	Code CIP de la pharmacie de ville (13 Caractères)   IR_PHA_R[PHA_CIP_C13]
            PHA_PRS_IDE	=	str(d.cip13),	#	nombre entier	Code CIP de la pharmacie de ville (ancien code sur 7 Caractères)  IR_PHA_R[PHA_CIP_C13]
            PHA_SEQ_RNV	=	d.sid,	#	nombre entier	Séquence de renouvellement
            PHA_SUB_MTF	=	0,	#	nombre entier	Motif de substitution du médicament IR_SUB_V[PHA_SUB_MTF] 0: sans objet, 2: generique, 6: refus de substitution
            PHA_ACT_QSN	=	d.quantity,	#	nombre entier	Quantité affinée signée (= nombre de boites facturées)
        
            ORG_CLE_NEW	=	"01C731222",	#	chaîne de caractères	Code de l'organisme de liquidation
            PHA_ACT_PRU	=	0.0,	#	nombre réel	Prix unitaire du médicament codé en CIP
            PHA_CPA_PCP	=	"xxx",	#	chaîne de caractères	Condition particulière de prise en charge
            PHA_DEC_PRU	=	0.0,	#	nombre réel	Prix unitaire de l'unité déconditionnée délivrée
            PHA_DEC_QSU	=	0,	#	nombre entier	Quantité complète de déconditionnement signée
            PHA_DEC_TOP	=	"",	#	chaîne de caractères	Top déconditionnement
            PHA_IDE_CPL	=	0,	#	nombre entier	Préfixe du code CIP
            PHA_MOD_PRN	=	"",	#	chaîne de caractères	Mode de prescription
            PHA_ORD_NUM	=	1,	#	nombre entier	Numéro d'ordre de la prestation affinée pharmacie        
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
    if 0: #basic simulation
        sim = simulation(nomencl="/home/tguyet/Progs/medtrajectory_datagen/datarep/snds_nomenclature.db")
    else: #Simulation based on open data
        sim = OpenSimulation(nomencl="/home/tguyet/Progs/medtrajectory_datagen/Generator/snds_nomenclature.db",
                     datarep="/home/tguyet/Progs/medtrajectory_datagen/datarep")
        sim.nb_patients=100
        sim.dpts=[35]    
    
    sim.run()
    dbgen = simDB()
    dbgen.generate(sim, rootschemas="../external/schema-snds-master/schemas")
