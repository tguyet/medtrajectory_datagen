#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Data model for a SNDS data simulator.

This file contains a list of classes that represent our data model.
This data model is both :
* a high level conceptual framework that can be manipulated programmatically to create simulation, and 
* a low level data representation (that can easily be mapped to the SNDS dataset)

@author: Thomas Guyet
@date: 08/2020
"""

from datetime import date
import warnings


#########################################
# https://documentation-snds.health-data-hub.fr/fiches/professionnel_sante.html

class PS: #professionnel de soins
    def __init__(self):
        self.id="00000000000" #PFS_PFS_NUM: c'est le numéro du cabinet du praticien à 8 chiffres (2 chiffre dpt+3eme comme categorie professionnelle) ! (numéro PS officiel à 11 chiffres ... pas encore en place)
        self.dpt="" #code dpt a 2 chiffres !! (different de IR_DPT_V)
        self.catpro=9 # voir IR_CAT_V
        self.finess=""
        self.code_commune=""
        self.nom_commune=""
        self.CP=""
        self.sex=9
        

class Physician(PS):
    """
    categorie professionnelle 1: médecins
    """
    def __init__(self):
        PS.__init__(self)
        self.speciality=0 #PSP_SPE_COD (prescripteur), voir IR_SPE_V / IR_SPA_D, 0="non renseigné" (plutôt que PSE_SPE_COD (executant))
        self.catpro=1
        
    def __str__(self):
        return "Physician ("+str(self.id)+") [spe: "+str(self.speciality)+"]"
        
class GP(Physician):
    def __init__(self):
        Physician.__init__(self)
        self.speciality=1 #PSP_SPE_COD (prescripteur), voir IR_SPE_V / IR_SPA_D, 1="Médecine Generaliste" (plutôt que PSE_SPE_COD (executant))
    

class Specialist(Physician):
    def __init__(self):
        Physician.__init__(self)
        self.speciality=0  #valeur entre 2 et 85 dans la liste IR_SPE_V / IR_SPA_D (virer le 36: dentiste ???)

class Provider(PS):    
    """
    categorie professionnelle 2: fournisseurs / pharmacies / transporteurs 
    """
    def __init__(self):
        PS.__init__(self)
        self.speciality=None #PSE_SPE_COD (executant)
        self.cat_nat=50 #PSE_ACT_NAT (nature), voir codes dans IR_ACT_V[PFS_ACT_NAT] -> pharmacie 50
        self.catpro=2 # voir IR_EPS_V (d'après doc en ligne, mais je l'ai pas !!) 2: LIBERAL ACTIVITE SALARIEE
    def __str__(self):
        return "Care Provider ("+str(self.id)+") [cat: "+str(self.cat_nat)+"]"
        
######################################

class ALD:
    def __init__(self):
        self.motif="" #CIM10 medical motif
        self.start=date(1900,1,1)
        self.finish=date(2100,1,1)

class Patient:
    def __init__(self):
        self.NIR=0
        self.RNG_GEM=1
        self.Sex=0
        self.BD=date(1900,1,1) #birth day
        self.Dpt="01"
        self.City="0000"
        self.ALD=[] #list of ALD (see class ALD)
        self.MTT=None #médecin traitant
        
        self.drugdeliveries=[]
        self.visits=[]
        self.medicalacts=[]
        self.hospitalStays=[]
        
        
    def __str__(self):
        s="patient ("+self.NIR+") [sex: "+str(self.Sex)+", birth:"+self.BD.isoformat()+", loc:("+str(self.Dpt)+","+str(self.City)+")"
        if len(self.ALD)>0:
            s+=", ALD:" + str(self.ALD[0])
            for ald in self.ALD[1:]:
                s+=","+str(ald)
        s+="]"
        return s


class Etablissement:
    def __init__(self):
        self.id="dfdsfds" #numeru finess
        self.current_RSA_NUM=1
        self.rs="" #raison sociale / nom
        self.dpt=""
        self.code_commune=""
        self.nom_commune=""
        self.cp=""
        self.cat="" #catégorie etablissement
        self.numvoie=""
        self.typvoie=""
        self.voie=""
    def __str__(self):
        return "Etablissement ("+str(self.id)+") [cat: "+str(self.cat)+","+self.rs+","+self.nom_commune+"]"
    
class CareDelivery:
    """
    - This class correspond to the concepts of the PRS table
    
    A caredelivery is characterized by:
        - a date (start and finish)
        - patient
        - a care provider
        - a prescriber (by default is it the usual general practitioner patient)
    
    The table key are not really related to the care ... and does not seems really interesting to represent here!
    """
    
    current_ord_num=1 ##to use as a static member
    
    def __init__(self, patient, provider, prescriber=None):
        """
        - patient object that represents a patient
        - provider object that represents a care provider (eg pharmacy)
        """
        self.ord_num = CareDelivery.current_ord_num #    nombre entier // numéro d'ordre du décompte dans l'organisme
        CareDelivery.current_ord_num+=1
        self.patient=patient
        self.provider=provider
        if prescriber:
            self.prescriber=prescriber
        else:
            self.prescriber=patient.MTT
        self.date_debut=date(1900,1,1)
        self.date_fin=date(2020,1,1)
        self.code_pres=None #6012: drug delivery IR_NAT_V[PRS_NAT]
        self.code_nature=None #50: drug delivery IR_ACT_V[PFS_ACT_NAT]


class DrugDelivery(CareDelivery):
    """
    class associated to the ER_PHA table
    """
    def __init__(self, CIP, patient, provider, prescriber=None):
        if provider.catpro!=50:
            warnings.warn("DrugDelivery but not a pharmacy")
        super().__init__(patient, provider, prescriber)
        self.code_nature=50
        self.code_pres=6012
        self.quantity=1
        self.sid=1 #delivery sequence order
        self.cip13=CIP #CIP code of the drug
        
    def __str__(self):
        return "patient ("+self.patient.NIR+") gets drug " + str(self.cip13)+" at date "+self.date_debut.isoformat()+"."
        
class DrugDeliverySequence(CareDelivery):
    """
    A drug sequence represents a collection of deliveries of the same drug (for example, for chronic diseases)
    """
    def __init__(self,patient, provider, prescriber=None):
        if provider.catpro!=50:
            warnings.warn("DrugDelivery but not a pharmacy")
        super().__init__(patient, provider)
        self.quantity=1
        self.cip13=None #CIP code of the drug
        self.number=1
        self.frequency=30 #

class MedicalVisit(CareDelivery):
    """
    A medical visit corresponds to a outpatient medical visit
    -> a medical visit does not have necessarily related medical acts
    """
    def __init__(self, patient, provider, prescriber=None):
        super().__init__(patient, provider, prescriber)
        self.code_pres=1111     # 1111: consultation côté C
        self.code_nature=0      # 0: non-renseigné
        
        #self.actes=[]   #list of MedicalActs qui ont été délivrés pdt la consutation [?]

    def __str__(self):
        return "patient ("+self.patient.NIR+") visited " + str(self.provider.id)+ " (" + str(self.code_pres)+") at date "+self.date_debut.isoformat()+"."
        

class MedicalAct(CareDelivery):
    """
    A medical act correspond to a outpatient medical acts (including dental cares)
    
    KWIKLY: le code descriptif détaillé d’un acte CCAM est constitué du triplet CAM_PRS_IDE (identifiant du code acte CCAM) + CAM_ACT_COD (code activité) + CAM_TRT_PHA (code phase de traitement).
    + https://sofia.medicalistes.fr/spip/IMG/pdf/codage_ccam.pdf
    """
    def __init__(self, CCAM, patient, provider, prescriber=None):
        super().__init__(patient, provider, prescriber)
        self.code_nature=0 #0: non-renseigner
        self.code_pres = 0 #0: Sans objet ... à mettre en relation avec le code acte
        self.code_ccam = CCAM
        
        self.activitycode = "1"
        """ 
        1 pour l’acte principal,
        2 pour le2ème geste éventuel d'un même acte,
        3 pour le 3ème geste éventuel d'un même acte,
        4 pour le geste d’anesthésie générale ou locorégionale,
        5 pourla surveillance d'une circulation extracorporelle
        
        Un même acte de chirurgie avec anesthésie à 2 entrées dans la base (même code CCAM)
        """
        
        self.treatmentphase = 0
        
    def __str__(self):
        return "patient ("+self.patient.NIR+") gets medical act " + str(self.code_ccam)+" at date "+self.date_debut.isoformat()+"."
        
class ShortHospStay:
    """
    Short stay in a hospital (Sejour MCO from PMSI)
    
    Ajouter les GHM ?? [pas utilisés par André]
    """
    
    def __init__(self, patient, hospital, CIM_DP):
        
        self.patient = patient
        self.hospital = hospital
        self.DP = CIM_DP            # principal diagnosis
        self.DRel = ""              # related diagnosis
        self.cim_das = []           # associate diagnosis
        self.ccam = []              # list of CCAM codes (medical acts) that have been delivered during the stay
        #self.from = None           # from another stay before (PMSI care pathways)
        #self.to = None             # to another stay after (PMSI care pathways)
        self.start_date = None      # beginning of the stay 
        self.finish_date = None     # endding of the stay
        self.GHM = ""
        
        
    def __str__(self):
        return "patient ("+self.patient.NIR+") gone to hospital with diagnosis " + str(self.DP)+" from "+self.start_date.isoformat()+" to "+self.finish_date.isoformat()+"."
    
