# -*- coding: utf-8 -*-
"""
SNDS data simulator based on Open Data

This files contains data factory specialisations.

@author: Thomas Guyet
@date: 08/2020
"""

from data_factory import FactoryContext, PharmacyFactory, EtablissementFactory, PhysicianFactory, PatientFactory, ShortStayFactory, DrugsDeliveryFactory, VisitFactory, ActFactory
from database_model import Provider, Etablissement, GP, Specialist, Patient, ShortHospStay, DrugDelivery, MedicalVisit, MedicalAct
import os
import numpy as np
import pandas as pd
import numpy.random as rd
import json
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import warnings


class OpenDataFactoryContext(FactoryContext):
    """
    Attributes
    ----------
    conn (inherited): (readonly) database connection to a database containing SNDS nomenclatures (default connection to "snds_nomenclature.db")
    datarep: repository of the data files
    year: simulation year (default: this year)
    
    """    
    def __init__(self, nomenclatures=None, datarep="./data/"):
        if nomenclatures:
            super().__init__(nomenclatures)
        else:
            super().__init__(os.path.join(datarep,"snds_nomenclature.db"))
        self.datarep=datarep
        self.year=datetime.today().year
        
        #Load Dpt/Region map
        try:
            self.reg_dpt = pd.read_csv( os.path.join(self.datarep,"reg_dpt.csv"), sep=";" )
            self.reg_dpt.set_index("DPT_COD", inplace=True)
        except FileNotFoundError:
            warnings.warn("Error: missing data file")
            self.reg_dpt=None


class OpenEtablissementFactory(EtablissementFactory):
    """
    Class to generate the hospitalisation structure from the FINESS dataset (informations from real pharmacies repository)
    
    
    required file: finess-clean.csv
    
    Attributes
    ----------
    dpts: list of departements from which are sampled the hospitals
    
    """
    
    def __init__(self, con, dpts=[]):
        """
        Parameters
        ----------
        con : Context
        dpts : List of strings
            List of departement numbers in which the hospitals must be randomly choiced.
            The list must contains integers (dept numbers) or strings ("2A", "2B")
        """
        super().__init__(con)
        self.dpts=dpts
        
    
    def generate(self, n=0):
        """
        Parameters
        ----------
        n : int, optional
            Number of etablissement to generate. The default is 0.

        Returns
        -------
        A list of Etablissement instances
        """
        
        ## Finess dataset load
        dataset_file= os.path.join(self.context.datarep,"finess-clean.csv")
        data=pd.read_csv(dataset_file, sep=";")
        data["departement"]=data["departement"].astype('str')
        data["numvoie"].fillna(0, inplace=True)
        data["numvoie"]=data["numvoie"].astype(int)
        data["commune"].fillna(0, inplace=True)
        data["commune"]=data["commune"].astype(int)
        data["categetab"].fillna(0, inplace=True)
        data["categetab"]=data["categetab"].astype(int)
        
        ## selection of hopitaux
        etabldpt=data[ ((data['categagretab']==1101) | (data['categagretab']==1102) | (data['categagretab']==1106) | (data['categagretab']==1110) ) & (data["departement"].astype("str")==str(self.dpts[0])) ]
        for dpt in self.dpts[1:]:
            etabldpt = pd.concat( (data[ ((data['categagretab']==1101) | (data['categagretab']==1102) | (data['categagretab']==1106) | (data['categagretab']==1110) ) & (data["departement"].astype("str")==str(dpt)) ],etabldpt) )
        etabldpt = pd.concat( (etabldpt[['nofinesset', 'categetab', 'rs', 'numvoie', 'typvoie', 'voie', 'commune', 'departement', 'codemft', 'libmft', 'codesph', 'libsph']], etabldpt['ligneacheminement'].str.replace(pat=" CEDEX", repl="").str.split(n=1,expand=True)), axis=1 )
        etabldpt.rename(columns={0:"CP",1:"libcom"}, inplace=True)
        
        ## free useless memory
        del(data)
        
        ## random subsets
        if n>0:
            etabldpt=etabldpt.sample(n=n, random_state=1)
        
        ## create care providers from the database informations
        Etabs=[]
        for index, d in etabldpt.iterrows():
            e=Etablissement()
            e.id = d['nofinesset']
            e.dpt = d['departement']
            e.finess = d['nofinesset']
            e.rs=d['rs'] 
            e.code_commune=d['commune'] 
            e.nom_commune=d['libcom'] 
            e.cp=d['CP'] 
            e.cat=d['categetab'] #catégorie etablissement
            e.numvoie=d['numvoie'] 
            e.typvoie=d['typvoie'] 
            e.voie=d['voie'] 
            
            Etabs.append(e)        
        return Etabs
    
    
class OpenPharmacyFactory(PharmacyFactory):
    """
    Class to generate pharmacy from the finess dataset (informations from real pharmacies repository)
    
    required file: finess-clean.csv
    
    Attributes
    ----------
    dpts: list of departements from which are sampled the pharmacies
    
    """
    def __init__(self, con, dpts=[]):
        """
        Parameters
        ----------
        con : Context
        dpts : List of strings
            List of departement numbers in which the drugstores must be randomly choiced.
            The list must contains integers (dept numbers) or strings ("2A", "2B")
            The pharmacies are selected by the aggregated category number 
            (which gathers "Pharmacie d'Officine", "Propharmacie", 
             "Pharmacie Mutualiste", ...)
        """
        super().__init__(con)
        self.dpts=dpts

        
    def generate(self, n=0):
        """
        Generate n drugs stores from the FINESS database

        Parameters
        ----------
        n : Integer, optional
            Number of drugstores to generate (if 0, then generate all of them). The default is 0.

        Returns
        -------
        Pharmacies : List of PS object (that represents pharmacies in the database)
        """
        
        ## Finess dataset load
        dataset_file= os.path.join(self.context.datarep,"finess-clean.csv")
        data=pd.read_csv(dataset_file, sep=";")
        data["departement"]=data["departement"].astype('str')
        data["numvoie"].fillna(0, inplace=True)
        data["numvoie"]=data["numvoie"].astype(int)
        data["commune"].fillna(0, inplace=True)
        data["commune"]=data["commune"].astype(int)
        data["categetab"].fillna(0, inplace=True)
        data["categetab"]=data["categetab"].astype(int)
        
        ## selection of drugstores
        ddpt=data[ (data["departement"].astype("str")==str(self.dpts[0])) & (data['categagretab']==3201)]
        for dpt in self.dpts[1:]:
            ddpt = pd.concat( (data[ (data["departement"].astype("str")==str(dpt)) & (data['categagretab']==3201)],ddpt) )
        ddpt=pd.concat( (ddpt[['nofinesset','rs', 'numvoie', 'typvoie', 'voie', 'commune', 'departement','categetab']],ddpt['ligneacheminement'].str.split(n=1,expand=True)), axis=1 )
        ddpt=ddpt.rename(columns={0:"CP",1:"libcom"})
        
        ## free useless memory
        del(data)
        
        ## random subsets
        if n>0:
            ddpt=ddpt.sample(n=n, random_state=1)
        
        ## create care providers from the database informations
        Pharmacies=[]
        for index, d in ddpt.iterrows():
            p=Provider()
            p.dpt = d['departement']
            p.code_commune = d['commune']
            p.cat_nat=50 #pharmacie de ville
            p.id = p.dpt+"2{:05}".format(rd.randint(99999)) #PFS_PFS_NUM: c'est le numéro du cabinet du praticien à 8 chiffres (2 chiffre dpt+3eme comme categorie professionnelle) ! (numéro PS officiel à 11 chiffres ... pas encore en place)
            p.finess = d['nofinesset']
            
            Pharmacies.append(p)
        return Pharmacies


class OpenPhysicianFactory(PhysicianFactory):
    """
    Generation of a collection of private practice doctors (doctors not working
                                                            in hospitals)
    
    We distinguish two types of doctors
        -> general practicioners (GP)
        -> specialist practitioners
        
    required file: medecins.csv
    This file is generated from the public list of physicians (not simulated).
    
    Attributes
    ----------
    dpts : List of strings
        List of departement numbers in which the physicians must be randomly choiced.
        The list must contains integers (dept numbers) or strings ("2A", "2B")
    """
    
    #definition d'une table de correspondance entre les codes professionnelles (du jeu des données de PS) et du codage des spécialités dans le SNDS (table IR_SPA_COD)
    # si pas d'entrée dans cette map, alors mettre la valeur '0' (non défini)

    catprof_SPACOD={45:1,3:2,6:3,7:4,22:5,67:6,37:7,33:8,72:9,52:10,59:11,60:12,64:13,70:14,56:15,15:16,54:17,74:18,71:21,46:22,47:23,39:24,43:26,61:27,57:28,58:29,40:30,73:31,53:32,65:33,34:34,51:35,2:37,41:39,42:40,12:41,23:42,8:43,9:44,10:45,13:46,14:47,16:48,17:49,62:50,63:52,18:53,19:54,1:55,24:60,25:61,26:62,27:63,28:64,29:65,30:66,31:67,32:68,11:69,35:70,38:71,49:72,5:73,4:74,66:75,68:76,55:77,48:78,36:79,50:80,69:83}

    def __init__(self,con,dpts):
        """
        Parameters
        ----------
        con : Simulation context
        """
        super().__init__(con)
        self.dpts=dpts
    
    def generate(self, n=0):
        dataset_file= os.path.join(self.context.datarep,"medecins.csv")
        medecins = pd.read_csv(dataset_file)
        
        medecins_dpt=medecins[ (medecins['CP']//1000==int(self.dpts[0])) ]
        for dpt in self.dpts[1:]:
            medecins_dpt = pd.concat( (medecins_dpt, medecins[(medecins['CP']//1000==int(dpt))] ) )
        if n>0:
            medecins_dpt=medecins_dpt.sample(n=n, random_state=1)
        
        physicians=[]
        for index, ps in medecins_dpt.iterrows():
            if ps["Profession"]<=47 and ps["Profession"]>=45:
                p=GP()
                p.dpt = "%02d"%(ps['CP']//1000)
                p.id = super().__generatePSNUM__(p)
                p.sex = ps["Sexe"]
                p.CP=ps['CP']
                p.code_commune=ps["Code_commune_INSEE"]
                p.finess=""
                p.nom_commune=ps['Ville']
                
                physicians.append(p)
            else: #specialists
                p=Specialist()
                p.dpt = "%02d"%(ps['CP']//1000)
                p.id = super().__generatePSNUM__(p)
                p.sex = ps["Sexe"]
                p.CP=ps['CP']
                p.code_commune=ps["Code_commune_INSEE"]
                p.finess=""
                p.nom_commune=ps['Ville']
                try:
                    p.speciality= OpenPhysicianFactory.catprof_SPACOD[ ps["Profession"] ]
                except KeyError:
                    print("unknown specialty: "+str(ps["Profession"]))
                    p.speciality= 0 #unknown
                
                physicians.append(p)
            
        return physicians


class OpenPatientFactory(PatientFactory):
    """
    Generate a population of Patients from the real popution statistics of France
        -> per city, age (bins of 5 years) and sex
    
    It also:
        - assigns a GP (médecin traitant) from the list of available GPs in the simulation. The GP is assigned such that she/he is in the same town, or at least in the same dpt
        - assigns a list of ALDs (according to statistics in real poputlation)
        
    required files
        - pop.csv: population statistiques
        - ALD_p.csv: probability to have a chronic long-term illness (ALD in french)
        
    
    This generator simulate a population at a given time.
        
    Attributes
    ----------
    dpts: List of departement numbers
    GPs: List of general practitioners
    pop: population statistics
    tot_pop: total number of patients
    Pald: proba of having ALD
    """

    def __init__(self, con, GPs=None, dpts=[]):
        super().__init__(con, GPs)
        self.dpts=dpts
        
        dataset_file= os.path.join(self.context.datarep,"pop.csv")
        pop=pd.read_csv(dataset_file)
        
        #selection des données uniquement pour les départements d'intérêt
        dpts=["%02d"%d for d in dpts]
        self.pop = pop[ pop['dpt'].isin(dpts) ]
        self.tot_pop = np.sum(self.pop['pop'])
        #pop contient des informations sur la population par sex, par age (tranches de 5 ans), par communes
        # colonnes 'age', 'sex', 'dpt', 'Ville', 'pop', 'Code'
        
        ## Get statistics about ALD per sex/age/dpt
        dataset_file= os.path.join(self.context.datarep,"ALD_p.csv")
        self.Pald=pd.read_csv(dataset_file)
        #Pald gives the conditional probabilities of having ALD knowing the dpt, sex and age
        
        
    def generate(self, n=0):
        patients=[]
        
        for index, ps in self.pop.iterrows():
            if n==0:
                nb=ps['pop']
            else:
                if np.isnan(ps['pop']):
                    nb=0
                else:
                    nb=int(ps['pop']*n/self.tot_pop)
            for i in range( nb ):
                p=Patient()
                p.Sex=ps['sex']
                age = rd.randint(ps['age'],ps['age']+5)
                #random generation of a birthdate corresponding to the patient age
                p.BD=self.context.generate_date(date(self.context.year-age,1,1), date(self.context.year-age,12,31))
                p.Dpt = ps['dpt']
                p.City = ps['code']
                self.__generateNIR__(p)
                
                if self.GPs:
                    #selection des médecins de la même ville
                    gps=[gp for gp in self.GPs if gp.code_commune==p.City]
                    if len(gps)==0:
                        #si il n'y en a pas, même département
                        gps=[gp for gp in self.GPs if gp.dpt==p.Dpt]
                    if len(gps)==0:
                        #sinon, on les prends tous
                        gps=self.GPs
                    #et on tire au hasard au milieu de ces médecins
                    mtt=rd.choice(gps)
                    p.MTT=mtt
                    
                #generate a list of ALDs 
                pALD=self.Pald[(self.Pald['dpt']==ps['dpt']) & (self.Pald['age']==ps['age'])& (self.Pald['sex']==ps['sex'])][["ALD",'p']]
                p.ALD=list(pALD[pALD['p']>=rd.rand(len(pALD))]['ALD'])
            
                patients.append( p )
    
        return patients


class OpenDrugsDeliveryFactory(DrugsDeliveryFactory):
    '''
    The OpenDrugsDeliveryFactory is based on the OpenMedic dataset to generate 
    drugs deliveries for patients.
    
    This class requires two csv files to feed the main dataframes
    
    required files:
        - drugs_freq.csv
        - mean_deliveries.csv

    Attributes
    ----------
    drug_freq: Pandas Dataframe, probabilities to deliver each CIP13 code per
        age, sex and region (during a year)
        
    mean_deliveries: Pandas dataframe, mean numbers of deliveries 
        per type of personnes (during a year)
    '''
    def __init__(self, con, Pharmacies):
        """
        Parameters
        ----------
        con : Factory context
            Include a connexion to the nomenclature database
        Pharmacies : List of Pharmacies
            Represent possible care delivers of drugs.
        """
            
        super().__init__(con, Pharmacies)
        try:
            self.drug_freq=pd.read_csv( os.path.join(self.context.datarep,"drugs_freq.csv") )
            self.mean_deliveries=pd.read_csv( os.path.join(self.context.datarep,"mean_deliveries.csv") )
        except FileNotFoundError:
            print("Error: missing data file")
            self.drug_freq=None
            self.mean_deliveries=None
        
    def generate(self, p):
        """
        Generate drug deliveries for one patient `p`
        The deliveries are added to the patient itself.
        
        A patient always goes to take drugs in the same pharmacy.
        The pharmacy is one of its location if any (otherwise in its departement).
        
        The number of drugs deliveries is drawn by a (rounded) exponential law with a lambda given
        by the mean deliveries expectation.
        The drugs delivered are randomly choiced using a weighted probability for patients to have
        this drug delivered (not conditionned to anything)
        
        Parameters
        ----------
        p: Patient
            A patient to which generate a drug delivery
        """
        
        #compute the patient age
        age= self.context.year - p.BD.year
        
        ## Déterminer la région à partir du dpt
        region=self.context.reg_dpt.loc[p.Dpt]['REG_COD']
        
        sex=int(p.Sex)
        if sex==9:
            sex=rd.choice([1,2])

        #we first determine the number of deliveries within a year
        if age<20:
            mean=self.mean_deliveries[ (self.mean_deliveries['RR']==region) & (self.mean_deliveries['age']==0) & (self.mean_deliveries['sex']==sex)]['mean']
        elif age<60:
            mean=self.mean_deliveries[ (self.mean_deliveries['RR']==region) & (self.mean_deliveries['age']==20) & (self.mean_deliveries['sex']==sex)]['mean']
        elif age<95:
            mean=self.mean_deliveries[ (self.mean_deliveries['RR']==region) & (self.mean_deliveries['age']==60) & (self.mean_deliveries['sex']==sex)]['mean']
        else:
            mean=self.mean_deliveries[ (self.mean_deliveries['RR']==region) & (self.mean_deliveries['age']==95) & (self.mean_deliveries['sex']==sex)]['mean']
        nb = int(np.round(rd.exponential(mean)))
        
        # select the drugs from the OpenMedic dataset
        if age<20:
            drugs = self.drug_freq[(self.drug_freq['age']==0) & (self.drug_freq['sex']==sex) & (self.drug_freq['RR']==region)]
        elif age<60:
            drugs = self.drug_freq[(self.drug_freq['age']==20) & (self.drug_freq['sex']==sex) & (self.drug_freq['RR']==region)]
        elif age<90:
            drugs = self.drug_freq[(self.drug_freq['age']==60) & (self.drug_freq['sex']==sex) & (self.drug_freq['RR']==region)]
        else:
            drugs = self.drug_freq[(self.drug_freq['age']==99) & (self.drug_freq['sex']==sex) & (self.drug_freq['RR']==region)]

        #generate a collection of nb drug deliveries
        drugs=rd.choice(np.array(drugs['CIP13']), nb, p=np.array(np.array(drugs['p'])), replace=True)
        if len(drugs)==0:
            p.drugdeliveries=[]
            return
        
        #selection des pharmacies de la même ville, où a défault dans le même département
        pharms=[ph for ph in self.Pharmacies if ph.code_commune==p.City]
        if len(pharms)==0:
            pharms=[ph for ph in self.Pharmacies if ph.dpt==p.Dpt]
        if len(pharms)==0:
            #sinon, on les prends tous
            pharms=self.Pharmacies
            
        #et on tire au hasard au milieu de ces pharmacies
        pharm=rd.choice( pharms )
        for drug in drugs:
            dd=DrugDelivery(drug, p, pharm)
            #delivered within the context simulation year
            dd.date_debut=self.context.generate_date(begin = date(self.context.year,1,1), end=date(self.context.year,12,31))
            dd.date_fin=dd.date_debut
            p.drugdeliveries.append(dd)
            


class OpenShortStayFactory(ShortStayFactory):
    """
    Factory of short hospital stays (séjours MCO) based on PMSI statistics.
    
    Requires files:
        - p_host.csv
        - p_sej.csv
        - cim_stats.json
        
    Attributes
    ----------
    hospitals: list of hospital
    
    p_hosp: proba of behing hospitalized in the year
    p_sej: number of hospitalisation per years 
    cims_stats: statistics of diagnosis codes (primary diagnosis, related and associate diagnosis and CCAM codes)
    
    cims: (internal) list of primary diagnosis
    cim_id: (internal) 
    
    """
    def __init__(self, con, hospitals):
        """
        Parameters
        ----------
        con : Context
            Simulation context.
        hospitals : List
            List of hopitals.
        """
        super().__init__(con,hospitals)
        self.hospitals = hospitals
        
        #load statistics details about hospital stays
        try:
            self.p_hosp=pd.read_csv( os.path.join(self.context.datarep,"p_host.csv") )
            self.p_sej=pd.read_csv( os.path.join(self.context.datarep,"p_sej.csv") )
        except FileNotFoundError:
            warnings.warn("Error: missing data file")
            return
        self.p_sej.set_index("dpt",inplace=True)
        
        try:
            f=open( os.path.join(self.context.datarep,"cim_stats.json") )
            self.cims_stats=json.load(f)
        except FileNotFoundError:
            warnings.warn("Error: missing data file")
            self.p_hosp=None
            self.p_sej=None
        
        counts={i:[s['count']] for i,s in self.cims_stats.items()}
        counts=pd.DataFrame.from_dict(counts, orient="index")
        counts['p']=counts[0]/np.sum(counts[0])
        self.cims=list(counts.reset_index().sample(n=10000, replace=True, weights='p', random_state=1)['index'])
        self.cim_id=0
    
    def __generate_one__(self, p,age,sex,dpt):
        """
        Generate the details of a short hopital stay
        
        TODO add the related diagnosis

        Parameters
        ----------
        p : Patient
        """
        
        #choice of the cim code at position cims_id in the list of random cims
        DP = self.cims[self.cim_id]
        #next cim id (circular)
        self.cim_id = (self.cim_id+1)%len(self.cims)
        
        #random choice of hospital
        e = rd.choice(self.hospitals)
        
        #create the stay to append to the patient history
        stay = ShortHospStay(p, e, DP )
        
        duration = rd.poisson( float(self.p_hosp[(self.p_hosp['dpt'].astype(str)==str(dpt)) & (self.p_hosp['age']==age) & (self.p_hosp['sex']==sex) & (self.p_hosp['type']==0) ]['DMS'].str.replace(",",".")) )
        
        stay.start_date = self.context.generate_date(begin = date(self.context.year,1,1), end=date(self.context.year,12,31)-timedelta(days=duration))
        stay.finish_date = stay.start_date+timedelta(days=duration)
        
        #generate 4 associated diagnosis 
        counts=pd.DataFrame.from_dict( dict(self.cims_stats[DP]["cim"]), orient='index' )
        counts[0]=counts[0]/np.sum(counts[0])
        stay.cim_das=list(counts.sample(n=4,weights=counts[0]).reset_index()['index'])
        
        ##generate a GHM code
        counts=pd.DataFrame.from_dict( dict(self.cims_stats[DP]["ghm"]), orient='index' )
        counts[0]=counts[0]/np.sum(counts[0])
        stay.GHM=str(counts.sample(n=1,weights=counts[0]).reset_index()['index'])
        
        #generates up to 4 medical acts (CCAM codes)
        nb_acts=rd.randint(5)
        counts=pd.DataFrame.from_dict( dict(self.cims_stats[DP]["ccam"]), orient='index' )
        counts[0]=counts[0]/np.sum(counts[0])
        stay.ccam=list(counts.sample(n=nb_acts,weights=counts[0]).reset_index()['index'])
        
        p.hospitalStays.append(stay)
        
        
    def generate(self, p):
        """
        Generate the hospital stays for a patient p
        It determines the number of hopital stays it is likely to occur for that patient 
        considering its location (dpt), sex and age

        Parameters
        ----------
        p : Patient
        """
        dpt=str(p.Dpt)
        
        age= self.context.year - p.BD.year
        age= age-age%5 #
        if age>95:
            age=95
        sex=int(p.Sex) #1,2 ... pour le 9, on tire aléatoirement
        if sex==9:
            sex=rd.randint(2)+1
        
        #probabilité d'être hospitalisé au moins une fois
        phosp=np.sum( self.p_hosp[(self.p_hosp['dpt'].astype(str)==str(dpt)) & (self.p_hosp['age']==age) & (self.p_hosp['sex']==sex) ]['p'] )
        
        #nombre d'hospitalisations ensuite déterminé par une loi de Poisson
        nbhosp=(1+rd.poisson(self.p_sej.loc[int(dpt)]['nbsejours']-1,1)) * ( int(rd.rand(1)<float(phosp)) )
        nbhosp=nbhosp[0]
        for i in range(nbhosp):
            self.__generate_one__(p,age,sex,dpt)



class OpenVisitFactoryDpt(VisitFactory):
    """
    A visit Factory that is based on the DAMIR R files, representative of dpt
    
    required files:
        - nb_prs_dptspe.csv
        - p_prsnat_dptspe.csv
    """
    
    def __init__(self, con, physicians):
        super().__init__(con, physicians)
        
        #Load the statistics computed from Open Data
        self.nb_prs_spedpt =pd.read_csv( os.path.join(self.context.datarep, "nb_prs_dptspe.csv") )
        self.nb_prs_spedpt.set_index(['dpt','exe_spe'],inplace=True)
        self.p_nat_spedpt =pd.read_csv( os.path.join(self.context.datarep, "p_prsnat_dptspe.csv") )
        self.p_nat_spedpt.set_index(['dpt','exe_spe'],inplace=True)
        
        
    def generate(self, p):
        """

        Parameters
        ----------
        p : Patient
        """
        #compute the list of specialties that are actually in the set of physicians !!
        spes = list(set([ p.speciality for p in self.physicians]))
        dpt  = p.Dpt
        
        for spe in spes:
            #random number of visits for a specialist
            try:
                nb=int(np.floor(rd.exponential(self.nb_prs_spedpt.loc[dpt,spe]['nb'])))
            except KeyError:
                warnings.warn("Unknown prs for spe/dpt:",spe,"/",dpt)
                continue
            except TypeError:
                warnings.warn("Unknown prs for spe/dpt:",spe,"/",dpt)
                continue
            
            if spe==1:
                #Generalist consultation => Medecin Traitant
                for i in range(nb):
                    visit = MedicalVisit(p,p.MTT)
                    visit.code_pres = self.p_nat_spedpt.loc[dpt,spe].sample(1,weights='p')['prs_nat'].iloc[0]
                    visit.date_debut = self.context.generate_date(begin = date(self.context.year,1,1), end=date(self.context.year,12,31))
                    visit.date_fin = visit.date_debut
                    p.visits.append( visit )
            else:
                # always the same specialist for a patient
                physis=[ p for p in self.physicians if p.speciality==spe]
                med = rd.choice(physis) 
                for i in range(nb):
                    visit = MedicalVisit(p, med)
                    visit.code_pres = self.p_nat_spedpt.loc[dpt,spe].sample(1,weights='p')['prs_nat'].iloc[0]
                    visit.date_debut = self.context.generate_date(begin = date(self.context.year,1,1), end=date(self.context.year,12,31))
                    visit.date_fin = visit.date_debut
                    p.visits.append( visit )
                
            

class OpenVisitFactory(VisitFactory):
    """
    A visit factory based on the A files of DAMIR, data based representative of age and sex, but at the regional level
    
    Required files:
        - nb_prs_rragesex.csv : numbers of visit per age, sex and region
        - p_prsnat_rragesex.csv : proba of each type of visit per age, sex and region
    """
    
    def __init__(self, con, physicians):
        super().__init__(con, physicians)
        
        #Load the statistics computed from Open Data
        self.nb_prs =pd.read_csv( os.path.join(self.context.datarep, "nb_prs_rragesex.csv") )
        self.nb_prs.set_index(['RR','age','sex','exe_spe'],inplace=True)
        self.p_nat =pd.read_csv( os.path.join(self.context.datarep, "p_prsnat_rragesex.csv") )
        self.p_nat.set_index(['RR','age','sex','exe_spe'],inplace=True)
        
        
    def generate(self, p):
        """
        Parameters
        ----------
        p : Patient
        """
        #compute the list of specialties that are actually in the set of physicians !!
        spes = list(set([ p.speciality for p in self.physicians]))
        
        #get the location region of the patient
        RR=self.context.reg_dpt.loc[p.Dpt]['REG_COD']
        
        age= self.context.year - p.BD.year
        age= age-age%10 # arrondis à la dixaine
        #2 exceptions: pas de valeurs 10, pas de détails >80
        if age==10:
            age=0
        if age>80:
            age=80
            
        sex=int(p.Sex) #1,2 ... pour le 9, on tire aléatoirement
        if sex==9:
            sex=rd.randint(2)+1
        
        for spe in spes:
            #random number of visits for a specialist
            try:
                nb=int(np.floor(rd.exponential(self.nb_prs.loc[RR,age,sex,spe]['nb'])))
            except KeyError:
                print("Unknown prs for spe/RR/age/sex:",spe,"/",RR,"/",age,"/",sex)
                continue
            except TypeError:
                print("Unknown prs for spe/RR/age/sex:",spe,"/",RR,"/",age,"/",sex)
                continue
            
            if spe==1:
                #Generalist consultation => Medecin Traitant
                med = p.MTT
            else:
                # always the same specialist for a patient
                physis=[ p for p in self.physicians if p.speciality==spe]
                med = rd.choice(physis) 
                
            for i in range(nb):
                visit = MedicalVisit(p, med)
                visit.code_pres = self.p_nat.loc[RR,age,sex,spe].sample(1,weights='p')['prs_nat'].iloc[0]
                visit.date_debut = self.context.generate_date(begin = date(self.context.year,1,1), end=date(self.context.year,12,31))
                visit.date_fin = visit.date_debut
                p.visits.append( visit )


class OpenActFactory(ActFactory):
    """
    Required files:
        - nb_acts_rragesex.csv: number of acts per region, age and sex (during a year)
        - p_act_prs.csv: proba of each acts (CCAM code)
        - p_exespe_acts.csv: proba of the speciality of the physician for each act
    """
    def __init__(self, con, physicians):
        super().__init__(con, physicians)
        
        
        #Load the statistics computed from Open Data
        self.nb_acts =pd.read_csv( os.path.join(self.context.datarep, "nb_acts_rragesex.csv") )
        self.nb_acts.set_index(['RR','age','sex'],inplace=True)
        self.nb_exespe =pd.read_csv( os.path.join(self.context.datarep, "p_exespe_acts.csv") )
        self.nb_exespe.set_index(['RR','age','sex','prs_nat'],inplace=True)
        self.p_ccam =pd.read_csv( os.path.join(self.context.datarep, "p_act_prs.csv") )
        self.p_ccam.set_index(['prs_nat'],inplace=True)
    
    def generate_one(self, p):
        """
        - p patient
        """
        ccam = rd.choice(self.ccams)
        spe = rd.choice(self.spes)
        mact= MedicalAct(ccam, p,spe)
        mact.date_debut=self.context.generate_date(begin = date(self.context.year,1,1), end=date(self.context.year,12,31))
        mact.date_fin=mact.date_debut
        
        p.medicalacts.append(mact)

    def generate(self, p):
        """
        Parameters
        ----------
        p : Patient
        """
        
        #get the location region of the patient
        RR=self.context.reg_dpt.loc[p.Dpt]['REG_COD']
        
        age= self.context.year - p.BD.year #relativedelta(date.today(), p.BD).years
        age= age-age%10 # arrondis à la dixaine
        #2 exceptions: pas de valeurs 10, pas de détails >80
        if age==10:
            age=0
        if age>80:
            age=80
            
        sex=int(p.Sex) #1,2 ... pour le 9, on tire aléatoirement
        if sex==9:
            sex=rd.randint(2)+1
        
        nb_prs=self.nb_acts.loc[RR,age,sex]
        for index, row in nb_prs.iterrows():
            prs_nat = row['prs_nat']
            if prs_nat==1352: ##pas de CCAM associé (?) TODO: vérifier la génération de données
                continue
            
            #generate a random number of cares of type 'prs_nat'
            nb = int(np.floor(rd.exponential(row['nb'])))
            for i in range(nb):
                # generate a random CCAM code for this care
                ccam = str(self.p_ccam.loc[prs_nat].sample(n=1,weights="p")["ccam"])
                
                #find a physician ...
                
                #he/she must have the right specialty
                spes=[]
                it=0
                while len(spes)==0 and it<10:
                    # generate a specialty for this care
                    spe = int(self.nb_exespe.loc[RR,age,sex,prs_nat].sample(n=1,weights="p")["exe_spe"])
                    
                    spes=[ ps for ps in self.physicians if ps.speciality==spe]
                    it+=1
                if len(spes)==0:
                    print("no specialist to deliver act "+str(ccam)+": you will die!!")
                    continue
                
                #and prefer physician in the same Dpt
                spes_loc=[ ps for ps in spes if str(ps.dpt)==str(p.Dpt)]
                if len(spes_loc)==0:
                    spes_loc=spes
                
                #create the care delivery and add it to the patient history
                mact= MedicalAct(ccam, p, rd.choice(spes_loc))
                mact.date_debut=self.context.generate_date(begin = date(self.context.year,1,1), end=date(self.context.year,12,31))
                mact.date_fin=mact.date_debut
                p.medicalacts.append(mact)

if __name__ == "__main__":
    
    context = OpenDataFactoryContext(datarep="/home/tguyet/Progs/medtrajectory_datagen/datarep")
    #context = OpenDataFactoryContext(datarep="/home/medtrajectory_datagen/data")
    ## Liberal physicians
    factory = OpenPhysicianFactory(context, [22,35])
    physicians= factory.generate(100)
    for p in physicians:
        print(p)
    
    #General practicioners
    GPs =  [ p for p in physicians if p.speciality==1 ]
    
    factory = OpenPatientFactory(context, GPs, [22,35])
    patients= factory.generate(1000)
    for p in patients:
        print(p)
        
    
    factory=OpenActFactory(context,physicians)
    for p in patients:
        factory.generate(p)
        print(p.medicalacts)
    
    factory=OpenVisitFactory(context,physicians)
    for p in patients:
        factory.generate(p)
        print(p.visits)
    
    
    factory = OpenEtablissementFactory(context, [22,35])
    etab= factory.generate()
    for p in etab:
        print(p)
    
    factory=OpenShortStayFactory(context,etab)
    for p in patients:
        factory.generate(p)
        print(p.hospitalStays)
    
    factory = OpenPharmacyFactory(context, [35])
    ps= factory.generate(100)
    for p in ps:
        print(p)
    
    factory=OpenDrugsDeliveryFactory(context,ps)
    for p in patients:
        factory.generate(p)
        print(p.drugdeliveries)
    
