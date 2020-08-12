# -*- coding: utf-8 -*-


from Generator.data_factory import FactoryContext, PharmacyFactory, EtablissementFactory, PhysicianFactory, PatientFactory
from Generator.database_model import Provider, Etablissement, GP, Specialist, Patient
#import os
import numpy as np
import pandas as pd
import numpy.random as rd
from datetime import datetime, date



class FinessEtablissementFactory(EtablissementFactory):
    """
    Class to generate the hospitalisation structure from the FINESS dataset
    """

    dataset_file="./data/finess-clean.csv"
    
    def __init__(self, con, dpts=[]):
        """
        Parameters
        ----------
        con : Context
        dpts : List of strings
            List of departement numbers in which the hospitals must be randomly choiced.
            The list must contains integers (dept numbers) or strings ("2A", "2B")
            
        Returns
        -------
        None.

        """
        super().__init__(con)
        self.dpts=dpts
        
    
    def generate(self, n=0):
        """
        

        Parameters
        ----------
        n : TYPE, optional
            DESCRIPTION. The default is 0.

        Returns
        -------
        Etablissement : TYPE
            DESCRIPTION.

        """
        
        ## Finess dataset load
        data=pd.read_csv(FinessEtablissementFactory.dataset_file, sep=";")
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
    
    
class FinessPharmacyFactory(PharmacyFactory):
    """
    Class to generate the pharmacies from the finess dataset
    """
    
    dataset_file="./data/finess-clean.csv"
    
    def __init__(self, con, dpts=[]):
        """
        Parameters
        ----------
        con : Context
        dpts : List of strings
            List of departement numbers in which the drugstores must be randomly choiced.
            The list must contains integers (dept numbers) or strings ("2A", "2B")
            The drugstores are selected by the aggregated category number (which gathers "Pharmacie d'Officine", "Propharmacie", "Pharmacie Mutualiste", ...)
            
        Returns
        -------
        None.

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
        data=pd.read_csv(FinessPharmacyFactory.dataset_file, sep=";")
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
            p.cat_nat=50 #pharmacie de ville
            p.id = p.dpt+"2{:05}".format(rd.randint(99999)) #PFS_PFS_NUM: c'est le numéro du cabinet du praticien à 8 chiffres (2 chiffre dpt+3eme comme categorie professionnelle) ! (numéro PS officiel à 11 chiffres ... pas encore en place)
            p.finess = d['nofinesset']
            
            Pharmacies.append(p)
        return Pharmacies


class OpenPhysicianFactory(PhysicianFactory):
    """
    Génération d'une base de médecins de "ville" (exercants en libéral), avec
        -> des médecins généralistes
        -> des médecins spécialistes
    """
    
    dataset_file="./data/ps-infospratiques.csv"
    #definition d'une table de correspondance entre les codes professionnelles (du jeu des données de PS) et du codage des spécialités dans le SNDS (table IR_SPA_COD)
    # si pas d'entrée dans cette map, alors mettre la valeur '0' (non défini)

    catprof_SPACOD={45:1,3:2,6:3,7:4,22:5,67:6,37:7,33:8,72:9,52:10,59:11,60:12,64:13,70:14,56:15,15:16,54:17,74:18,18:19,69:20,71:21,46:22,47:23,39:24,43:26,61:27,57:28,58:29,40:30,73:31,53:32,65:33,34:34,51:35,19:36,2:37,41:39,42:40,12:41,23:42,8:43,9:44,10:45,13:46,14:47,16:48,17:49,62:50,62:51,63:52,18:53,19:54,1:55,24:60,25:61,26:62,27:63,28:64,29:65,30:66,31:67,32:68,11:69,35:70,38:71,49:72,5:73,4:74,66:75,68:76,55:77,48:78,36:79,50:80,69:83}

    def __init__(self,con,dpts):
        """
        Parameters
        ----------
        con : Simulation context

        Returns
        -------
        None.

        """
        super().__init__(con)
        self.dpts=dpts
    
    def generate(self, n=0):
    
        data = pd.read_csv(OpenPhysicianFactory.dataset_file, sep=";", header=None, encoding="latin_1")
        data.rename(columns={
                        0:"Sexe", 
                        1:"nom", 
                        2:"prenom", 
                        3:"Adresse ligne 1,", 
                        4:"Adresse ligne 2",
                        5:"Adresse ligne 3",
                        6:"Adresse ligne 4",
                        7:"CP", ## un numéro qui sembl être un CP (mais des soucis avec les 2 ... (?))
                        8:"Ville",
                        9:"Téléphone", 
                        10:"Profession", #code de profession (voir nommenclature ci-dessous)
                        11:"mode", #mode d'exercice
                        12:"nature", #nature d'exercice
                        13:"convention", 
                        14:"option contrat", 
                        15:"sesam_vital", 
                        16:"Type", # Liberal/hors activité, etc.
                        17:"Type consultation", 
                        18:"heure_debut", 
                        19:"heure_debut",
                        20:"jour"}, inplace=True)
        # Médecins libéraux exercant en ville dans le département 35
        datadpt=data[(data['CP']//1000==int(self.dpts[0])) & (data['Type']!=0) & (data['Type']!=6) & (data['Type']!=7) & (data['Type']!=8) ]
        
        for dpt in self.dpts[1:]:
            datadpt = pd.concat( (datadpt, data[(data['CP']//1000==int(dpt)) & (data['Type']!=0) & (data['Type']!=6) & (data['Type']!=7) & (data['Type']!=8) ]) )
            
        # codage du sex selon la IR_SEX_COD
        datadpt["Sexe"].fillna("9",inplace=True)
        datadpt["Sexe"]=datadpt["Sexe"].str.replace(pat="F",repl="2")
        datadpt["Sexe"]=datadpt["Sexe"].str.replace(pat="H",repl="1")
        # suppression des CEDEX
        datadpt["Ville"]=datadpt["Ville"].str.replace(pat=" CEDEX.*",repl="", regex=True)
        
        datadpt=datadpt[["Sexe","nom","prenom","CP","Ville","Profession"]].drop_duplicates()
        # on supprime les "non-médecins" (parce qu'ils ne correspondent pas à cette classe des spécialistes)
        datadpt=datadpt[ datadpt["Profession"]!=1 ] # suppression dans ambulanciers
        datadpt=datadpt[ datadpt["Profession"]!=2 ] # suppression dans anathamo-patho
        datadpt=datadpt[ (datadpt["Profession"]<18) | (datadpt["Profession"]>21) ] # suppression des dentistes
        datadpt=datadpt[ (datadpt["Profession"]<24) | (datadpt["Profession"]>32) ] # suppression des fournisseurs de matériel
        datadpt=datadpt[ (datadpt["Profession"]!=39) ] # suppression des infirmiers
        datadpt=datadpt[ (datadpt["Profession"]<40) | (datadpt["Profession"]>42)] # suppression des laboratoires
        datadpt=datadpt[ (datadpt["Profession"]!=43) ] # suppression des kinés
        datadpt=datadpt[ (datadpt["Profession"]!=57) ] # suppression des orthophonistes
        datadpt=datadpt[ (datadpt["Profession"]!=58) ] # suppression des orthoptiste
        datadpt=datadpt[ (datadpt["Profession"]!=58) ] # suppression des pédicures-podologues
        datadpt=datadpt[ (datadpt["Profession"]<62) | (datadpt["Profession"]>63)] # suppression des pharmaciens
        datadpt=datadpt[ (datadpt["Profession"]!=71) ] # suppression des sages-femmes
        
        if n>0:
            datadpt=datadpt.sample(n=n, random_state=1)
        
        physicians=[]
        for index, ps in datadpt.iterrows():
            if ps["Profession"]<=47 and ps["Profession"]>=45:
                p=GP()
                p.dpt = "%02d"%(ps['CP']//1000)
                p.id = super().__generatePSNUM__(p)
                p.sex = ps["Sexe"]
                p.CP=ps['CP']
                p.code_commune="%03d"%(ps['CP']%1000) #code issue du CP
                p.finess=""
                p.nom_commune=ps['Ville']
                
                physicians.append(p)
            else: #specialists
                p=Specialist()
                p.dpt = ps['CP']//1000
                p.id = super().__generatePSNUM__(p)
                p.sex = ps["Sexe"]
                p.CP=ps['CP']
                p.code_commune="" #TODO ??
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
    """

    def __init__(self, con, GPs=None, dpts=[]):
        super().__init__(con, GPs)
        self.dpts=dpts
        
        pop_saq = pd.read_csv("./data/pop-sexe-age-quinquennal.zip", header=0, sep=',', encoding="latin_1", dtype={'DR':str,'DR18':str })
        pop_saq.dropna(inplace=True) #some cities disappeared leading to none lines
        try:
            #remove some a priori useless columns
            del(pop_saq['RR'])
            del(pop_saq['CR'])
            del(pop_saq['DR'])
            del(pop_saq['STABLE'])
        except:
            pass
        
        #Gather all the columns in a unique column with variables to describe them ('sex' and 'age')
        pop_saq=pop_saq.melt(id_vars=['DR18','LIBELLE'])
        tmp=pop_saq['variable'].str.extract(r'ageq_rec(?P<age>\d+)s(?P<sex>\d)rpop2016')
        pop_saq=pd.concat([tmp,pop_saq],axis=1)
        pop_saq['age']=(pd.to_numeric(pop_saq['age'])-1)*5 #counts for 5 years intervals of ages
        del(tmp)
        del(pop_saq['variable'])
        
        #rename columns
        pop_saq=pop_saq.rename(columns={'LIBELLE':'Ville', 'DR18':'dpt'})
        pop_saq['Ville']=pop_saq['Ville'].str.lower()
        
        pop_cities = pd.read_csv("./data/population.zip", header=0, sep=';', encoding="utf-8")
        pop_cities['dpt'] = pop_cities['Code'].str[:-3].str.pad(width=2, fillchar='0')
        
        pop_cities['Ville']=pop_cities['Ville'].str.lower()
        # on arrange déjà en enlevant certains "l'" en début
        pop_cities['Ville']=pop_cities['Ville'].str.replace(pat="^l'",repl="")
        pop_cities['Ville']=pop_cities['Ville'].str.replace(pat="^la ",repl="")
        pop_cities['Ville']=pop_cities['Ville'].str.replace(pat="^le ",repl="")
        pop_cities['Ville']=pop_cities['Ville'].str.replace(pat="^les ",repl="")
        pop_cities['Ville']=pop_cities['Ville'].str.replace(pat="œ",repl="?")
        
        ## le merge fait perdre environ 775 communes (dont des grosses) ... mais aucune en bretagne
        pop=pd.merge(pop_saq,pop_cities,how='inner', on=('dpt','Ville'))
        
        #selection des données uniquement pour les départements d'intérêt
        dpts=["%02d"%d for d in dpts]
        self.pop=pop[ pop['dpt'].isin(dpts)]
        self.tot_pop = np.sum(self.pop['value'])
        #pop contient des informations sur la population par sex, par age (tranches de 5 ans), par communes
        # colonnes 'age', 'sex', 'dpt', 'Ville', 'value', 'Code'
        #print(self.tot_pop)
        
        del(pop_cities)
        
        ## Gather statistics about ALD
        # population per Age, sex and dpt
        pop_ASD=pop_saq.groupby(["age","sex",'dpt']).agg({"value":"sum"})
        pop_ASD= pop_ASD.reset_index() #transform the group object into a dataframe
        pop_ASD.columns = pop_ASD.columns.get_level_values(0)
        pop_ASD['value']=pop_ASD['value']/self.tot_pop
        pop_ASD['sex']=pop_ASD['sex'].astype("int64")
        
        del(pop_saq)
        
        #counts of ALD per departement
        ald_per_dpt=pd.read_excel('./data/count_ALD_dpt.xls', sheet_name='dpt')
        ald_per_dpt=pd.melt(ald_per_dpt,id_vars=['dpt'])
        ald_per_dpt.rename(columns={'variable':'ALD'},inplace=True)
        #incidence of each ALD in each dpt
        pop_D=pop_ASD.groupby(['dpt']).agg({"value":"sum"})
        pop_D= pop_D.reset_index()
        pop_D.columns = pop_D.columns.get_level_values(0)
        pALD_knowing_dpt = pd.merge(ald_per_dpt,pop_D,how="inner",on="dpt")
        pALD_knowing_dpt['p'] = pALD_knowing_dpt['value_x']/pALD_knowing_dpt['value_y']
        pALD_knowing_dpt = pALD_knowing_dpt[['dpt','ALD','p']]
        
        #estimate the incidence of each ALD in the general population
        ald=ald_per_dpt.groupby(["ALD"]).agg({"value":["sum"]})
        ald= ald.reset_index()
        ald.columns = ald.columns.get_level_values(0)
        ald.set_index("ALD", inplace=True)
        ald['p']=ald['value']/self.tot_pop
        ald.reset_index(inplace=True)
        
        
        #counts of ALD per sex/age
        ald_per_sexage=pd.read_excel('./data/count_ALD_dpt.xls', sheet_name='sexe-age')
        ald_per_sexage=pd.melt(ald_per_sexage,id_vars=['Ald','Sexe'])
        ald_per_sexage.rename(columns={'Ald':'ALD', 'variable':"age", 'Sexe':'sex'},inplace=True)
        ald_per_sexage['age']=ald_per_sexage['age'].astype("int64")
        #incidence of each ALD for each sex/age
        pop_AS=pop_ASD.groupby(['sex','age']).agg({"value":"sum"})
        pop_AS= pop_AS.reset_index()
        pop_AS.columns = pop_AS.columns.get_level_values(0)
        pALD_knowing_sexage = pd.merge(ald_per_sexage, pop_AS, how="inner", on=["sex","age"])
        pALD_knowing_sexage['p'] = pALD_knowing_sexage['value_x']/pALD_knowing_sexage['value_y']
        pALD_knowing_sexage = pALD_knowing_sexage[['sex','age','ALD','p']]

        p_d_ald=pd.merge(ald_per_dpt,ald, how="inner", on='ALD')
        p_d_ald['p']=p_d_ald['value_x']/p_d_ald['value_y']
        p_d_ald=p_d_ald[['dpt','ALD','p']]
        p_sa_ald=pd.merge(ald_per_sexage,ald, how="inner", on='ALD')
        p_sa_ald['p']=p_sa_ald['value_x']/p_sa_ald['value_y']
        p_sa_ald=p_sa_ald[['sex','age','ALD','p']]
        #estimate join distribution (with independance assumption between dpt and sex)
        p_dsa_ald=pd.merge(p_sa_ald,p_d_ald,how="inner",on="ALD")
        p_dsa_ald['p']=p_dsa_ald['p_x']*p_dsa_ald['p_y']
        p_dsa_ald=p_dsa_ald[['sex','age','ALD','dpt', 'p']]
        
        P=pd.merge(p_dsa_ald,ald[['ALD','p']],how="inner",on="ALD")
        P.rename(columns={'p_x':'p_dsa_ald','p_y':'p_ald'},inplace=True)
        P=pd.merge(P,pop_ASD,how="inner",on=["sex","age","dpt"])
        P.rename(columns={'value':'p_dsa'},inplace=True)
        P['p'] = P['p_dsa_ald']*P['p_ald']/P['p_dsa']
        self.P=P[['sex','age','dpt','ALD','p']]
        
        #p gives the conditional probabilities of having ALD knowing the dpt, sex and age
        
        #free memory
        #del(pop_ASD,pop_AS,ald_per_dpt,ald_per_sexage,pALD_knowing_sexage,p_d_ald,p_sa_ald)
        
        
    def generate(self, n=0):
        patients=[]
        
        for index, ps in self.pop.iterrows():
            if n==0:
                nb=ps['value']
            else:
                nb=int(ps['value']*n/self.tot_pop)
            for i in range( nb ):
                p=Patient()
                p.Sex=ps['sex']
                age = rd.randint(ps['age'],ps['age']+5)
                p.BD=self.context.generate_date(date(datetime.today().year-age,1,1), date(datetime.today().year-age,12,31))
                p.Dpt = ps['dpt']
                p.City = ps['Code'][-3:]
                self.__generateNIR__(p)
                
                if self.GPs:
                    #selection des médecins de la même ville
                    gps=[gp for gp in self.GPs if gp.code_commune==p.City]
                    if len(gps)==0:
                        #si il n'y en a pas, même département
                        gps=[gp for gp in self.GPs if gp.dpt==p.Dpt]
                        #print("\tGP found")
                    #else:
                    #    print("\tGP not found")
                    if len(gps)==0:
                        #sinon, on les prends tous
                        gps=self.GPs
                    #et on tire au hasard au milieu de ces médecins
                    mtt=rd.choice(gps)
                    p.MTT=mtt
                    
                #generate a list of ALDs 
                pALD=self.P[(self.P['dpt']==ps['dpt']) & (self.P['age']==ps['age'])& (self.P['sex']==ps['sex'])][["ALD",'p']]
                p.ALD=pALD[pALD['p']>=rd.rand(len(pALD))]['ALD']
            
                patients.append( p )
    
        return patients



if __name__ == "__main__":
    
    context = FactoryContext(nomenclatures="/home/tguyet/Progs/medtrajectory_datagen/Generator/snds_nomenclature.db")
    """
    factory = FinessPharmacyFactory(context, [35])
    ps= factory.generate()
    for p in ps:
        print(p)
        
    factory = FinessEtablissementFactory(context, [22,35])
    etab= factory.generate()
    for p in etab:
        print(p)
    """
    factory = OpenPhysicianFactory(context, [22,35])
    GPs= factory.generate(1000)
    for p in GPs:
        print(p)
    
    """
    spes =  [ p.speciality for p in etab ]
    pd.Series(spes).hist(bins=50)
    """
    
    factory = OpenPatientFactory(context, GPs, [22,35])
    etab= factory.generate(1000)
    for p in etab:
        print(p)
    