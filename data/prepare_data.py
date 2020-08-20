# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np

import os.path
outdir="../data"
indir="../data"


gen_pop_csv=False
gen_ALD_csv=False
gen_medecins_csv=False
gen_visits_csv=False
gen_acts_csv=False
gen_drugs_csv=True


############ Population ###################
if gen_pop_csv:
    print("############ Population ###################")
    # "pop.csv"
    try:
        pop=pd.read_excel( os.path.join(indir,"pop-sexe-age-quinquennal6816.xls"), sheet_name="COM_2016", skiprows=[i for i in range(13)], dtype={'CR':str})
    except:
        print("Error while opening file pop-sexe-age-quinquennal6816.xls")
    try:
        #remove some a priori useless columns
        del(pop['DR'])
        del(pop['STABLE'])
    except:
        pass
    
    #Gather all the columns in a unique column with variables to describe them ('sex' and 'age')
    pop=pop.melt(id_vars=['DR18','CR','RR','LIBELLE'])
    tmp=pop['variable'].str.extract(r'ageq_rec(?P<age>\d+)s(?P<sex>\d)rpop2016')
    pop=pd.concat([tmp,pop],axis=1)
    pop['age']=(pd.to_numeric(pop['age'])-1)*5 #counts for 5 years intervals of ages
    pop['sex']=pop['sex'].astype(int)
    
    del(tmp)
    del(pop['variable'])
    
    #rename columns
    pop=pop.rename(columns={'CR':'code', 'value':'pop', 'LIBELLE':'city_name', 'DR18':'dpt'})
    pop['city_name']=pop['city_name'].str.lower()
    
    print("\t generate pop.csv")
    pop.to_csv( os.path.join(outdir,"pop.csv") )

if gen_ALD_csv:
    print("############  ALD #################")
    # "ALD_p.csv"
    
    try:pop
    except: pop=None
    if pop is None:
        try:
            pop=pd.read_csv( os.path.join(outdir,"pop.csv") )
        except:
            print("Error while opening file pop.csv")
            
    pop_tot = np.sum(pop["pop"])
    
    pop_ASD=pop.groupby(["age","sex",'dpt']).agg({"pop":"sum"})
    pop_ASD= pop_ASD.reset_index() #transform the group object into a dataframe
    pop_ASD.columns = pop_ASD.columns.get_level_values(0)
    pop_ASD['pop']=pop_ASD['pop']/pop_tot
    pop_ASD['sex']=pop_ASD['sex'].astype("int64")
    
    #counts of ALD per departement
    ald_per_dpt=pd.read_excel( os.path.join(indir,"count_ALD.xls"), sheet_name='dpt')
    ald_per_dpt=pd.melt(ald_per_dpt,id_vars=['dpt'])
    ald_per_dpt.rename(columns={'variable':'ALD'},inplace=True)
    
    #estimate the incidence of each ALD in the general population
    ald=ald_per_dpt.groupby(["ALD"]).agg({"value":["sum"]})
    ald= ald.reset_index()
    ald.columns = ald.columns.get_level_values(0)
    ald.set_index("ALD", inplace=True)
    
    ald['p']=ald['value']/pop_tot
    ald.reset_index(inplace=True)
    
    #incidence of each ALD in each dpt
    pop_D=pop_ASD.groupby(['dpt']).agg({"pop":"sum"})
    pop_D= pop_D.reset_index()
    pop_D.columns = pop_D.columns.get_level_values(0)
    pALD_knowing_dpt = pd.merge(ald_per_dpt,pop_D,how="inner",on="dpt")
    pALD_knowing_dpt['p'] = pALD_knowing_dpt['value']/pALD_knowing_dpt['pop']
    pALD_knowing_dpt = pALD_knowing_dpt[['dpt','ALD','p']]
    
    #counts of ALD per sex/age
    ald_per_sexage=pd.read_excel( os.path.join(indir,"count_ALD.xls"), sheet_name='sexe-age')
    ald_per_sexage=pd.melt(ald_per_sexage,id_vars=['Ald','Sexe'])
    ald_per_sexage.rename(columns={'Ald':'ALD', 'variable':"age", 'Sexe':'sex'},inplace=True)
    ald_per_sexage['age']=ald_per_sexage['age'].astype("int64")
    
    #incidence of each ALD for each sex/age
    pop_AS=pop_ASD.groupby(['sex','age']).agg({"pop":"sum"})
    pop_AS= pop_AS.reset_index()
    pop_AS.columns = pop_AS.columns.get_level_values(0)
    #pop_AS['sex']=pop_AS['sex'].astype("int64")
    pALD_knowing_sexage = pd.merge(ald_per_sexage, pop_AS, how="inner", on=["sex","age"])
    pALD_knowing_sexage['p'] = pALD_knowing_sexage['value']/pALD_knowing_sexage['pop']
    pALD_knowing_sexage = pALD_knowing_sexage[['sex','age','ALD','p']]
    
    p_d_ald=pd.merge(ald_per_dpt,ald, how="inner", on='ALD')
    p_d_ald['p']=p_d_ald['value_x']/p_d_ald['value_y']
    p_d_ald=p_d_ald[['dpt','ALD','p']]
    
    p_sa_ald=pd.merge(ald_per_sexage,ald, how="inner", on='ALD')
    p_sa_ald['p']=p_sa_ald['value_x']/p_sa_ald['value_y']
    p_sa_ald=p_sa_ald[['sex','age','ALD','p']]
    
    p_dsa_ald=pd.merge(p_sa_ald,p_d_ald,how="inner",on="ALD")
    p_dsa_ald['p']=p_dsa_ald['p_x']*p_dsa_ald['p_y']
    p_dsa_ald=p_dsa_ald[['sex','age','ALD','dpt', 'p']]
    
    P=pd.merge(p_dsa_ald,ald[['ALD','p']],how="inner",on="ALD")
    P.rename(columns={'p_x':'p_dsa_ald','p_y':'p_ald'},inplace=True)
    P=pd.merge(P,pop_ASD,how="inner",on=["sex","age","dpt"])
    P.rename(columns={'pop':'p_dsa'},inplace=True)
    P['p'] = P['p_dsa_ald']*P['p_ald']/P['p_dsa']
    P=P[['sex','age','dpt','ALD','p']]
    
    print("\t generate ALD_p.csv")
    P.to_csv( os.path.join(outdir,"ALD_p.csv") )


if gen_medecins_csv:
    print("############  Medecins ########################")
    # "medecins.csv"
    
    
    data = pd.read_csv( os.path.join(indir,"ps-infospratiques.csv"), sep=";", header=None, encoding="latin_1")
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
        
    # Médecins libéraux exercant en ville
    medecins=data[(data['Type']!=0) & (data['Type']!=6) & (data['Type']!=7) & (data['Type']!=8) ]
    
    # codage du sex selon la IR_SEX_COD
    medecins["Sexe"].fillna("9",inplace=True)
    medecins["Sexe"]=medecins["Sexe"].str.replace(pat="F",repl="2")
    medecins["Sexe"]=medecins["Sexe"].str.replace(pat="H",repl="1")
    # suppression des CEDEX
    medecins["Ville"]=medecins["Ville"].str.replace(pat=" CEDEX.*",repl="", regex=True)
    
    medecins=medecins[["Sexe","nom","prenom","CP","Ville","Profession"]].drop_duplicates()
    # on supprime les "non-médecins" (parce qu'ils ne correspondent pas à cette classe des spécialistes)
    medecins=medecins[ medecins["Profession"]!=1 ] # suppression dans ambulanciers
    medecins=medecins[ medecins["Profession"]!=2 ] # suppression dans anathamo-patho
    medecins=medecins[ (medecins["Profession"]<18) | (medecins["Profession"]>21) ] # suppression des dentistes
    medecins=medecins[ (medecins["Profession"]<24) | (medecins["Profession"]>32) ] # suppression des fournisseurs de matériel
    medecins=medecins[ (medecins["Profession"]!=39) ] # suppression des infirmiers
    medecins=medecins[ (medecins["Profession"]<40) | (medecins["Profession"]>42)] # suppression des laboratoires
    medecins=medecins[ (medecins["Profession"]!=43) ] # suppression des kinés
    medecins=medecins[ (medecins["Profession"]!=57) ] # suppression des orthophonistes
    medecins=medecins[ (medecins["Profession"]!=58) ] # suppression des orthoptiste
    medecins=medecins[ (medecins["Profession"]!=58) ] # suppression des pédicures-podologues
    medecins=medecins[ (medecins["Profession"]<62) | (medecins["Profession"]>63)] # suppression des pharmaciens
    medecins=medecins[ (medecins["Profession"]!=71) ] # suppression des sages-femmes
    
    medecins.drop(["nom","prenom"],axis=1, inplace=True)
    
    # Alignement des noms de ville
    
    data = pd.read_csv(os.path.join(indir,"laposte_hexasmal.csv"), sep=";", usecols=["Code_commune_INSEE","Code_postal","Nom_commune"])
    # du fait des libellés d'acheminement, il peut y avoir plusieurs entrées identique pour la correspondance CP/INSEE
    data.drop_duplicates(inplace=True)
    
    medecins_insee=pd.merge(data, medecins, how="right", right_on=["CP","Ville"], left_on=["Code_postal","Nom_commune"])[["Code_commune_INSEE","Ville","CP","Sexe","Profession"]]
    """
    #mais ... des correspondances n'ont pas fontionner
    medecins_manquants=medecins_insee[medecins_insee["Code_commune_INSEE"].fillna("")==""]
    
    #utilisation d'une librairie permettant de faire une matching approximatif
    import difflib
    
    # Pour chaque ville, on cherche la correspondance la plus proche dans la base des communes disponibles !
    ## ATTENTION: cette étape est longue en calcul
    def proceed(x):
        corresp = difflib.get_close_matches(x, data["Nom_commune"])
        if len(corresp)==0:
            return ""
        else:
            return corresp[0]
        return 
    
    medecins_manquants['ville_align']=medecins_manquants["Ville"].apply(proceed)
    medecins_insee=pd.merge(data, medecins, how="right", right_on=["CP","ville_align"], left_on=["Code_postal","Nom_commune"])[["Code_commune_INSEE","Ville","CP","Sexe","Profession"]]
    """
    print("\t generate medecins.csv")
    medecins_insee[medecins_insee["Code_commune_INSEE"].fillna("")!=""].to_csv( os.path.join(outdir,"medecins.csv") )
    
    
if gen_visits_csv:
    print("############# VISITS #################")
    # "p_prsnat_rragesex.csv" "nb_prs_rragesex.csv"
    
    try:pop
    except: pop=None
    if pop is None:
        try:
            pop=pd.read_csv( os.path.join(outdir,"pop.csv") )
        except:
            print("Error while opening file pop.csv")
          
    damir=pd.read_csv( os.path.join(indir,"A201601.csv"), sep=';', usecols=['BEN_SEX_COD','AGE_BEN_SNDS','BEN_RES_REG','PSE_ACT_CAT','PSE_SPE_SNDS', 'PRS_NAT', "PRS_ACT_QTE"])
    damir.rename(columns={"BEN_RES_REG":"RR","BEN_SEX_COD":"sex","AGE_BEN_SNDS":"age","PRS_NAT":"prs_nat","PSE_ACT_CAT":"pse_cat","PSE_SPE_SNDS":"exe_spe", "PRS_ACT_QTE":"act_dnb"}, inplace=True)
    damir_dnb=damir.groupby(["RR","sex","age","prs_nat","pse_cat","exe_spe"]).agg({"act_dnb":'sum'}).reset_index()
    damir_dnb.columns = damir_dnb.columns.get_level_values(0)
    
    #Sélection uniquement des codes de PRS inférieurs à 1300 (sinon, hors champs qui nous intéresse) + comptes positifs uniquement
    # -> uniquement les 11XX pour les consultations
    # -> et les 12XX pour les visites
    prs=damir_dnb[(damir_dnb["prs_nat"]<1300) & (damir_dnb["act_dnb"]>=0)]
    
    #on ne garde que les prestations executées par des médecins
    prs=prs[(prs["pse_cat"]==1)]
    pop_rr=pop.groupby(['RR','sex','age']).agg({"pop":'sum'})
    pop_rr.reset_index(inplace=True)
    pop_rr.columns = pop_rr.columns.get_level_values(0)
    ## Define new Region (gather outre-mer lands and departements)
    def new_rr(x):
        if x<=6: return 5
        else: return x
    pop_rr['RR']=pop_rr['RR'].apply(new_rr)
    
    ##Define new age classes (compliant with DAMIR)
    def new_age(x):
        x=x-x%10
        if x==10: x=0
        if x>80: x=80
        return x
    pop_rr['age']=pop_rr['age'].apply(new_age)
    
    # recompute the population
    pop_rr=pop_rr.groupby(['RR','sex','age']).agg({"pop":'sum'})
    pop_rr.reset_index(inplace=True)
    pop_rr.columns = pop_rr.columns.get_level_values(0)
    
    ## Number of visits per specialist per year (knowing the departement)
    prs_spedpt=prs.groupby(["RR","age","sex","exe_spe"]).agg({"act_dnb":["sum"]})
    prs_spedpt.reset_index(inplace=True)
    prs_spedpt.columns = prs_spedpt.columns.get_level_values(0)
    nb_prs_spedpt=pd.merge(prs_spedpt,pop_rr,on=["RR","age","sex"])
    nb_prs_spedpt['nb']=nb_prs_spedpt['act_dnb']/nb_prs_spedpt["pop"]*12 # *12 to have a yearly number
    nb_prs_spedpt=nb_prs_spedpt[["RR","age","sex","exe_spe",'nb']]
    
    print("\t generate nb_prs_rragesex.csv")
    nb_prs_spedpt.to_csv( os.path.join(outdir,"nb_prs_rragesex.csv") )
    
    prs_spe=prs.groupby(["RR","age","sex","exe_spe"]).agg({"act_dnb":["sum"]})
    prs_spe.reset_index(inplace=True)
    prs_spe.columns = prs_spe.columns.get_level_values(0)
    
    prs_spe2=prs.groupby(["RR","age","sex","exe_spe","prs_nat"]).agg({"act_dnb":["sum"]})
    prs_spe2.reset_index(inplace=True)
    prs_spe2.columns = prs_spe2.columns.get_level_values(0)
    
    #probability of having a PRS of nature X knowing that you live in a dpt (and that you had a visit to a specialist, with specialty exe_spe)
    p_nat_spedpt=pd.merge(prs_spe2,prs_spe,on=["RR","age","sex","exe_spe"],suffixes=('','_dpt'))
    p_nat_spedpt['p']=p_nat_spedpt['act_dnb']/p_nat_spedpt["act_dnb_dpt"]
    p_nat_spedpt=p_nat_spedpt[["RR","age","sex","exe_spe",'prs_nat','p']]
    
    print("\t generate p_prsnat_rragesex.csv")
    p_nat_spedpt.to_csv(os.path.join(outdir,"p_prsnat_rragesex.csv") )
    


if gen_acts_csv:
    print("############# MEDICAL ACTS #################")
    ## "p_act_prs.csv" and "nb_acts_rragesex.csv"
    
    try:pop_rr
    except: pop_rr=None
    if pop_rr is None:
        try:
            pop=pd.read_csv( os.path.join(outdir,"pop.csv") )
            pop_rr=pop.groupby(['RR','sex','age']).agg({"pop":'sum'})
            pop_rr.reset_index(inplace=True)
            pop_rr.columns = pop_rr.columns.get_level_values(0)
            ## Define new Region (gather outre-mer lands and departements)
            def new_rr(x):
                if x<=6: return 5
                else: return x
            pop_rr['RR']=pop_rr['RR'].apply(new_rr)
            
            ##Define new age classes (compliant with DAMIR)
            def new_age(x):
                x=x-x%10
                if x==10: x=0
                if x>80: x=80
                return x
            pop_rr['age']=pop_rr['age'].apply(new_age)
            
            # recompute the population
            pop_rr=pop_rr.groupby(['RR','sex','age']).agg({"pop":'sum'})
            pop_rr.reset_index(inplace=True)
            pop_rr.columns = pop_rr.columns.get_level_values(0)
        except:
            print("Error while opening file pop.csv")
    
    #Ouverture du jeu de données OpenDAMIR
    try: damir
    except NameError: damir = None
    if damir is None:
        damir=pd.read_csv(os.path.join(indir,"A201601.csv"), sep=';', usecols=['BEN_SEX_COD','AGE_BEN_SNDS','BEN_RES_REG','PSE_ACT_CAT','PSE_SPE_SNDS', 'PRS_NAT', "PRS_ACT_QTE"])
        damir.rename(columns={"BEN_RES_REG":"RR","BEN_SEX_COD":"sex","AGE_BEN_SNDS":"age","PRS_NAT":"prs_nat","PSE_ACT_CAT":"pse_cat","PSE_SPE_SNDS":"exe_spe", "PRS_ACT_QTE":"act_dnb"}, inplace=True)
        damir_dnb=damir.groupby(["RR","sex","age","prs_nat","pse_cat","exe_spe"]).agg({"act_dnb":'sum'}).reset_index()
        damir_dnb.columns = damir_dnb.columns.get_level_values(0)
    
    #Sélection uniquement des codes de ANT_PRS de type 13XX : actes médicaux
    acts=damir_dnb[(damir_dnb["prs_nat"]<1400) &(damir_dnb["prs_nat"]>=1300) & (damir_dnb["act_dnb"]>=0)]
    
    #on regroupe les actes délivrées par des médecins ou des établissements (code pse_cat 0 et 1)
    acts=acts.groupby(["RR","sex","age","prs_nat","exe_spe"]).agg({"act_dnb":"sum"}).reset_index()
    acts=acts[ (acts["prs_nat"]!=1311) & (acts["prs_nat"]!=1341) & (acts["prs_nat"]!=1318) & (acts["prs_nat"]!=1361) & (acts["prs_nat"]!=1316) ]
    
    
    ## Mean number of acts per acts type per year per people (knowing their age, sex and region), whatever the specialist
    acts_grp=acts.groupby(["RR","age","sex","prs_nat"]).agg({"act_dnb":"sum"}).reset_index()
    nb_acts=pd.merge(acts_grp,pop_rr,on=["RR","age","sex"])
    nb_acts['nb']=nb_acts['act_dnb']/nb_acts["pop"]*12 # *12 to have a yearly number
    nb_acts=nb_acts[["RR","age","sex","prs_nat",'nb']]
    nb_acts.head()
    
    print("\t generate nb_acts_rragesex.csv")
    nb_acts.to_csv( os.path.join(outdir,"nb_acts_rragesex.csv") )
    
    
    p_spe=pd.merge(acts,acts_grp, on=["RR","age","sex","prs_nat"])
    p_spe['p']=p_spe['act_dnb_x']/p_spe['act_dnb_y']
    p_spe=p_spe[["RR","age","sex","prs_nat","exe_spe",'p']]
    print("\t generate p_exespe_acts.csv")
    p_spe.to_csv( os.path.join(outdir,"p_exespe_acts.csv") )
    
    actes = pd.read_excel( os.path.join(indir,"Actes_techniques_de_la_CCAM_en_2016.xls"), sheet_name='Panorama des actes CCAM')
    actes.rename(columns={"Code Acte":'ccam',"Code Regroupement":"grp", "Quantité d'actes ":"nb"}, inplace=True)
    actes=actes.groupby(['ccam',"grp"]).agg({"nb":"sum"}).reset_index()
    
    actes=actes[ (actes['grp']=="ADC") | (actes['grp']=="ACO") |(actes['grp']=="ADA") |(actes['grp']=="ADE") |(actes['grp']=="ADI") |(actes['grp']=="ADT") ]
    actes=actes[ (actes['ccam'].str[:2]!="HB") ]
    actes_tot=actes.groupby(['grp']).agg({"nb":"sum"}).reset_index()
    p_act=pd.merge(actes,actes_tot,on=['grp'])
    p_act['p']=p_act['nb_x']/p_act['nb_y']
    p_act=p_act[['ccam',"grp",'p']]
    
    prsnat_regrp= {1312:"ADC", #ACTES DE SPECIALITE EN K
                   1321:"ADC", #ACTE DE CHIRURGIE CCAM
                   1322:"ACO", #ACTE D'OBSTETRIQUE CCAM
                   1323:"ADA", #ACTE D'ANESTHESIE CCAM
                   1324:"ADE", #ACTE D'ECHOGRAPHIE CCAM
                   1331:"ADI", #ACTES DE RADIOLOGIE
                   1335:"ADI", #ACTE DE RADIOLOGIE MAMMOGRAPHIE
                   1336:"ADI", #ACTE DE RADIOLOGIE MAMMOGRAPHIE DEPISTAGE
                   1351:"ADI", #ACTE D'IMAGERIE (hors ECHOGRAPHIE) CCAM
                   1352:"ADT"  #ACTES TECHNIQUES MEDICAUX  (hors IMAGERIE) CCAM
                  }
    corresp=pd.DataFrame.from_dict(prsnat_regrp, orient='index').reset_index()
    corresp.rename(columns={"index":"prs_nat",0:'grp'},inplace=True)
    p_act_prs=pd.merge(p_act,corresp,on="grp",how="outer")[["prs_nat","ccam","p"]]
    
    print("\t generate p_act_prs.csv")
    p_act_prs.to_csv( os.path.join(outdir,"p_act_prs.csv") )

if gen_drugs_csv:
    print("############# DRUGS DELIVERIES #################")
    #"mean_deliveries.csv" and "drugs_freq.csv"
    
    try:pop
    except: pop=None
    if pop is None:
        try:
            pop=pd.read_csv( os.path.join(outdir,"pop.csv") )
        except:
            print("Error while opening file pop.csv")
    
    data = pd.read_csv(os.path.join(indir,"OPEN_MEDIC_2019.zip"), header=0, sep=';', encoding="latin_1")
    #remove labels of drugs and useless codes
    try:
        del(data['l_ATC1'])
        del(data['L_ATC2'])
        del(data['L_ATC3'])
        del(data['L_ATC4'])
        del(data['L_ATC5'])
        del(data['l_cip13'])
        del(data['ATC1'])
        del(data['ATC2'])
        del(data['ATC3'])
        del(data['ATC4'])
        del(data['TOP_GEN'])
        del(data['GEN_NUM'])
        del(data['REM'])
        del(data['BSE'])
    except:
        pass
    
    ## The following lines keep only the meaningful dimensions
    drugs=data.rename(columns={"sexe":"sex", "BEN_REG":"RR"}).groupby(["age","sex","RR","ATC5","CIP13"]).agg({"BOITES":["sum"]}) #compute a group object
    drugs = drugs.reset_index() #transform the group object into a dataframe
    drugs.columns = drugs.columns.get_level_values(0)
    
    ## Drugcounts evaluates the total number of deliveries per age and sex (what ever the drug)
    
    # Remove negative counts
    drugs=drugs[drugs['BOITES']>0]
    
    #Evaluated the totals of drug deliveries per age and per sex
    drugcounts=drugs.groupby(["age","RR","sex"]).agg({"BOITES":["sum"]})
    drugcounts = drugcounts.reset_index() #transform the group object into a dataframe
    drugcounts.columns = drugcounts.columns.get_level_values(0)
    drugcounts.rename( columns={'BOITES':'count'}, inplace=True)
    
    # We now compute the frequency of the deliveries for each drug per group of sex and age
    drug_freq=pd.merge(drugs,drugcounts,how='left', on=['age','RR','sex'])
    drug_freq['p']=drug_freq['BOITES']/drug_freq['count']
    drug_freq
    
    #sauvegarde de la matrice pour réutilisation
    print("\t generate drugs_freq.csv")
    drug_freq[['age','sex','ATC5','RR','CIP13','p']].to_csv(os.path.join(outdir,"drugs_freq.csv"))
    
    mapping = {84:84, 32:32, 93:93, 44:44, 76:76, 28:28, 75:75, 24:24, 94:93, 27:27, 53:53, 52:52, 11:11,  1:0,  2:0,  3:0,  4:0}
    
    regions = [84, 32, 93, 44, 76, 28, 75, 24, 94, 27, 53, 52, 11,  1,  2,  3,  4]
    sexes = [1,2]
    ages=[(0,20), (20,60),(60,95),(95,150) ]
    
    index = pd.MultiIndex.from_product([ages, regions, sexes], names = ["age", "RR", 'sex'])
    mean_deliveries=pd.DataFrame(index = index)
    mean_deliveries['mean']=float(0)
    
    for r in regions:
        for s in sexes:
            for age in ages:
                if r==93 or r==94:
                    #sum populations of 93 + 94
                    cpop=np.sum(pop[ ((pop["RR"]==93) |(pop["RR"]==94) ) & (pop["age"]>=age[0]) & (pop["age"]< age[1]) & (pop['sex']==s) ]['pop'])
                    rd=93
                elif r==1 or r==2 or r==3 or r==4:
                    #sum populations of 1+2+3+4
                    cpop=np.sum(pop[ ((pop["RR"]==1) |(pop["RR"]==2)|(pop["RR"]==3) |(pop["RR"]==4) ) & (pop["age"]>=age[0]) & (pop["age"]< age[1]) & (pop['sex']==s) ]['pop'])
                    rd=0
                else:
                    cpop=np.sum(pop[ (pop["RR"]==r) & (pop["age"]>=age[0]) & (pop["age"]< age[1]) & (pop['sex']==s) ]['pop'])
                    rd=r
                if age[0]==95:
                    a=99
                else:
                    a=age[0]
                if cpop!=0:
                    mean_deliveries.loc[age,r,s]['mean']=np.sum(drug_freq[ (drug_freq['age']==a) & (drug_freq['sex']==s) & (drug_freq['RR']==rd) ]['BOITES'])/cpop
    mean_deliveries.reset_index(inplace=True)
    mean_deliveries['age']=mean_deliveries['age'].apply(lambda x: x[0])
    
    print("\t generate mean_deliveries.csv")
    mean_deliveries.to_csv( os.path.join(outdir,"mean_deliveries.csv") )
    