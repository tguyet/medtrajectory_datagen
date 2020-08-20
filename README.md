# medtrajectory_datagen


# Resources 

* SNDS data originates from the Synthetic SNDS project [https://gitlab.com/healthdatahub/synthetic-snds]
  - SNDS Schemas have to be downloaded on the HdH [https://gitlab.com/healthdatahub/schema-snds]
    * The schemas use the `Table Schema` format
    * WARNING: I add to make small changes in the schemas to make it compatible with SQite
  - SNDS Nomenclature datasets (alos available in the repos above)
    * CSV dataset containing the nomeclature used in the SNDS database
    * this information is anonymous
    
* Data coming from national statistics institute
  - population statistics, 2016: Quinquenal age, sex, city population [https://www.insee.fr/fr/statistiques/1893204#consulter]
  
* Data coming from the French Health Insurance (most of them are aggregated data from the raw SNDS itself)
  - For drugs deliveries Open Medic 2019, download: [https://www.data.gouv.fr/fr/datasets/r/91d193b4-dd34-4cda-a843-bd1eb30aa7bc]
  - For medical acts (outhospital), Open Damir 2019, download: [https://www.data.gouv.fr/fr/datasets/depenses-d-assurance-maladie-hors-prestations-hospitalieres-par-caisse-primaire-departement/] (used only data in January due to the size of the data)
  	- Open CCAM [https://www.scansante.fr/open-ccam/open-ccam-2016]: contains CCAM codes (not OpenDamir)
  - Information about Care Providers, Open data: [https://www.data.gouv.fr/fr/datasets/annuaire-sante-de-la-cnam/], download: [https://www.data.gouv.fr/fr/datasets/r/296394b6-d539-4cc7-a440-2698eec06c18]
  - Data about ALD (Long cares) 2016, Open data: [https://www.data.gouv.fr/fr/datasets/personnes-en-affection-de-longue-duree-ald/], downloads
    * per age/sex  [https://www.data.gouv.fr/fr/datasets/r/360e6600-7c05-46ef-aac5-cf0ea84968ae]
    * per dpt [https://www.data.gouv.fr/fr/datasets/r/26ac640b-e65f-4324-bbed-e0e9690bf449]
    * data have been reorganized manually
  - Data about care etablishment (including hospitals, pharmacies) FINESS database [https://www.data.gouv.fr/fr/datasets/finess-extraction-du-fichier-des-etablissements/], download: [https://www.data.gouv.fr/fr/datasets/r/16ee2cd3-b9fe-459e-8a57-46e03ba3adbd]
    * Note that the FINESS database is also available in the nomenclature
    * I used the clean version of the FINESS dataset, cleaned by a user (`finess_clean.csv`): [https://www.data.gouv.fr/fr/datasets/r/3dc9b1d5-0157-440d-a7b5-c894fcfdfd45]
  - Informations about hospitalisation (numbers of hospitalisation, CIMS codes) have been digged from [https://www.scansante.fr/] and [http://www.aideaucodage.fr/]


# How to proceed

* Download all the raw data available online
  * In the `data` repository, run the script `load_opendata.py` to download and unzip open data in the repository
  * Open data links may require user interactions to start the download, in this case, datasets have to be manually downloaded (use the links that will be provided by `load_opendata.py` script
* Run the Notebooks to generate intermediary files
  * Start by running the script `prepare_data.py` (ensute all the flags at the beginning of the script are set to True to generate all the required files)
  * Each steps is details in corresponding Notebooks in the `Data_Analysis`repository
* Run the script `create_nomenclature.py` to create the nomenclature part of the SNDS
* Move all the generated files in a repository corresponding to a simulation
* Run the `simulation35.py` script to run the simulator based on Open Data

# requirements

python library
- sqlalchemy
- sqlite3
- tableschema
- tableschema-sql
- wget
- unzip
- gzip
- pandas
- jupyterlab
