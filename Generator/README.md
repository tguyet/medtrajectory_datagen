
_author:_ Thomas Guyet (Institut Agro-IRISA)
_date:_ 15/5/2020


## Project objective and steps

The main objective of the project is to simulate a synthetic dataset of care pathways that may have some (statistical) similarities with a real dataset and that has some privacy guarantees.
More especially, we focus on the SNDS database schema.

In order to acheive this goal, we propose to organize the project in 3 main steps

* generating purely random data in the schema of the SNDS database (**on-going**)
* generating random data that reproduce some statistical distributions
    1. reproduce simple global statistical features such as the distribution of medics in the whole population, distribution of genders, distribution of physicians, geographic distributions, etc. Here, the specificy is to reproduce aggregated statistics that are available in open data. The challenge here is to have statistical processes to would approxiate the joint distributions!
    2. reproduce some characteristics of care pathways. At this stage, the objective is to reproduce some count statistics at the individual level. This objective requierts to have an access to real care pathways from which extract the statistical characteristics to reproduce.
    3. generate realistic care pathways (and collection of patients with different types of pathways), including the sequentiality and the delays between cares. Statistical random generation processes may be blended with rule based generation processes to reproduce care trajectories.
* generating data with privacy guarantees. The steps 2. and 3. of the previous main step may be 

## Simulation principle

To generate random data in the schema of the SNDS, there are different aspects to consider:
* the *medical environment*: it represents the types of cares or drugs that can be delivered, the geography, the temporal bourdaries of the simulation. All this information can be considered as static and available in the nomenclaure
* the *care offer*: it represents the set of care providers (physicians, pharmacists, GPs, specialists, hospital...). The list of hospital in known, but the list of care provider has to be randomly generated
* the *population*: it represents the set of patients to which cares have been delivered (or not). 
* the *care pathways*: it represent the sequence of cares delivered for a patient

For each of these aspects, there are specific data to reuse or to generate.

From the computer enginering point of view, the main idea of the tool is to have two different layers:
* a theoretical model of a popution/environment/cares: it is an internal model that we start to organize be listing the large four categories of entities
* a concrete data generation model that is able to concretise an instance of the model in a data format. The first target is a SNDS database (we choose a SQLite database and the database schema provided by the HDH). The second target is a RDF schema.


The theoretical model may have different shapes: statistical model, object oriented model, multi-agent model, ...

As a computer scientist (and pragmatically) I lean toward an object oriented model.

## A object oriented theoretical model

The model is given in the `database_model.py` file which describes a different classes that represents the entities of our model.
The `data_factory.py` file holds classes that are factories of the classes in the model. This factories may be implemented with the different strastegies we introduced above (from random to realistic).

The model we propose is made of the following main classes:
* `PS` a care provider that has sub-classes
    * `Provider` that, at the time, represents the pharmacists
    * `Physician`  that can be a `GP` or a `Specialist`
* `Patient` 
* `CareDelivery` that corresponds to the different types of care a patient may receive (drug deliveries, medical acts, GP visits, hospital stays, etc.). The categories of cares are guided by the SNDS stucture that model outpatient trajectories.
    * `DrugDelivery`
