#import dependencies
import sqlalchemy
import dotenv
import os

#define a function to drop existing tables
def drop_tables(dbtables, engine):
    for table in dbtables:
        engine.execute("DROP TABLE IF EXISTS " + table + ";")
        print("table: " + table + " dropped")

#load environment variables
dotenv.load_dotenv()
 
mysqlhost = os.getenv("MYSQL_HOSTNAME")
mysqluser = os.getenv("MYSQL_USERNAME")
mysqlpassword = os.getenv("MYSQL_PASSWORD")
mysqldatabase = os.getenv("MYSQL_DATABASE")

#connect to mysql database
sqlengine = sqlalchemy.create_engine(f"mysql+pymysql://{mysqluser}:{mysqlpassword}@{mysqlhost}/{mysqldatabase}")
#drop existing tables if they exist
dbtables = ['conditions', 'medications', 'patient_conditions', 'patient_medications', 'patient_social_determinants', 'patient_treatements_procedures', 'patients', 'social_determinants', 'treatments_procedures']
drop_tables(dbtables, sqlengine)

#Set up queries for creating tables
table_prod_patients = """
create table if not exists patients (
    id int auto_increment,
    mrn varchar(255) default null unique,
    first_name varchar(255) default null,
    last_name varchar(255) default null,
    zip_code varchar(255) default null,
    dob varchar(255) default null,
    gender varchar(255) default null,
    contact_mobile varchar(255) default null,
    contact_home varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""
table_prod_medications = """
create table if not exists medications (
    id int auto_increment,
    med_ndc varchar(255) default null unique,
    med_human_name varchar(255) default null,
    med_is_dangerous varchar(255) default null,
    PRIMARY KEY (id)
); 
"""
table_prod_conditions = """
create table if not exists conditions (
    id int auto_increment,
    icd10_code varchar(255) default null unique,
    icd10_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""
table_prod_patients_medications = """
create table if not exists patient_medications (
    id int auto_increment,
    mrn varchar(255) default null,
    med_ndc varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (med_ndc) REFERENCES medications(med_ndc) ON DELETE CASCADE
); 
"""
table_prod_patient_conditions = """
create table if not exists patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES conditions(icd10_code) ON DELETE CASCADE
); 
"""
table_prod_patients_social_determinants = """
create table if not exists patient_social_determinants (
    id int auto_increment,
    mrn varchar(255) default null,
    loinc_code varchar(255) default null,
    loinc_description varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (loinc_code) REFERENCES social_determinants(loinc_code) ON DELETE CASCADE
); 
"""
table_prod_social_determinants = """
create table if not exists social_determinants (
    id int auto_increment,
    loinc_code varchar(255) default null unique,
    loinc_category varchar(255) default null,
    loinc_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""
table_prod_treatments_procedures = """
create table if not exists treatments_procedures (
    id int auto_increment,
    cpt_code varchar(255) default null unique,
    cpt_description varchar(255) default null,
    PRIMARY KEY (id) 
); 
"""
table_prod_patients_treatments_procedures = """
create table if not exists patient_treatments_procedures (
    id int auto_increment,
    mrn varchar(255) default null,
    cpt_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (cpt_code) REFERENCES treatments_procedures(cpt_code) ON DELETE CASCADE
); 
"""
#execute queries
sqlengine.execute(table_prod_patients)
sqlengine.execute(table_prod_medications)
sqlengine.execute(table_prod_conditions)
sqlengine.execute(table_prod_social_determinants)
sqlengine.execute(table_prod_patients_medications)
sqlengine.execute(table_prod_patient_conditions)
sqlengine.execute(table_prod_patients_social_determinants)
sqlengine.execute(table_prod_treatments_procedures)
sqlengine.execute(table_prod_patients_treatments_procedures)


#confirm tables were created
sqlengine.table_names()