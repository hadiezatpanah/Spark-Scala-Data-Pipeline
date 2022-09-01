CREATE ROLE postgres LOGIN SUPERUSER PASSWORD 'postgres';
CREATE DATABASE brgroup;
GRANT ALL PRIVILEGES ON DATABASE brgroup to airflow;
GRANT ALL PRIVILEGES ON DATABASE brgroup to postgres;
\c brgroup;    
CREATE SCHEMA BRGROUP;
CREATE TABLE BRGROUP.PATENT_DATA (family_id bigint PRIMARY KEY, country text array, doc_number bigint array, kind text array, doc_id bigint array, date_publ bigint array, invention_title_en text array, classification_ipcr text array, aplicant_name text array, abstract text array);