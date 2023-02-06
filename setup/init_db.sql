-- Create user
CREATE USER dataeng WITH ENCRYPTED PASSWORD 'best';

-- Create database
CREATE DATABASE doctor_office;

-- Connect to new db
\c doctor_office

-- Create tables
CREATE TABLE IF NOT EXISTS patient(
  id serial PRIMARY KEY,
  phone_number VARCHAR( 10 )  NOT NULL,
	first_name VARCHAR ( 50 )  NOT NULL,
	last_name VARCHAR ( 50 )  NOT NULL
);

CREATE TABLE IF NOT EXISTS diagnose(
  id BIGINT PRIMARY KEY,
  icd10_code VARCHAR ( 10 )  NOT NULL,
  patient_id INT  NOT NULL,
  tsp TIMESTAMP NOT NULL,
  FOREIGN KEY(patient_id) REFERENCES patient(id)
);

CREATE TABLE IF NOT EXISTS claim(
  id BIGINT PRIMARY KEY,
  code VARCHAR ( 10 )  NOT NULL,
  price NUMERIC   NOT NULL,
  patient_id INT  NOT NULL,
  tsp TIMESTAMP NOT NULL,
  FOREIGN KEY(patient_id) REFERENCES patient(id)
);


-- Grants
GRANT ALL PRIVILEGES ON DATABASE doctor_office TO dataeng;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO dataeng;
GRANT CREATE ON SCHEMA public TO dataeng;

-- Insert random rows to patient
INSERT INTO patient(
  phone_number,
  first_name,
  last_name
) SELECT
  '210'  || round(random()*100) || round(random()*100) ,
  'John-' || s.*,
  'Doe-'  || s.*
FROM
    generate_series(1,1000) as s

