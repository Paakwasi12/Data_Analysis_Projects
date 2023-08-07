// --- step 1. Create table
/
--If object_id('raw_data_gdp') is not null drop table raw_data_gpd
CREATE TABLE raw_data_gdp_external (
  demo_ind NVARCHAR2(200),
  indicator NVARCHAR2(200),
  location NVARCHAR2(200),
  country NVARCHAR2(200),
  time NVARCHAR2(200),
  value FLOAT,
  flag_code NVARCHAR2(200),
  flags NVARCHAR2(200)
)
/
--select * from raw_data_gdp;
-- Data uploaded into table via ctl file (oracle loader utility)
select * from raw_data_gdp;
-- step2. Importing data
-- done from from oracle load
/
-- creating the view we need fr bi
create view GDP_EXCEL_Input As
With a as (select country,time As year_no, value as gdp_value
            from raw_data_gdp
            where indicator = 'GDP (current US$)'),
            
b as (select country, time as year_no, value as gdp_per_capita
        from raw_data_gdp
        where indicator = 'GDP per capita (current US$)')

select a.*,b.gdp_per_capita 
from a
Left join b
On a.country = b.country 
and a.year_no = b.year_no;
/

CREATE OR REPLACE PROCEDURE BULK_LOAD_DATA AS
BEGIN
   -- Run SQL*Loader to load data from CSV file
    DBMS_SCHEDULER.CREATE_JOB(
      job_name        => 'BULK_LOAD_JOB',
      job_type        => 'EXECUTABLE',
      job_action      => 'sqlldr',
      number_of_arguments => 4,
      auto_drop       => TRUE,
      enabled         => FALSE
   );
   
   DBMS_SCHEDULER.set_job_argument_value('BULK_LOAD_JOB', 1, 'control=' || 'C:\Users\donto\Downloads\database\gdp_loader.ctl');
   DBMS_SCHEDULER.set_job_argument_value('BULK_LOAD_JOB', 2, 'data=' || 'C:\Users\donto\Downloads\database\gdp_raw_data.csv');
   DBMS_SCHEDULER.set_job_argument_value('BULK_LOAD_JOB', 3, 'log=' || 'C:\Users\donto\Downloads\database\load.log');
   DBMS_SCHEDULER.set_job_argument_value('BULK_LOAD_JOB', 4, 'bad=' || 'C:\Users\donto\Downloads\database\load.bad');

   DBMS_SCHEDULER.enable('BULK_LOAD_JOB');
END;
/
BEGIN
   BULK_LOAD_DATA;
END;
/
select *
from GDP_EXCEL_Input
order by year_no;


