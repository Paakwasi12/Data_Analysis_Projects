create table datetime_dim(datetime_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) Primary key,
tpep_pickup_date Timestamp,
tpep_dropoff_datetime Timestamp
);
/
insert into datetime_dim(tpep_pickup_date,tpep_dropoff_datetime)
Select distinct tpep_pickup_datetime,tpep_dropoff_datetime
from uber_table;
/
ALTER TABLE datetime_dim
ADD pick_hour NUMBER;
/
ALTER TABLE datetime_dim
ADD pick_day NUMBER;
/
ALTER TABLE datetime_dim
ADD pick_month NUMBER;
/
ALTER TABLE datetime_dim
ADD pick_year NUMBER;
/
ALTER TABLE datetime_dim
ADD pick_weekday NUMBER;
/
UPDATE datetime_dim
SET pick_hour = EXTRACT(HOUR FROM tpep_pickup_date);
/
UPDATE datetime_dim
SET pick_day = EXTRACT(DAY FROM tpep_pickup_date);
/
UPDATE datetime_dim
SET pick_month = EXTRACT(MONTH FROM tpep_pickup_date);
/
UPDATE datetime_dim
SET pick_year = EXTRACT(YEAR FROM tpep_pickup_date);
/
UPDATE datetime_dim
SET pick_weekday = TO_CHAR(tpep_pickup_date, 'D');
/
ALTER TABLE datetime_dim
ADD dropoff_hour NUMBER;
/
ALTER TABLE datetime_dim
ADD dropoff_day NUMBER;
/
ALTER TABLE datetime_dim
ADD dropoff_month NUMBER;
/
ALTER TABLE datetime_dim
ADD dropoff_year NUMBER;
/
ALTER TABLE datetime_dim
ADD dropoff_weekday NUMBER;
/
UPDATE datetime_dim
SET dropoff_hour = EXTRACT(HOUR FROM tpep_dropoff_datetime);
/
UPDATE datetime_dim
SET dropoff_day = EXTRACT(DAY FROM tpep_dropoff_datetime);
/
UPDATE datetime_dim
SET dropoff_month = EXTRACT(MONTH FROM tpep_dropoff_datetime);
/
UPDATE datetime_dim
SET dropoff_year = EXTRACT(YEAR FROM tpep_dropoff_datetime);
/
UPDATE datetime_dim
SET dropoff_weekday = TO_CHAR(tpep_dropoff_datetime, 'D');
//
create table passenger_count_dim(passenger_count_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) Primary key,
passenger_count number);
/
INSERT INTO passenger_count_dim (passenger_count)
SELECT DISTINCT passenger_count
FROM uber_table;
/
create table pickup_location_dim(pickup_location_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) Primary key,
pickup_latitude number,
pickup_longitude number);
/
INSERT INTO pickup_location_dim (pickup_latitude, pickup_longitude)
SELECT DISTINCT pickup_latitude,pickup_longitude
FROM uber_table;
/
create table drop_location_dim(drop_location_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) Primary key,
drop_latitude number,
drop_longitude number);
/
INSERT INTO drop_location_dim (drop_latitude, drop_longitude)
SELECT DISTINCT dropoff_latitude,dropoff_longitude
FROM uber_table;
/
create table trip_distance_dim(trip_distance_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) Primary key,
trip_distance number);
/
insert into trip_distance_dim(trip_distance)
select distinct trip_distance
from uber_table;
/
create table rate_code_dim(rate_code_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) Primary key,
ratecodeid number,
rate_code_name varchar(25));
/
insert into rate_code_dim(ratecodeid)
select distinct RatecodeID 
from uber_table;
/
update rate_code_dim
set rate_code_name= 
    case ratecodeid
        when 1 Then 'Standard rate'
        when 2 Then 'Jfk'
        when 3 Then 'Newark'
        when 4 Then 'Nassau or Westchester'
        when 5 Then 'Negotiated fare'
        when 6 Then 'Group ride'
        Else 'Unknown'
    End;
/
create table payment_type_dim(payment_type_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) Primary key,
payment_type number,
payment_type_name varchar(30));
/
insert into payment_type_dim(paymnet_type)
select distinct payment_type
from uber_table;
/
update payment_type_dim
set payment_type_name= 
    case paymnet_type
        when 1 Then 'credit card'
        when 2 Then 'cash'
        when 3 Then 'no charge'
        when 4 Then 'dispute'
        when 5 Then 'unknown'
        Else 'voided trip'
    End;
/
create table fact_table(trip_id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1) Primary key,
vendor_id number,
datetime_id number,
passenger_count_id number,
trip_distance_id number,
pickup_location_id number,
drop_location_id number,
rate_code_id number,
payment_type_id number,
fare_amount number,
extra_fee number,
mta_tax number,
tip_amount number,
tolls_amount number,
improvement_surcharge number,
total_amount number);
/
insert into fact_table(vendor_id,fare_amount,extra_fee,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount)
select VendorID,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount
from uber_table;
/
-- Step 1: Create a sequence
CREATE SEQUENCE datetime_id1_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE passenger_count_id1_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE trip_distance_id1_seq START WITH 1 INCREMENT BY 1; 
CREATE SEQUENCE pickup_location_id1_seq START WITH 1 INCREMENT BY 1; 
CREATE SEQUENCE drop_location_id1_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE rate_code_id1_seq START WITH 1 INCREMENT BY 1;
CREATE SEQUENCE payment_type_id1_seq START WITH 1 INCREMENT BY 1;

-- Step 2: Update the columns with consecutive numbers for the NULL values
UPDATE fact_table f
SET datetime_id = NVL(datetime_id, datetime_id1_seq.NEXTVAL),
    passenger_count_id = NVL(passenger_count_id, passenger_count_id1_seq.NEXTVAL),
    trip_distance_id = NVL(trip_distance_id, trip_distance_id1_seq.NEXTVAL),
    pickup_location_id = NVL(pickup_location_id, pickup_location_id1_seq.NEXTVAL),
    drop_location_id = NVL(drop_location_id, drop_location_id1_seq.NEXTVAL),
    rate_code_id = NVL(rate_code_id, rate_code_id1_seq.NEXTVAL),
    payment_type_id = NVL(payment_type_id, payment_type_id1_seq.NEXTVAL)
WHERE datetime_id IS NULL
    OR passenger_count_id IS NULL
    OR trip_distance_id IS NULL
    OR pickup_location_id IS NULL
    OR drop_location_id IS NULL
    OR rate_code_id IS NULL
    OR payment_type_id IS NULL;
/
-- Add FOREIGN KEY constraint to the fact_table
ALTER TABLE fact_table
ADD CONSTRAINT fk_fact_datetime
FOREIGN KEY (datetime_id)
REFERENCES datetime_dim(datetime_id);
/
-- Add FOREIGN KEY constraint to the fact_table
ALTER TABLE fact_table
ADD CONSTRAINT fk_fact_passenger_count
FOREIGN KEY (passenger_count_id)
REFERENCES passenger_count_dim(passenger_count_id);
/
ALTER TABLE fact_table
ADD CONSTRAINT fk_fact_trip_distance
FOREIGN KEY (trip_distance_id)
REFERENCES trip_distance_dim(trip_distance_id);
/
ALTER TABLE fact_table
ADD CONSTRAINT fk_fact_pickup_location
FOREIGN KEY (pickup_location_id)
REFERENCES pickup_location_dim(pickup_location_id);
/
ALTER TABLE fact_table
ADD CONSTRAINT fk_fact_drop_location
FOREIGN KEY (drop_location_id)
REFERENCES drop_location_dim(drop_location_id);
/
ALTER TABLE fact_table
ADD CONSTRAINT fk_fact_rate_code
FOREIGN KEY (rate_code_id)
REFERENCES rate_code_dim(rate_code_id);
/
ALTER TABLE fact_table
ADD CONSTRAINT fk_fact_payment
FOREIGN KEY (payment_type_id)
REFERENCES payment_type_dim(payment_type_id);
/
select vendor_id, tip_amount, datetime_id
from fact_table;
/
select datetime_dim.datetime_id,datetime_dim.tpep_pickup_date,fact_table.fare_amount
from datetime_dim
Inner Join fact_table
On datetime_dim.datetime_id=fact_table.datetime_id;
/
select value from v$parameter where name='service_names'
/
select b.payment_type_name, avg(a.tip_amount) as fo
from fact_table a
join payment_type_dim b
on a.payment_type_id = b.payment_type_id
group by b.payment_type_name;







