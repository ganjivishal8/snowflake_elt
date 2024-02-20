--Source Table
create or replace table liquor_sales_src(
invoice_and_item_number STRING ,
  date DATE ,
  store_number STRING ,
  store_name STRING ,
  address STRING ,
  city STRING ,
  zip_code STRING,
  store_location varchar,
  county_number STRING ,
  county STRING ,
  category STRING ,
  category_name STRING,
  vendor_number STRING,
  vendor_name STRING ,
  item_number STRING ,
  item_description STRING ,
  pack INT ,
  bottle_volume_ml INT ,
  state_bottle_cost FLOAT,
  state_bottle_retail FLOAT,
  bottles_sold INT,
  sale_dollars FLOAT,
  volume_sold_liters FLOAT,
  volume_sold_gallons FLOAT

);

---Target table
create or replace table liquor_sales_target(invoice_and_item_number STRING ,
  date DATE ,
  store_number STRING ,
  store_name STRING ,
  address STRING ,
  city STRING ,
  zip_code STRING,
  store_location varchar,
  county_number STRING ,
  county STRING ,
  category STRING ,
  category_name STRING,
  vendor_number STRING,
  vendor_name STRING ,
  item_number STRING ,
  item_description STRING ,
  pack INT ,
  bottle_volume_ml INT ,
  state_bottle_cost FLOAT,
  state_bottle_retail FLOAT,
  bottles_sold INT,
  sale_dollars FLOAT,
  volume_sold_liters FLOAT,
  volume_sold_gallons FLOAT
);

select * from liquor_sales_target;
---Creating Stream on Source Table
create or replace stream trans_stream on table liquor_sales_src;

select * from trans_stream;


---Stream Consume Table
create or replace table liquor_stream_consume(invoice_and_item_number STRING ,
  date DATE ,
  store_number STRING ,
  store_name STRING ,
  address STRING ,
  city STRING ,
  zip_code STRING,
  store_location varchar,
  county_number STRING ,
  county STRING ,
  category STRING ,
  category_name STRING,
  vendor_number STRING,
  vendor_name STRING ,
  item_number STRING ,
  item_description STRING ,
  pack INT ,
  bottle_volume_ml INT ,
  state_bottle_cost FLOAT,
  state_bottle_retail FLOAT,
  bottles_sold INT,
  sale_dollars FLOAT,
  volume_sold_liters FLOAT,
  volume_sold_gallons FLOAT
);


---Integration
CREATE or replace STORAGE INTEGRATION gcs_snowpipe
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://gcs_snowpipe') ;
  
DESC storage integration gcs_snowpipe;
//pasting service account
//koh400000@gcpuscentral1-1dfa.iam.gserviceaccount.com


CREATE or replace  NOTIFICATION INTEGRATION snowpipe_notification
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/janeesh-scrumteam-18jul2022/subscriptions/gcssnowpipe_sub';

DESC integration snowpipe_notification;
//pasting SA
//kph400000@gcpuscentral1-1dfa.iam.gserviceaccount.com

---Creating File Format

CREATE OR REPLACE FILE FORMAT my_csv_file_format
TYPE = CSV
FIELD_DELIMITER = ','
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
SKIP_HEADER = 1;


---Stages
CREATE or replace STAGE mystage
URL='gcs://gcs_snowpipe/'
STORAGE_INTEGRATION = gcs_snowpipe 
file_format = my_file_format;

list @mystage;


----Pipe Creation for Loading Data
CREATE or replace PIPE liquorsales_pipe
AUTO_INGEST = true
INTEGRATION = snowpipe_notification
AS
COPY INTO liquor_sales_src
from @mystage
on_error=CONTINUE;

select SYSTEM$PIPE_STATUS( 'liquorsales_pipe' );


---merge sql stored procedure
CREATE OR REPLACE PROCEDURE rows_inserted()
RETURNS number
LANGUAGE SQL
AS
$$
BEGIN
    merge into liquor_sales_target using liquor_sales_src
    on liquor_sales_src.INVOICE_AND_ITEM_NUMBER=liquor_sales_target.INVOICE_AND_ITEM_NUMBER
    when not matched
    then     insert(invoice_and_item_number,date,store_number,store_name,address,city,zip_code,store_location,county_number,county,category,category_name,vendor_number,vendor_name,item_number,item_description,pack,bottle_volume_ml,state_bottle_cost,state_bottle_retail,bottles_sold,sale_dollars,volume_sold_liters,volume_sold_gallons) values (liquor_sales_src.invoice_and_item_number, liquor_sales_src.date, liquor_sales_src.store_number, liquor_sales_src.store_name, liquor_sales_src.address, liquor_sales_src.city, liquor_sales_src.zip_code, liquor_sales_src.store_location, liquor_sales_src.county_number, liquor_sales_src.county, liquor_sales_src.category, liquor_sales_src.category_name, liquor_sales_src.vendor_number, liquor_sales_src.vendor_name, liquor_sales_src.item_number, liquor_sales_src.item_description, liquor_sales_src.pack, liquor_sales_src.bottle_volume_ml, liquor_sales_src.state_bottle_cost, liquor_sales_src.state_bottle_retail, liquor_sales_src.bottles_sold, liquor_sales_src.sale_dollars,liquor_sales_src.volume_sold_liters, liquor_sales_src.volume_sold_gallons);
    RETURN SQLROWCOUNT;

END
$$
;

call rows_inserted();


---Transformation Procedure
CREATE OR REPLACE PROCEDURE trans_procedure()
RETURNS number
LANGUAGE SQL
as
$$
BEGIN
    create or replace table sales_final as (select store_name,city,sum(sale_dollars) as sales,year(date)as yearly from liquor_sales_target
    group by yearly,store_name,city
    order by yearly,sales desc);

    RETURN 'Success'
END
$$
;

call trans_procedure();

---Stream Procedure

CREATE OR REPLACE PROCEDURE stream_procedure()
RETURNS number
LANGUAGE SQL
as
$$
BEGIN
    insert into liquor_stream_consume select invoice_and_item_number,date,store_number,store_name,address,city,zip_code,store_location,county_number,county,category,category_name,vendor_number,vendor_name,item_number,item_description,pack,bottle_volume_ml,state_bottle_cost,state_bottle_retail,bottles_sold,sale_dollars,volume_sold_liters,volume_sold_gallons from trans_stream;
    RETURN 'Success'
END
$$
;
call stream_procedure();


---Creating Email Stored Procedure


create or replace procedure email_notification()
returns string
language python
runtime_version=3.8
packages = ('snowflake-snowpark-python', 'tabulate')
handler = 'x'
execute as caller
as
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import pandas as pd
def x(session: snowpark.Session): 
    trans=session.sql("call rows_inserted()").collect()
    trans_df=pd.DataFrame(trans)
    trans_df=trans_df.to_html()
    
    trans_1=session.sql("call trans_procedure()").collect()
    trans_2=session.sql("call stream_procedure()").collect()

    
    rec_load_success=session.sql("select file_name,to_varchar(last_load_time,'dd-mm-yyyy hh:mm:ss') as last_load_time,status from SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY where status='Loaded' and last_load_time>dateadd(day,-1,getdate())").to_pandas().to_html(classes='table table-stripped')
    
    rec_load_ploaded=session.sql("select file_name,to_varchar(last_load_time,'dd-mm-yyyy hh:mm:ss') as last_load_time,status,first_error_message from SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY where status='Partially loaded' and last_load_time>dateadd(day,-1,getdate())").to_pandas().to_html()
    
    rec_load_failed=session.sql("select file_name,to_varchar(last_load_time,'dd-mm-yyyy hh:mm:ss') as last_load_time,status,first_error_message from SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY where status='Load failed' and last_load_time>dateadd(day,-1,getdate())").to_pandas().to_html()
    
    final_var='<h3> The number of rows inserted in final_target_table are: <h3>'+ trans_df + '<h3>The following mentioned files got LOADED in last 24 hours <h3>' + rec_load_success+ '<h3>The following mentioned files got PARTIALLY LOADED in the last 24 hours<h3>' + rec_load_ploaded +'<h3>The following mentioned files got FAILED to load in the last 24 hours<h3>' +  rec_load_failed 

    styled_html = final_var.replace('<table ','<table style="border: 2px solid;text-align:center;border-collapse: collapse" class="dataframe"')
    styled_html =  styled_html.replace('<th>','<th style="border: 2px solid;background-color: #00CED1;font-family: Arial, Helvetica, sans-serif;padding:5px">')
    styled_html = styled_html.replace('<td','<td style="border: 2px solid;padding:5px;"')
    styled_html= styled_html.replace('<tr','<tr style="border: 2px solid;text-align: left; background-color: #f2f2f2"')
    styled_html= styled_html + '<h3>Analytics Results:</h3>' + '<a href="https://app.snowflake.com/qtnjtrx/os40346/#/liquor_sales-analytics-d6A1F4mRK">Analytics Dashboard</a>'

    
    
    session.call('system$send_email','my_email_int','ktharunsnowflake@gmail.com,vishsnow123@gmail.com','Email Alert:Status of Pipelines and transformations ',styled_html,'text/html')
           
    return 'Pocedure Executed Successfully'
$$;

call email_notification();



----Creating tasks on merge_sql and annual revenue

create or replace task trans_task 
WAREHOUSE = COMPUTE_WH
SCHEDULE = '3 minute'
when system$stream_has_data('trans_stream')
as
call email_notification()
;

create or replace task final_task
warehouse= compute_wh
after my_task
as
create or replace table sales_final as (select store_name,city,sum(sale_dollars) as sales,year(date)as yearly from liquor_sales_target
group by yearly,store_name,city
order by yearly,sales desc);

create or replace task consume_task
warehouse=compute_wh
after my_task
as
insert into liquor_stream_consume select invoice_and_item_number,date,store_number,store_name,address,city,zip_code,store_location,county_number,county,category,category_name,vendor_number,vendor_name,item_number,item_description,pack,bottle_volume_ml,state_bottle_cost,state_bottle_retail,bottles_sold,sale_dollars,volume_sold_liters,volume_sold_gallons from trans_stream;


alter task trans_task resume;
alter task trans_task suspend;

alter task email_task resume;
alter task email_task suspend;

alter task final_task resume;
alter task final_task suspend;

alter task consume_task resume;
alter task consume_task suspend;





---Integration


CREATE or replace STORAGE INTEGRATION gcs_snowpipe
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = GCS
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://gcs_snowpipe') ;
  
DESC storage integration gcs_snowpipe;
//pasting service account
//koh400000@gcpuscentral1-1dfa.iam.gserviceaccount.com


CREATE or replace  NOTIFICATION INTEGRATION snowpipe_notification
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/janeesh-scrumteam-18jul2022/subscriptions/gcssnowpipe_sub';

DESC integration snowpipe_notification;
//pasting SA
//kph400000@gcpuscentral1-1dfa.iam.gserviceaccount.com

---Creating File Format

CREATE OR REPLACE FILE FORMAT my_csv_file_format
TYPE = CSV
FIELD_DELIMITER = ','
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
SKIP_HEADER = 1;


---Stages
CREATE or replace STAGE mystage
URL='gcs://gcs_snowpipe/'
STORAGE_INTEGRATION = gcs_snowpipe 
file_format = my_file_format;

list @mystage;


----Pipe Creation for Loading Data
CREATE or replace PIPE liquorsales_pipe
AUTO_INGEST = true
INTEGRATION = snowpipe_notification
AS
COPY INTO liquor_sales_src_target
from @mystage
on_error=CONTINUE;

select SYSTEM$PIPE_STATUS( 'liquorsales_pipe' );




















-----Rough Work 

---Merge Sql Inline

merge into liquor_sales_src_target
using liquor_sales_src
on liquor_sales_src.INVOICE_AND_ITEM_NUMBER=liquor_sales_src_target.INVOICE_AND_ITEM_NUMBER
when not matched
then
insert(invoice_and_item_number,date,store_number,store_name,address,city,zip_code,store_location,county_number,county,category,category_name,vendor_number,vendor_name,item_number,item_description,pack,bottle_volume_ml,state_bottle_cost,state_bottle_retail,bottles_sold,sale_dollars,volume_sold_liters,volume_sold_gallons,created_time,updated_time) values (liquor_sales_src.invoice_and_item_number, liquor_sales_src.date, liquor_sales_src.store_number, liquor_sales_src.store_name, liquor_sales_src.address, liquor_sales_src.city, liquor_sales_src.zip_code, liquor_sales_src.store_location, liquor_sales_src.county_number, liquor_sales_src.county, liquor_sales_src.category, liquor_sales_src.category_name, liquor_sales_src.vendor_number, liquor_sales_src.vendor_name, liquor_sales_src.item_number, liquor_sales_src.item_description, liquor_sales_src.pack, liquor_sales_src.bottle_volume_ml, liquor_sales_src.state_bottle_cost, liquor_sales_src.state_bottle_retail, liquor_sales_src.bottles_sold, liquor_sales_src.sale_dollars, liquor_sales_src.volume_sold_liters, liquor_sales_src.volume_sold_gallons)
;


--Annual Revenue Inline
select store_name,city,sum(sale_dollars) as sales,year(date)as yearly from liquor_sales_src_target
group by yearly,store_name,city
order by yearly,sales desc;


create table sales_2019 as(select store_name,sum(sale_dollars)as total_sales ,sum(bottles_sold) as total_bottles_sold from liquor_sales_src
where year(date)=2019
group by store_name
order by total_sales desc);

create table sales_2020 as(select distinct store_name,sum(sale_dollars) as total_sales ,sum(bottles_sold) as total_bottles_sold from liquor_sales_src
where year(date)=2020
group by store_name
order by total_sales desc);

create table sales_2021 (select store_name,sum(sale_dollars) as total_sales ,sum(bottles_sold) as total_bottles_sold from liquor_sales_src
where year(date)=2021
group by store_name
order by total_sales desc);

create table sales_2022 (select store_name,sum(sale_dollars) as total_sales ,sum(bottles_sold) as total_bottles_sold from liquor_sales_src
where year(date)=2022
group by store_name
order by total_sales desc);

create table sales_2023 (select store_name,sum(sale_dollars) as total_sales ,sum(bottles_sold) as total_bottles_sold  from liquor_sales_src
where year(date)=2023
group by store_name
order by total_sales desc);




select store_name,sale_dollars,date,bottles_sold from liquor_sales_src
where store_name='CENTRAL CITY 2';



create or replace function year_results(sale_year number)
returns table(STORE_NAME VARCHAR(16777216),TOTAL_SALES FLOAT,TOTAL_BOTTLES_SOLD NUMBER(38,0))
as 'select store_name,sum(sale_dollars) as total_sales ,sum(bottles_sold) as total_bottles_sold from liquor_sales_src
where year(date)=sale_year
group by store_name
order by total_sales desc';


create table sales_2021 as (select * from table(year_results(2021)));
create table sales_2022 as (select * from table(year_results(2022)));
create table sales_2023 as (select * from table(year_results(2023)));



select * from table(year_results(2021));

year_results(2022);



CREATE or replace NOTIFICATION INTEGRATION my_email_int
    TYPE=EMAIL
    ENABLED=TRUE
    ALLOWED_RECIPIENTS=('ktharunsnowflake@gmail.com','vishsnow123@gmail.com');



create or replace procedure yearly_results(num number)
returns string
language python
runtime_version=3.8
packages = ('snowflake-snowpark-python')
handler = 'x'
execute as caller
as
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col

def x(session: snowpark.Session,num): 

    
    k=session.sql("call merge_sql2() into num").to_pandas().to_markdown()
    session.call('system$send_email','my_email_int','vishsnow123@gmail.com','Email Alert:Status of files Failed', k)
    
    return 'Created Suceessfully'
$$;

call yearly_results(1);

create or replace function merge_sql_r()
returns number
as 'merge into liquor_sales_src_target using liquor_sales_src
    on liquor_sales_src.INVOICE_AND_ITEM_NUMBER=liquor_sales_src_target.INVOICE_AND_ITEM_NUMBER
    when not matched
    then     insert(invoice_and_item_number,date,store_number,store_name,address,city,zip_code,store_location,county_number,county,category,category_name,vendor_number,vendor_name,item_number,item_description,pack,bottle_volume_ml,state_bottle_cost,state_bottle_retail,bottles_sold,sale_dollars,volume_sold_liters,volume_sold_gallons) values (liquor_sales_src.invoice_and_item_number, liquor_sales_src.date, liquor_sales_src.store_number, liquor_sales_src.store_name, liquor_sales_src.address, liquor_sales_src.city, liquor_sales_src.zip_code, liquor_sales_src.store_location, liquor_sales_src.county_number, liquor_sales_src.county, liquor_sales_src.category, liquor_sales_src.category_name, liquor_sales_src.vendor_number, liquor_sales_src.vendor_name, liquor_sales_src.item_number, liquor_sales_src.item_description, liquor_sales_src.pack, liquor_sales_src.bottle_volume_ml, liquor_sales_src.state_bottle_cost, liquor_sales_src.state_bottle_retail, liquor_sales_src.bottles_sold, liquor_sales_src.sale_dollars,liquor_sales_src.volume_sold_liters, liquor_sales_src.volume_sold_gallons)';



create table return_value(str number);

create or replace task my_task_r 
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 minute'
as
call system$set_return_value(call merge_sql())
;

create or replace task child_task_r
warehouse= compute_wh
after my_task
as
insert into return_value values(system$get_predecessor_return_value());



alter task my_task_r resume;
alter task my_task_r suspend;
alter task child_task_r resume;
alter task child_task_r suspend;

////sql procedure mail
create or replace procedure email1()
returns string
language SQL
as
$$
declare 
   res varchar;
   result varchar;
   res1 varchar;
begin
    call merge_sql2() into res;
    res1 := 'no. of rows inserted are';
    result := :res1 || ' ' || :res;
    
   CALL SYSTEM$SEND_EMAIL(
    'My_email_notify',
    'ktharunsnowflake@gmail.com',
    'Email Alert: Email notification for snowpipe.',
     :result
     );
     return 'done';
end;
$$;

call email1();




when matched
then
update
set liquor_sales_src_target.store_name=liquor_sales_src.store_name,liquor_sales_src_target.updated_time=CURRENT_TIMESTAMP()
;


delete from liquor_sales_src_target where invoice_and_item_number='INV-18730800002' ;

delete from liquor_sales_src where invoice_and_item_number='INV-18730800002' ;

select * from liquor_sales_src_target where invoice_and_item_number='INV-18730800002' ;



insert into liquor_sales_src values ('INV-187308000131','2024-04-11','5146','down-apple STORES #153  /  W DES MOINES','329 GRAND AVE','WEST DES MOINES','50265','POINT(-93.70689 41.58269)','77','POLK','1701100.0','TEMPORARY & SPECIALTY PACKAGES',65,'JIM BEAM BRANDS',430,'JIM BEAM BLACK W/2 GLASSES',6,750,15.07,22.61,24,2033.64,18.0,4.75
)



set my_val=(select * from table(result_scan(last_query_id())));

select $my_val






