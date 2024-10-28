create or replace table stock.unsold.lotstock
(
  vin varchar(25)
, exterior varchar(50)	
, interior varchar(50)
, manuf_name varchar(25)
, vehicle_type varchar(25)
, make_name varchar(25)
, plant_name varchar(25)
, model_year varchar(25)
, model_name varchar(25)
, desc1 varchar(25)
, desc2 varchar(25)
, desc3 varchar(25)
, desc4 varchar(25)
, desc5 varchar(25)
, engine varchar(25)
, drive_type varchar(25)
, transmission varchar(25)
, mpg varchar(25)
);

list @stock.unsold.aws_s3_bucket;

-- Fill in the rest of the file name by looking at the files in your new stage
-- Replace the question marks with the file name (remember AWS is case sensitive)
select $1, $2, $3
from @stock.unsold.aws_s3_bucket/Lotties_LotStock_Data.csv;


CREATE FILE FORMAT stock.unsold.CSV_COMMA_LF_HEADER 
type = 'CSV' 
field_delimiter = ',' 
record_delimiter = '\n' 
skip_header = 1 
;

select $1 as VIN
, $2 as Exterior, $3 as Interior
from @stock.unsold.aws_s3_bucket/Lotties_LotStock_Data.csv
(file_format => stock.unsold.csv_comma_lf_header);



-- This file format will allow the 3 column file to be loaded into an 18 column table
-- By parsing the header, Snowflake can infer the column names
CREATE FILE FORMAT stock.unsold.CSV_COL_COUNT_DIFF 
type = 'CSV' 
field_delimiter = ',' 
record_delimiter = '\n' 
field_optionally_enclosed_by = '"'
trim_space = TRUE
error_on_column_count_mismatch = FALSE
parse_header = TRUE;

copy into stock.unsold.lotstock
from @stock.unsold.aws_s3_bucket/Lotties_LotStock_Data.csv
file_format = (format_name = stock.unsold.csv_col_count_diff)
match_by_column_name='CASE_INSENSITIVE';



SELECT * from stock.unsold.lotstock;



select * 
from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN('5UXCR6C0XL9C77256'));




--A simple select from Lot Stock (choose any VIN from the LotStock table)
select * 
from stock.unsold.lotstock
where vin = '5J8YD4H86LL013641';

-- here we use ls for lotstock table and pf for parse function
-- this more complete statement lets us combine the data already in the table 
-- with the data returned from the parse function
select ls.vin, ls.exterior, ls.interior, pf.*
from
(select * 
from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN('5J8YD4H86LL013641'))
) pf
join stock.unsold.lotstock ls
where pf.vin = ls.vin
;



-- We can use a local (session) variable to make it easier to change the VIN we are trying to enhance


select $my_vin;
select ls.vin, pf.manuf_name, pf.vehicle_type
        , pf.make_name, pf.plant_name, pf.model_year
        , pf.desc1, pf.desc2, pf.desc3, pf.desc4, pf.desc5
        , pf.engine, pf.drive_type, pf.transmission, pf.mpg
from stock.unsold.lotstock ls
join 
    (   select 
          vin, manuf_name, vehicle_type
        , make_name, plant_name, model_year
        , desc1, desc2, desc3, desc4, desc5
        , engine, drive_type, transmission, mpg
        from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN($my_vin))
    ) pf
on pf.vin = ls.vin;


set my_vin = '19UUB2F35LA005011';

update stock.unsold.lotstock t
set manuf_name = s.manuf_name
, vehicle_type = s.vehicle_type
, make_name = s.make_name
, plant_name = s.plant_name
, model_year = s.model_year
, desc1 = s.desc1
, desc2 = s.desc2
, desc3 = s.desc3
, desc4 = s.desc4
, desc5 = s.desc5
, engine = s.engine
, drive_type = s.drive_type
, transmission = s.transmission
, mpg = s.mpg
from 
(
    select ls.vin, pf.manuf_name, pf.vehicle_type
        , pf.make_name, pf.plant_name, pf.model_year
        , pf.desc1, pf.desc2, pf.desc3, pf.desc4, pf.desc5
        , pf.engine, pf.drive_type, pf.transmission, pf.mpg
    from stock.unsold.lotstock ls
    join 
    (   select 
          vin, manuf_name, vehicle_type
        , make_name, plant_name, model_year
        , desc1, desc2, desc3, desc4, desc5
        , engine, drive_type, transmission, mpg
        from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN($my_vin))
    ) pf
    on pf.vin = ls.vin
) s
where t8.vin = s.vin;

select * from stock.unsold.lotstock where MANUF_NAME is not null;

set row_count = (select count(*) 
                from stock.unsold.lotstock
                where manuf_name is null);

select $row_count;




-- This scripting block runs very slow, but it shows how blocks work for people who are new to using them
DECLARE
    update_stmt varchar(2000);
    res RESULTSET;
    cur CURSOR FOR select vin from stock.unsold.lotstock where manuf_name is null;
BEGIN
    OPEN cur;
    FOR each_row IN cur DO
        update_stmt := 'update stock.unsold.lotstock t '||
            'set manuf_name = s.manuf_name ' ||
            ', vehicle_type = s.vehicle_type ' ||
            ', make_name = s.make_name ' ||
            ', plant_name = s.plant_name ' ||
            ', model_year = s.model_year ' ||
            ', desc1 = s.desc1 ' ||
            ', desc2 = s.desc2 ' ||
            ', desc3 = s.desc3 ' ||
            ', desc4 = s.desc4 ' ||
            ', desc5 = s.desc5 ' ||
            ', engine = s.engine ' ||
            ', drive_type = s.drive_type ' ||
            ', transmission = s.transmission ' ||
            ', mpg = s.mpg ' ||
            'from ' ||
            '(       select ls.vin, pf.manuf_name, pf.vehicle_type ' ||
                    ', pf.make_name, pf.plant_name, pf.model_year ' ||
                    ', pf.desc1, pf.desc2, pf.desc3, pf.desc4, pf.desc5 ' ||
                    ', pf.engine, pf.drive_type, pf.transmission, pf.mpg ' ||
                'from stock.unsold.lotstock ls ' ||
                'join ' ||
                '(   select' || 
                '     vin, manuf_name, vehicle_type' ||
                '    , make_name, plant_name, model_year ' ||
                '    , desc1, desc2, desc3, desc4, desc5 ' ||
                '    , engine, drive_type, transmission, mpg ' ||
                '    from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN(\'' ||
                  each_row.vin || '\')) ' ||
                ') pf ' ||
                'on pf.vin = ls.vin ' ||
            ') s ' ||
            'where t.vin = s.vin;';
        res := (EXECUTE IMMEDIATE :update_stmt);
    END FOR;
    CLOSE cur;   
END;


use role accountadmin;

create or replace api integration dora_api_integration
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
enabled = true
api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

use role accountadmin;  

create or replace external function util_db.public.grader(
      step varchar
    , passed boolean
    , actual integer
    , expected integer
    , description varchar)
returns variant
api_integration = dora_api_integration 
context_headers = (current_timestamp, current_account, current_statement, current_account_name) 
as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'
; 


use role accountadmin;
use database util_db; 
use schema public; 

select grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 


-- set your worksheet drop lists to the location of your GRADER function
--DO NOT EDIT ANYTHING BELOW THIS LINE

--THIS DORA CHECK MUST BE RUN IN THE ACME ACCOUNT!!!!!
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'CMCW10' as step
 ,( select count(*)
    from snowflake.account_usage.databases
    where (database_name in ('WEATHERSOURCE','INTERNATIONAL_CURRENCIES')
           and type = 'IMPORTED DATABASE'
           and deleted is null)
    or (database_name = 'MARKETING'
          and type = 'STANDARD'
          and deleted is null)
   ) as actual
 , 3 as expected
 ,'ACME Account Set up nicely' as description
);


-- set the worksheet drop lists to match the location of your GRADER function
--DO NOT MAKE ANY CHANGES BELOW THIS LINE
--RUN THIS DORA CHECK IN YOUR ACME ACCOUNT

select grader(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 
  'CMCW11' as step
 ,( select count(*) 
   from MARKETING.MAILERS.DETROIT_ZIPS) as actual
 , 9 as expected
 ,'Detroit Zips' as description
); 





-- set the worksheet drop lists to match the location of your GRADER function
--DO NOT MAKE ANY CHANGES BELOW THIS LINE

--RUN THIS DORA CHECK IN YOUR ACME ACCOUNT

select grader(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 
  'CMCW14' as step
 ,( select count(*) 
   from STOCK.UNSOLD.LOTSTOCK
   where engine like '%.5 L%'
   or plant_name like '%z, Sty%'
   or desc2 like '%xDr%') as actual
 , 145 as expected
 ,'Intentionally cryptic test' as description
); 


// How many of my deliveries will be delayed due to snowfall?
/*
When it snows in excess of six inches per day, my company experiences delivery delays. How many of my deliveries were impacted during the third week of January for the previous year?
*/
WITH timestamps AS
(   
    SELECT
        DATE_TRUNC(year,DATEADD(year,-1,CURRENT_DATE())) AS ref_timestamp,
        LAST_DAY(DATEADD(week,2 + CAST(WEEKISO(ref_timestamp) != 1 AS INTEGER),ref_timestamp),week) AS end_week,
        DATEADD(day, day_num - 7, end_week) AS date_valid_std
    FROM
    (   
        SELECT
            ROW_NUMBER() OVER (ORDER BY SEQ1()) AS day_num
        FROM
            TABLE(GENERATOR(rowcount => 7))
    ) 
)
SELECT
    country,
    postal_code,
    date_valid_std,
    tot_snowfall_in 
FROM 
    standard_tile.history_day
NATURAL INNER JOIN
    timestamps
WHERE
    country='US' AND
    tot_snowfall_in > 6.0 
ORDER BY 
    postal_code,date_valid_std
;

// Determine if an event will be impacted by rain.
/*
I am hosting an outdoor event in seven days. How can I utilize your forecast data to determine if my event will be impacted by rain?
*/
SELECT COUNTRY,DATE_VALID_STD, POSTAL_CODE, DATEDIFF(day,current_date(),DATE_VALID_STD) AS DAY, HOUR(TIME_INIT_UTC) AS HOUR, TOT_PRECIPITATION_IN FROM STANDARD_TILE.FORECAST_DAY WHERE POSTAL_CODE='32333' AND DAY=7;

// Use temperature data to create sales forecast.
/*
Our company sells 70% more product when the temperature is in excess of 80 degrees and I am trying to create a product sales forecast for this upcoming July. How can we use your climatology data to quickly ascertain how many days “normally” exceed 80 degrees during the month of July?
*/
SELECT COUNTRY, POSTAL_CODE, SUM(IFF(AVG_OF__DAILY_MAX_TEMPERATURE_AIR_F>80, 1, 0)) DaysAbove80 FROM STANDARD_TILE.CLIMATOLOGY_DAY WHERE DOY_STD>=182 AND DOY_STD<=212 AND COUNTRY='US' GROUP BY COUNTRY,POSTAL_CODE ORDER BY DaysAbove80 DESC, COUNTRY, POSTAL_CODE;

//  Can my restaurant use weather to determine the amount of footfall traffic that we will have in the next week?
/*
Our restaurant has a significant amount of outdoor dining space. We need to determine staffing and demand based on the forecasted weather for next week.
*/
SELECT
    postal_code,
    country,
    date_valid_std,
    avg_temperature_air_2m_f,
    avg_humidity_relative_2m_pct,
    avg_wind_speed_10m_mph,
    tot_precipitation_in,
    tot_snowfall_in,
    avg_cloud_cover_tot_pct,
    probability_of_precipitation_pct,
    probability_of_snow_pct
FROM
(
    SELECT
        postal_code,
        country,
        date_valid_std,
        avg_temperature_air_2m_f,
        avg_humidity_relative_2m_pct,
        avg_wind_speed_10m_mph,
        tot_precipitation_in,
        tot_snowfall_in,
        avg_cloud_cover_tot_pct,
        probability_of_precipitation_pct,
        probability_of_snow_pct,
        DATEADD(DAY,1,CURRENT_DATE()) AS skip_date,
        DATEADD(DAY,7 - DAYOFWEEKISO(skip_date),skip_date) AS next_sunday
    FROM
        standard_tile.forecast_day
)
WHERE
    date_valid_std BETWEEN next_sunday AND DATEADD(DAY,6,next_sunday)
ORDER BY
    date_valid_std
;


alter database global_weather__climate_data_for_bi
rename to WEATHERSOURCE;

select distinct country from history_day;

select distinct postal_code from history_day where country='US' and postal_code like '481%' or postal_code like '482%';


create database MARKETING;
create schema MARKETING.MAILERS;

create view DETROIT_ZIPS (postal_code) as select distinct postal_code from WEATHERSOURCE.STANDARD_TILE.HISTORY_DAY where country='US' and postal_code like '481%' or postal_code like '482%';


select row_count
from weathersource.INFORMATION_SCHEMA.TABLES 
where table_schema='standard_tile'
and table_name= 'history_day';

select count(*) from weathersource.standard_tile.history_day;

select count(*) from weathersource.standard_tile.history_day tbl1 inner join
marketing.mailers.detroit_zips tbl2 on tbl1.postal_code=tbl2.postal_code;

select min(date_valid_std),max(date_valid_std) from weathersource.standard_tile.history_day;

select min(date_valid_std),max(date_valid_std) from weathersource.standard_tile.forecast_day;

select date_valid_std,avg(AVG_CLOUD_COVER_TOT_PCT) from weathersource.standard_tile.forecast_day tbl1 inner join
marketing.mailers.detroit_zips tbl2 on tbl1.postal_code=tbl2.postal_code group by 1 order by 2;


alter view intl_db.public.NATIONS_SAMPLE_PLUS_ISO
set secure; 

alter view intl_db.public.SIMPLE_CURRENCY
set secure;



grant usage 
on function UTIL_DB.PUBLIC.GRADER(VARCHAR, BOOLEAN, NUMBER, NUMBER, VARCHAR) 
to SYSADMIN;

select GRADER(step,(actual = expected), actual, expected, description) as graded_results from (
SELECT 'DORA_IS_WORKING' as step
 ,(select 223 ) as actual
 ,223 as expected
 ,'Dora is working!' as description
); 






-- YOU WILL NEED TO USE ACCOUNTADMIN ROLE on this test.
--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from( 
 SELECT 'CMCW01' as step
 ,( select count(*) 
   from snowflake.account_usage.databases
   where database_name = 'INTL_DB' 
   and deleted is null) as actual
 , 1 as expected
 ,'Created INTL_DB' as description
 );



 -- set your worksheet drop lists to the location of your GRADER function
-- role can be set to either SYSADMIN or ACCOUNTADMIN for this check

--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW02' as step
 ,( select count(*) 
   from INTL_DB.INFORMATION_SCHEMA.TABLES 
   where table_schema = 'PUBLIC' 
   and table_name = 'INT_STDS_ORG_3166') as actual
 , 1 as expected
 ,'ISO table created' as description
);




-- set your worksheet drop lists to the location of your GRADER function 
-- either role can be used

-- DO NOT EDIT BELOW THIS LINE 
select grader(step, (actual = expected), actual, expected, description) as graded_results from( 
SELECT 'CMCW03' as step 
 ,(select row_count 
   from INTL_DB.INFORMATION_SCHEMA.TABLES  
   where table_name = 'INT_STDS_ORG_3166') as actual 
 , 249 as expected 
 ,'ISO Table Loaded' as description 
); 




-- SET YOUR DROPLISTS PRIOR TO RUNNING THE CODE BELOW 
--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW04' as step
 ,( select count(*) 
   from INTL_DB.PUBLIC.NATIONS_SAMPLE_PLUS_ISO) as actual
 , 249 as expected
 ,'Nations Sample Plus Iso' as description
);



-- set your worksheet drop lists

--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW05' as step
 ,(select row_count 
  from INTL_DB.INFORMATION_SCHEMA.TABLES 
  where table_schema = 'PUBLIC' 
  and table_name = 'COUNTRY_CODE_TO_CURRENCY_CODE') as actual
 , 265 as expected
 ,'CCTCC Table Loaded' as description
);



-- set your worksheet context menus
--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
SELECT 'CMCW06' as step
 ,(select row_count 
  from INTL_DB.INFORMATION_SCHEMA.TABLES 
  where table_schema = 'PUBLIC' 
  and table_name = 'CURRENCIES') as actual
 , 151 as expected
 ,'Currencies table loaded' as description
);



-- don't forget your droplists

--DO NOT EDIT BELOW THIS LINE
select grader(step, (actual = expected), actual, expected, description) as graded_results from(
 SELECT 'CMCW07' as step 
,( select count(*) 
  from INTL_DB.PUBLIC.SIMPLE_CURRENCY ) as actual
, 265 as expected
,'Simple Currency Looks Good' as description
);


-- set your worksheet drop lists to the location of your GRADER function
--DO NOT EDIT ANYTHING BELOW THIS LINE

--This DORA Check Requires that you RUN two Statements, one right after the other
show shares in account;

--the above command puts information into memory that can be accessed using result_scan(last_query_id())
-- If you have to run this check more than once, always run the SHOW command immediately prior
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'CMCW08' as step
 ,( select IFF(count(*)>0,1,0) 
    from table(result_scan(last_query_id())) 
    where "kind" = 'OUTBOUND'
    and "database_name" = 'INTL_DB') as actual
 , 1 as expected
 ,'Outbound Share Created From INTL_DB' as description
); 


-- set your worksheet drop lists to the location of your GRADER function
--DO NOT EDIT ANYTHING BELOW THIS LINE

--This DORA Check Requires that you RUN two Statements, one right after the other
show resource monitors in account;

--the above command puts information into memory that can be accessed using result_scan(last_query_id())
-- If you have to run this check more than once, always run the SHOW command immediately prior
select grader(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'CMCW09' as step
 ,( select IFF(count(*)>0,1,0) 
    from table(result_scan(last_query_id())) 
    where "name" = 'DAILY_3_CREDIT_LIMIT'
    and "credit_quota" = 3
    and "frequency" = 'DAILY') as actual
 , 1 as expected
 ,'Resource Monitors Exist' as description
); 

---------------------------------------------------------------------------------------------------------------


-- set the worksheet drop lists to match the location of your GRADER function
--DO NOT MAKE ANY CHANGES BELOW THIS LINE

--RUN THIS DORA CHECK IN YOUR ORIGINAL TRIAL ACCOUNT
select grader(step, (actual = expected), actual, expected, description) as graded_results from ( SELECT 'CMCW12' as step ,( select count(*) from SNOWFLAKE.ORGANIZATION_USAGE.ACCOUNTS where account_name = 'ACME' 
 and region like 'AZURE_%' and deleted_on is null) as actual , 1 as expected ,'ACME Account Added on Azure Platform' as description ); 


 

 -- set the worksheet drop lists to match the location of your GRADER function
--DO NOT MAKE ANY CHANGES BELOW THIS LINE
--RUN THIS DORA CHECK IN YOUR ORIGINAL TRIAL ACCOUNT

select grader(step, (actual = expected), actual, expected, description) as graded_results from (
SELECT 
  'CMCW13' as step
 ,( select count(*) 
   from SNOWFLAKE.ORGANIZATION_USAGE.ACCOUNTS 
   where account_name = 'AUTO_DATA_UNLIMITED' 
   and region like 'GCP_%'
   and deleted_on is null) as actual
 , 1 as expected
 ,'ADU Account Added on GCP' as description
); 





alter database LEARNING_SAMPLE_DATA
rename to snowflake_sample_data;

grant imported privileges
on database SNOWFLAKE_SAMPLE_DATA
to role SYSADMIN;




--Check the range of values in the Market Segment Column
SELECT DISTINCT c_mktsegment
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

--Find out which Market Segments have the most customers
SELECT c_mktsegment, COUNT(*)
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
GROUP BY c_mktsegment
ORDER BY COUNT(*);





-- Nations Table
SELECT N_NATIONKEY, N_NAME, N_REGIONKEY
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

-- Regions Table
SELECT R_REGIONKEY, R_NAME
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

-- Join the Tables and Sort
SELECT R_NAME as Region, N_NAME as Nation
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
ORDER BY R_NAME, N_NAME ASC;

--Group and Count Rows Per Region
SELECT R_NAME as Region, count(N_NAME) as NUM_COUNTRIES
FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
JOIN SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
ON N_REGIONKEY = R_REGIONKEY
GROUP BY R_NAME;

-- where did you put the function?
show user functions in account;

-- did you put it here?
select * 
from util_db.information_schema.functions
where function_name = 'GRADER'
and function_catalog = 'UTIL_DB'
and function_owner = 'ACCOUNTADMIN';



use role SYSADMIN;
create database INTL_DB;
use schema INTL_DB.PUBLIC;



use role SYSADMIN;

create warehouse INTL_WH 
with 
warehouse_size = 'XSMALL' 
warehouse_type = 'STANDARD' 
auto_suspend = 600 --600 seconds/10 mins
auto_resume = TRUE;

use warehouse INTL_WH;




create or replace table intl_db.public.INT_STDS_ORG_3166 
(iso_country_name varchar(100), 
 country_name_official varchar(200), 
 sovreignty varchar(40), 
 alpha_code_2digit varchar(2), 
 alpha_code_3digit varchar(3), 
 numeric_country_code integer,
 iso_subdivision varchar(15), 
 internet_domain_code varchar(10)
);




create or replace file format util_db.public.PIPE_DBLQUOTE_HEADER_CR 
  type = 'CSV' --use CSV for any flat file
  compression = 'AUTO' 
  field_delimiter = '|' --pipe or vertical bar
  record_delimiter = '\r' --carriage return
  skip_header = 1  --1 header row
  field_optionally_enclosed_by = '\042'  --double quotes
  trim_space = FALSE;




  create stage util_db.public.aws_s3_bucket url = 's3://uni-cmcw';
  list @util_db.public.aws_s3_bucket;


copy into INTL_DB.PUBLIC.INT_STDS_ORG_3166
from @UTIL_DB.PUBLIC.AWS_S3_BUCKET
files = ( 'ISO_Countries_UTF8_pipe.csv')
file_format = ( format_name='util_db.public.PIPE_DBLQUOTE_HEADER_CR' );




select count(*) as found, '249' as expected 
from INTL_DB.PUBLIC.INT_STDS_ORG_3166;


select count(*) as OBJECTS_FOUND
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';



select row_count
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3166';


create view intl_db.public.NATIONS_SAMPLE_PLUS_ISO 
( iso_country_name
  ,country_name_official
  ,alpha_code_2digit
  ,region) AS
select  
     iso_country_name
    ,country_name_official,alpha_code_2digit
    ,r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3166 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)= n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey;

select *
from intl_db.public.NATIONS_SAMPLE_PLUS_ISO;




create table intl_db.public.CURRENCIES 
(
  currency_ID integer, 
  currency_char_code varchar(3), 
  currency_symbol varchar(4), 
  currency_digital_code varchar(3), 
  currency_digital_name varchar(30)
)
  comment = 'Information about currencies including character codes, symbols, digital codes, etc.';




create table intl_db.public.COUNTRY_CODE_TO_CURRENCY_CODE 
  (
    country_char_code varchar(3), 
    country_numeric_code integer, 
    country_name varchar(100), 
    currency_name varchar(100), 
    currency_char_code varchar(3), 
    currency_numeric_code integer
  ) 
  comment = 'Mapping table currencies to countries';




 create file format util_db.public.CSV_COMMA_LF_HEADER
  type = 'CSV' 
  field_delimiter = ',' 
  record_delimiter = '\n' -- the n represents a Line Feed character
  skip_header = 1 
;

list @util_db.public.aws_s3_bucket;

copy into INTL_DB.PUBLIC.CURRENCIES
from @UTIL_DB.PUBLIC.AWS_S3_BUCKET
files = ( 'currencies.csv')
file_format = ( format_name='util_db.public.CSV_COMMA_LF_HEADER' );

copy into INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE
from @UTIL_DB.PUBLIC.AWS_S3_BUCKET
files = ( 'country_code_to_currency_code.csv')
file_format = ( format_name='util_db.public.CSV_COMMA_LF_HEADER' );


select * from country_code_to_currency_code;

create or replace view simple_currency (
cty_code,
cur_code
) as select COUNTRY_CHAR_CODE as cty_code,
            CURRENCY_CHAR_CODE as cur_code 
            from country_code_to_currency_code;


select * from simple_currency;





--------------------------------------------------------------------------------------------------------
use role accountadmin;

create or replace api integration dora_api_integration
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
enabled = true
api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

use role accountadmin;  

create or replace external function util_db.public.grader(
      step varchar
    , passed boolean
    , actual integer
    , expected integer
    , description varchar)
returns variant
api_integration = dora_api_integration 
context_headers = (current_timestamp, current_account, current_statement, current_account_name) 
as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader'
; 


use role accountadmin;
use database util_db; 
use schema public; 

select grader(step, (actual = expected), actual, expected, description) as graded_results from
(SELECT 
 'DORA_IS_WORKING' as step
 ,(select 123) as actual
 ,123 as expected
 ,'Dora is working!' as description
); 
--------------------------------------------------------------------------------------------------------------