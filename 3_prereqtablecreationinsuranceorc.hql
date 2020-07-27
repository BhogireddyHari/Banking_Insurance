
use insure;

drop table if exists insurance;
CREATE TABLE insurance (IssuerId1 int,IssuerId2 int,BusinessYear int,StateCode string,SourceName string,NetworkName string,NetworkURL string,RowNumber int,MarketCoverage string,DentalOnlyPlan string)
row format delimited fields terminated by ','
TBLPROPERTIES ("skip.header.line.count"="1");
load data local inpath '/home/hduser/Banking_Insurance/source/insuranceinfo.csv' into table insurance;

drop table if exists state_master;
CREATE EXTERNAL TABLE state_master (statecd STRING, statedesc STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
WITH SERDEPROPERTIES ("input.regex" = "(.{2})(.{20})" )
LOCATION '/user/hduser/source/states';
load data local inpath '/home/hduser/Banking_Insurance/source/states_fixedwidth' overwrite into table state_master;

drop table if exists defaulters;

CREATE TABLE defaulters (id int,IssuerId1 int,IssuerId2 int,lmt int,newlmt double,sex int,edu int,marital int,pay int,billamt int,newbillamt int,defaulter int)
row format delimited fields terminated by ','
LOCATION '/user/hduser/Banking_Insurance/defaultersout';	