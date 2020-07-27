create database if not exists insure;

use insure;

create temporary table cst
(id int,lmt int,sex int,edu int,marital int,age int,pay int,billamt int,defaulter int,issuerid1 int,issuerid2 int,tz varchar(3))
row format delimited fields terminated by ','
TBLPROPERTIES ("skip.header.line.count"="1");

load data inpath '/user/hduser/Banking_Insurance/credits_cst/' into table cst;

create temporary table pst (id int,lmt int,sex int,edu int,marital int,age int,pay int,billamt int,defaulter int,issuerid1 int,issuerid2 int,tz varchar(3))
row format delimited fields terminated by ','
TBLPROPERTIES ("skip.header.line.count"="1");

load data inpath '/user/hduser/Banking_Insurance/credits_pst/' into table pst;

Drop table if exists cstpstreorder;

create table cstpstreorder
as
select * from (
select * from pst
union
select * from cst) CPC where billamt > 0;