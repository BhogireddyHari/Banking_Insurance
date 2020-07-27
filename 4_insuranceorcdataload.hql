use insure;
drop table if exists insuranceorc;

CREATE TABLE insuranceorc (IssuerId int,BusinessYear int,StateCode string,statedesc string,SourceName string,NetworkName string,NetworkURL string,RowNumber int,MarketCoverage string,DentalOnlyPlan string,id int,lmt int,newlmt int,reduced_lmt int,sex varchar(6),grade varchar(20),marital int,pay int,billamt int,newbillamt int,penality int,defaulter int)
row format delimited fields terminated by ','
stored as orc
LOCATION '/user/hduser/Banking_Insurance/insuranceorc'
TBLPROPERTIES ("orc.compress"="SNAPPY");
insert into table insuranceorc select distinct concat(i.IssuerId1,i.IssuerId2),i.businessyear,i.statecode,s.statedesc as statedesc,i.sourcename,i.networkname,i.networkurl,i.rownumber,i.marketcoverage,i.dentalonlyplan,
d.id,d.lmt,d.newlmt,d.newlmt-d.lmt as reduced_lmt,case when d.sex=1 then 'male' else 'female' end as sex ,case when d.edu=1 then 'lower grade' when d.edu=2 then 'lower middle grade' when d.edu=3 then 'middle grade' when d.edu=4 then 'higher grade' when d.edu=5 then 'doctrate grade' end as grade ,d.marital,d.pay,d.billamt,d.newbillamt,d.newbillamt-d.billamt as penality,d.defaulter
from insurance i inner join defaulters d
on (i.IssuerId1=d.IssuerId1 and i.IssuerId2=d.IssuerId2)
inner join state_master s
on (i.statecode=s.statecd)
and concat(i.IssuerId1,i.IssuerId2) is not null;


alter table insuranceorc SET TBLPROPERTIES('EXTERNAL'='TRUE');
