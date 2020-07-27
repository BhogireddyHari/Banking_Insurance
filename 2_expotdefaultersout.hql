
use insure;

drop table if exists cstpstpenality;

create table cstpstpenality
as 
select 
id,issuerid1,issuerid2,lmt,
case defaulter when 1 then lmt-(lmt*0.04) else lmt end as newlmt ,sex,edu,marital,pay,billamt,
case defaulter when 1 then billamt+(billamt*0.02) else billamt end as newbillamt,defaulter
from  cstpstreorder;

insert overwrite directory '/user/hduser/Banking_Insurance/defaultersout/' row format delimited fields terminated by ',' select * from cstpstpenality where defaulter=1;
insert overwrite directory '/user/hduser/Banking_Insurance/nondefaultersout/' row format delimited fields terminated by ',' select * from cstpstpenality where defaulter=0;