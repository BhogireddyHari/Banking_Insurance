Logfile=/home/hduser/Banking_Insurance/execution.out
Logfileerror=/home/hduser/Banking_Insurance/executionerror.out
hivelog=/home/hduser/Banking_Insurance/hivelog.out
echo $(date) > $Logfile
echo $(date +"%y/%m/%d %H:%M:%S")"# Script execution started"
echo $(date +"%y/%m/%d %H:%M:%S")"# Mysql Tables creation started" >> $Logfile
mysql -u root -proot -D custdb -e "create database if not exists custdb;
use custdb;
drop table if exists credits_pst;
drop table if exists credits_cst;
drop table if exists custmaster;
create table if not exists credits_pst (id integer,lmt integer,sex integer,edu integer,marital integer,age integer,pay integer,billamt integer,defaulter integer,issuerid1 integer,issuerid2 integer,tz varchar(3));
create table if not exists credits_cst (id integer,lmt integer,sex integer,edu integer,marital integer,age integer,pay integer,billamt integer,defaulter integer,issuerid1 integer,issuerid2 integer,tz varchar(3));
create table if not exists custmaster (id integer,fname varchar(100),lname varchar(100),ageval integer,profession varchar(100));
source /home/hduser/Banking_Insurance/Source/2_2_creditcard_defaulters_pst
source /home/hduser/Banking_Insurance/Source/2_creditcard_defaulters_cst
source /home/hduser/Banking_Insurance/Source/custmaster
">> $Logfile
echo $(date +"%y/%m/%d %H:%M:%S")"# Mysql Tables creation credits_pst,credits_cst,custmaster if exists old one dropped"
echo $(date +"%y/%m/%d %H:%M:%S")"# Mysql Tables data population Completed."
mysql -u root -proot -D custdb -e  "use custdb; select count(1) credits_pst_count from credits_pst;
select count(1) credits_cst_count from credits_cst;
select count(1) custmaster_count from custmaster;"
echo $(date +"%y/%m/%d %H:%M:%S")"#Sqoop Command Data Import Started for credits_pst"
sqoop import --connect jdbc:mysql://inceptez/custdb --username root --password root --table credits_pst --delete-target-dir --target-dir /user/hduser/Banking_Insurance/credits_pst/ --m 1 2>$Logfileerror
credits_pst_cnt=$(hadoop fs -cat /user/hduser/Banking_Insurance/credits_pst/* | wc -l)
#echo "#Sqoop Data Import credits_cst file count $credits_pst_cnt"
#bash /home/hduser/Banking_Insurance/sqoop_import_call.sh >> $Logfile
echo $(date +"%y/%m/%d %H:%M:%S")"#Sqoop Command Data Import Completed for credits_pst count $credits_pst_cnt"
echo $(date +"%y/%m/%d %H:%M:%S")"#Sqoop Command Data Import Started for credits_cst"
sqoop import --connect jdbc:mysql://inceptez/custdb --username root --password root --table credits_cst --delete-target-dir --target-dir /user/hduser/Banking_Insurance/credits_cst/ --m 1 2>>$Logfileerror
credits_cst_cnt=$(hadoop fs -cat /user/hduser/Banking_Insurance/credits_cst/* | wc -l)
#echo "#Sqoop Data Import credits_cst file count $credits_cst_cnt"
#bash /home/hduser/Banking_Insurance/sqoop_import_call.sh >> $Logfile
echo $(date +"%y/%m/%d %H:%M:%S")"#Sqoop Command Data Import Completed for credits_pst count $credits_cst_cnt"
echo $(date +"%y/%m/%d %H:%M:%S")"# Deleting folders defaultersout and nondefaultersout"
hadoop fs -rmr -f /user/hduser/Banking_Insurance/defaultersout/
hadoop fs -rmr -f /user/hduser/Banking_Insurance/nondefaultersout/
echo $(date +"%y/%m/%d %H:%M:%S")"# Deletion Completed folders defaultersout and nondefaultersout"
echo $(date +"%y/%m/%d %H:%M:%S") "#Create cst and pst temporary tables and loade both tables date into cstpstreorder table where billamt > 0"
hive -f /home/hduser/Banking_Insurance/1_tempdataload.hql &>$hivelog
cstpstreorder_count=$(hive -e"select count(1) cstpstreorder_count from insure.cstpstreorder")
echo $(date +"%y/%m/%d %H:%M:%S") "#data load completed total records inserted in cstpstreorder $cstpstreorder_count"
echo $(date +"%y/%m/%d %H:%M:%S") "#create cstpstpenality and export data to defaultersout and nondefaultersout folders"
hive -f /home/hduser/Banking_Insurance/2_expotdefaultersout.hql &>>$hivelog
defaultersout_cnt=$(hadoop fs -cat /user/hduser/Banking_Insurance/defaultersout/* | wc -l)
nondefaultersout_cnt=$(hadoop fs -cat /user/hduser/Banking_Insurance/nondefaultersout/* | wc -l)
echo $(date +"%y/%m/%d %H:%M:%S") "#data export completed total records defaultersout $defaultersout_cnt and nondefaultersout   $nondefaultersout_cnt"
echo $(date +"%y/%m/%d %H:%M:%S") "#creating new tables insurance,state_master, and defaulters tables are dependency for insuranceorc"
hive -f /home/hduser/Banking_Insurance/3_prereqtablecreationinsuranceorc.hql &>>$hivelog
echo $(date +"%y/%m/%d %H:%M:%S") "#table creation and data load completed"
echo $(date +"%y/%m/%d %H:%M:%S") "#creating new insuranceorc tables and loading data using insurance,state_master, and defaulters tables with joins"
hive -f /home/hduser/Banking_Insurance/4_insuranceorcdataload.hql &>>$hivelog
insuranceorc_count=$(hive -e"select count(1) insuranceorc_count from insure.insuranceorc")
echo $(date +"%y/%m/%d %H:%M:%S") "#insuranceorc data load completed record count $insuranceorc_count"
echo $(date +"%y/%m/%d %H:%M:%S") "#Maximum panalty check started"
penality_max=$(hive -e"
with T1 as ( select max(penality) as penalitymale from insure.insuranceorc where sex='male'),
T2 as ( select max(penality) as penalityfemale from insureBanking_Insurance.insuranceorc where sex='female')
select penalitymale,penalityfemale
from T1 inner join T2
ON 1=1;")
echo $(date +"%y/%m/%d %H:%M:%S") "#Maximum panalty for male and female $penality_max)
echo $(date +"%y/%m/%d %H:%M:%S") "creating view middlegradeview on top of insuranceorc where grade='middle grade'and issuerid is not null;"
hive -f /home/hduser/Banking_Insurance/5_middlegradeview.hql &>>$hivelog
middlegrademaskedout_cnt=$(hadoop fs -cat /user/hduser/Banking_Insurance/middlegrademaskedout/* | wc -l)
echo $(date +"%y/%m/%d %H:%M:%S") "view creation completed and overwrite directory '/user/hduser/Banking_Insurance/middlegrademaskedout' count $middlegrademaskedout_cnt"