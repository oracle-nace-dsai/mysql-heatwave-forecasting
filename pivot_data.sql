#pivot_data.sql
#by Joe Hahn
#joe.hahn@oracle.come
#September 21, 2023

#pivot weekly crime data so that a forecast model can be trained

#use crimes  table in Chicago database
use Chicago;

#compute count daily thefts, narcotics, and weapons summed across all chicago Wards, from the crimes_filtered_sub table 
drop table if exists crimes_daily;
create table crimes_daily as
    select Date, cast(max(Year) as char) as Year, max(Week) as Week, cast(max(weekday(Date)) as char) as Weekday, Primary_Type, count(ID) as N from crimes_filtered_sub 
        group by Date, Primary_Type order by Date, Primary_Type;
select * from crimes_daily order by rand() limit 5;
desc crimes_daily;

#manually pivot the above to get crime counts versus date
drop table if exists TTV_pivot;
create table TTV_pivot as
    select T.Date, T.Year, T.Week, T.Weekday, T.N as THEFT, N.N as NARCOTICS, W.N as WEAPONS_VIOLATION from 
        (select * from crimes_daily where (Primary_Type='THEFT')) T
        inner join
        (select * from crimes_daily where (Primary_Type='NARCOTICS')) N
        on (T.Date=N.Date)
        inner join
        (select * from crimes_daily where (Primary_Type='WEAPONS VIOLATION')) W
        on (T.Date=W.Date)
    order by Date;
drop table crimes_daily;
select count(*) from TTV_pivot;
select * from TTV_pivot order by rand() limit 10;
desc TTV_pivot;

#train-test split
drop table if exists forecast_train;
create table forecast_train as
    select * from TTV_pivot where (Date <= str_to_date('2018-10-15', '%Y-%m-%d'));
drop table if exists forecast_test;
    create table forecast_test as
        select * from TTV_pivot where (Date >  str_to_date('2018-10-15', '%Y-%m-%d'));
select count(*) from forecast_train order by date;
select count(*) from forecast_test order by date;
select min(Date), max(Date) from forecast_train;
select min(Date), max(Date) from forecast_test;
drop table if exists TTV_pivot;
select * from forecast_test limit 10;
