#prep_data.sql
#by Joe Hahn
#joe.hahn@oracle.come
#July 20, 2023

#prepare data for regression ML model training

#set random number
set @rn_seed=17;

#use crimes  table in Chicago database
use Chicago;
desc crimes;

#crimes table contains 7.8M records
select count(*) from crimes;

#crime data spans years 2001-2023
select Year, count(*) as N_records from crimes group by Year order by Year;

#there are 50 wards across chicago
select Ward, count(*) as N_records from crimes group by Ward order by Ward;

#note that crime-counts in Ward=0 and Year=2000,2001 are suspicious and should be dropped
select Ward, Year, count(*) as N_records from 
    (select Ward, Year(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Year from crimes where ((Ward=0) or (Ward=28))) T
    group by Ward, Year order by Ward, Year;

#create table containing all Wards except 0
drop table if exists top_wards;
create table top_wards as
    select Ward from 
        (select Ward, count(*) as N_records from crimes where (Ward!=0) group by Ward order by N_records desc) as T;
alter table top_wards add column `id` int(10) unsigned primary KEY AUTO_INCREMENT;
select * from top_wards;

#this data tracks 36 distinct crimes
select Primary_Type, count(*) as N_records from crimes group by Primary_Type order by N_records desc;

#note top 12 crimes
select Primary_Type, count(*) as N_records from crimes group by Primary_Type order by N_records desc limit 12;

#create table containing only top 12 crimes
drop table if exists top_crimes;
create table top_crimes as
select Primary_Type from 
    (select Primary_Type, count(*) as N_records from crimes group by Primary_Type order by N_records desc limit 12) as t
    order by N_records desc;
alter table top_crimes add column `id` int(10) unsigned primary KEY AUTO_INCREMENT;
select * from top_crimes;

#preserve selected fields and extract various time quantities
select ID_key as ID, 
    str_to_date(Date, '%m/%d/%Y %h:%i:%s %p') as Timestamp, 
    date(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Date, 
    year(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Year,
    month(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Month,
    yearweek(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p'), 0) as Yearweek,
    dayofweek(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Weekday,
    hour(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Hour,
    Ward, Primary_Type, 1 as Event, Latitude, Longitude from crimes
limit 20;

#note that final week where Date>='2023-07-16' is incomplete and should be dropped
select Yearweek, min(Date), count(ID) as N_records from
    (select ID, Ward, date(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Date, yearweek(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p'), 0) as Yearweek from crimes) T
    where (Date > '2022-11-01') and (Ward > 0)
    group by Yearweek order by Yearweek;

#also note that weeks having Date<='2002-05-05' are incomplete and should be dropped
select Yearweek, min(date), count(ID) as N_records from
    (select ID, Ward, date(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Date, yearweek(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p'), 0) as Yearweek from crimes) T
    where (Date < '2002-10-01') and (Ward > 0)
    group by Yearweek order by Yearweek;

#create table containing all possible dates having Date >= '2002-05-05' (since prior records are incomplete) and Date < '2023-07-16') since final week is incomplete
drop table if exists all_dates;
create table all_dates as
    select * from 
        (select distinct date(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Date from crimes) T
    where ((Date >= '2002-05-05') and (Date < '2023-07-16'))
    order by Date;
alter table all_dates add column `id` int(10) unsigned primary KEY AUTO_INCREMENT;
select * from all_dates order by Date limit 20;

#create grid of all possible Dates, Wards, and Crimes
drop table if exists grid;
create table grid as
    select * from 
        (select Date, Ward from all_dates cross join top_wards) as DW
        cross join 
        (select Primary_Type from top_crimes) as C
    order by Date, Ward, Primary_Type;
alter table grid add column `id` int(10) unsigned primary KEY AUTO_INCREMENT;
select * from grid order by rand(@rn_seed) limit 20;
select min(Date), max(Date) from grid;

#inner join grid to selected fields in crimes to filter in only top_crimes occurring within top_wards
drop table if exists crimes_filtered;
create table crimes_filtered (PRIMARY KEY (id)) as
    select ID_key as ID, Timestamp, Date, Year, Month, substring(convert(yearweek, char), 5, 2) as Week, Yearweek, Hour, Ward, Primary_Type, Latitude, Longitude from
        (select Date as D, Ward as W, Primary_Type as PT from grid) as G
        join
        (
            select ID_key, 
                str_to_date(Date, '%m/%d/%Y %h:%i:%s %p') as Timestamp, 
                date(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Date, 
                year(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Year,
                month(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Month,
                yearweek(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p'), 0) as Yearweek,
                dayofweek(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Weekday,
                hour(str_to_date(Date, '%m/%d/%Y %h:%i:%s %p')) as Hour,
                Ward, Primary_Type, Latitude, Longitude from crimes
        ) as C
        on ((G.D=C.Date) and (G.W=C.Ward) and (G.PT=C.Primary_Type))
        ;
select * from crimes_filtered order by rand(@rn_seed) limit 20;
select min(Date), max(Date), min(Ward), max(Ward) from crimes_filtered;
desc crimes_filtered;

#confirm final week was dropped as expected
select Yearweek, min(Date), count(*) as N_records from crimes_filtered where (Date > '2022-11-01') group by Yearweek order by Yearweek;

#confirm that Dates < 2002-05-05 having incomplete records are dropped
select Yearweek, min(Date), count(*) as N_records from crimes_filtered where (Year < 2003) group by Yearweek order by Yearweek;

#compare record counts...filtering drops record-count by 12%, from 7.8M to 6.9M
select count(*) from crimes;
select count(*) from crimes_filtered;

#69K records ie 1% dont have legit Latitude & Longitude
select count(*) from crimes_filtered where not((Latitude > 30.0) and (Longitude < -80.0));

#preserve only 500K THEFT, NARCOTICS, and WEAPONS VIOLATION events occurring during years 2014 to 2019, for mapping within OAC
#also drop records having suspicious lat,long
drop table if exists crimes_filtered_sub;
    create table crimes_filtered_sub as 
        select * from crimes_filtered where (
            ((Year>=2014) and (Year<=2019))
            and (Primary_Type in ('THEFT', 'NARCOTICS', 'WEAPONS VIOLATION'))
            and ((Latitude > 20.0) and (Longitude < -45.0))
        );
select count(*) from crimes_filtered_sub;
select * from crimes_filtered_sub limit 10;
desc crimes_filtered_sub;

#generate weekly_grid of all Yearweek, Wards, and crimes
drop table if exists weekly_grid;
create table weekly_grid (PRIMARY KEY (id)) as
    select ID, Date, year(Date) as Year, cast(trim(leading '0' from substring(convert(Yearweek, char), 5, 2)) as unsigned) as Week, Yearweek, Ward, Primary_Type from 
        (select min(ID) as ID, min(Date) as Date, Yearweek, Ward, Primary_Type from
            (select ID, Date, Yearweek(Date) as Yearweek, Ward, Primary_Type from grid) as T 
            group by Yearweek, Ward, Primary_Type) as TT;
select * from weekly_grid order by rand(@rn_seed) limit 20;
desc weekly_grid;

#count weekly crimes across all wards
select min(ID) as ID, min(Year) as Year, min(Week) as Week, Yearweek, Ward, Primary_Type, count(ID) as N_events from crimes_filtered group by Yearweek, Ward, Primary_Type order by rand(@rn_seed) limit 20;

#left outer join weekly_grid to crimes_filtered to pad with zeros when missing data due to zero crimes
#also cast Year, Week, and Ward as zero-padded strings, to ensure that AutoML does index or onehot encoding of those columns
drop table if exists current_crimes;
create table current_crimes (PRIMARY KEY (ID)) as
    select ID, Date, cast(Year as char) as Year, lpad(cast(Week as char), 2, '0') as Week, Yearweek, 
        lpad(cast(Ward as char), 2, '0') as Ward, Primary_Type, ifnull(N_events, 0) as N_current from 
            (select ID, Date, Year, Week, Yearweek, Ward, Primary_Type from weekly_grid) G
            left join
            (select Yearweek as Yw, Ward as W, Primary_Type as PT, count(ID) as N_events from crimes_filtered group by Yearweek, Ward, Primary_Type) as C
            on ((G.Yearweek=C.Yw) and (G.Ward=C.W) and (G.Primary_Type=C.PT));
select * from current_crimes order by rand(@rn_seed) limit 20;
select min(Date), max(Date) from current_crimes;

#spot-check record counts, current_crimes contains 664K records for 6.9M crimes
select count(*) from weekly_grid;
select count(*) from current_crimes;
select sum(N_current) from current_crimes;
select count(*) from crimes_filtered;

#use window functions to compute N_next=next week's counts, and N_change=change in N_current over previous week. Also add column of rand() and drop null values
drop table if exists weekly_crimes;
create table weekly_crimes (PRIMARY KEY (ID)) as
    select ID, Date, Year, Week, Ward, Primary_Type, N_current-N_previous as N_change, N_current, N_next, rand(@rn_seed) as ran_num from 
        (select ID, Date, Year, Week, Yearweek, Ward, Primary_Type, N_current, 
            lead(N_current, 1) over (partition by Ward, Primary_Type order by Date) as N_next,
            lag(N_current, 1)  over (partition by Ward, Primary_Type order by Date) as N_previous
            from current_crimes) as T
        where ((N_current >= 0) and (N_previous >= 0) and (N_next >= 0));
drop table if exists current_crimes;

#spot-check the above
select count(*) from weekly_crimes;
select sum(N_current) from weekly_crimes;
select * from weekly_crimes where (Primary_Type='THEFT') and (Ward='28') order by Date limit 20;
desc weekly_crimes;

#flag records as test-train when date < '2023-01-20' with subsequent records going to validation sample. Train:test split will be 2:1
#N_next_noisy = N_next random +-0.25, adding noise to a positive-definite target variable sometimes helps regressor accuracy for targets near zero
drop table if exists TTV;
create table TTV (PRIMARY KEY (ID)) as
    select C.*, C.N_next + (rand(@rn_seed)-0.5)/2 as N_next_noisy,
    case 
        when (Date < str_to_date('2023-01-20', '%Y-%m-%d')) then 
            case when (ran_num < 0.667) then 'train' else 'test' end
        else 'valid'
    end as TTV
    from weekly_crimes as C;
select * from TTV order by rand(@rn_seed) limit 20;

#extract train, test, and validation samples
drop table if exists train;
create table train (PRIMARY KEY (ID)) select ID, Date, Year, Week, Ward, Primary_Type, N_change, N_current, N_next, N_next_noisy from TTV where (TTV='train');
drop table if exists test;
create table test  (PRIMARY KEY (ID)) select ID, Date, Year, Week, Ward, Primary_Type, N_change, N_current, N_next, N_next_noisy from TTV where (TTV='test');
drop table if exists valid;
create table valid (PRIMARY KEY (ID)) select ID, Date, Year, Week, Ward, Primary_Type, N_change, N_current, N_next, N_next_noisy from TTV where (TTV='valid');
drop table TTV;
select count(*), min(Date), max(Date) from train;
select count(*), min(Date), max(Date) from test;
select count(*), min(Date), max(Date) from valid;
select * from valid where (Primary_Type='THEFT') and (Ward='28') order by Date;


