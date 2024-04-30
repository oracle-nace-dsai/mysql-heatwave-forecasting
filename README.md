# mysql-heatwave-forecasting

by Joe Hahn,<br />
joe.hahn@oracle.com,<br />
18 July 2023<br />
git branch=master

Launch MySQL Heatwave cluster, load data, use AutoML to train an ML model on that data, then validate that model,
per this blog post \<blog-post-url-tbd\>.

### create VCN:

1 use VCN Wizard to create VCN using the default settings. In the following, \<your-VCN\> will refer the name of the VCN you just created

2 add ingress rules to private subnet-\<your-VCN\>

    source cidr=0.0.0.0/0
    Destination port range=3306,33060
    description=MySQL Port Access

3 add ingress rules to public subnet-\<your-VCN\> Default Security List

    source cidr=0.0.0.0/0
    Destination port range=80,443
    description=Allow HTTP connections


### create bastion VM:

1 create ssh key on cloud shell

    ssh-keygen -t rsa
    cat ~/.ssh/id_rsa.pub

and note the public key

2 copy keys to your desktop since cloud shell files can disappear a few weeks or months

3 create VM with these settings

    name=<your-VM>
    shape=VM.Standard.E4.Flex
    ocpu=2
    VCN=<your-VCN>
    Subnet=public subnet-<your-VCN> (Regional)
    Public key=above public key
    Boot volume=256
    Public IP=<VM-Public-IP>

where \<VM-Public-IP\> is your VM's public IP

4 ssh into VM

    ssh opc@<VM-Public-IP>

5 create ssh key on VM

    ssh-keygen -t rsa
    cat ~/.ssh/id_rsa.pub

6 install git

    sudo yum install git -y

7 clone repo to VM

    git clone https://github.com/oracle-nace-dsai/mysql-heatwave-forecasting.git

8 install oci cli

    sudo dnf install python36-oci-cli -y
    oci --version

9 create & configure ~/.oci/config and pem keys, to allow VM to talk to ObjStore

    oci setup config

using default values except for user & tenancy ocids and region index

10 paste output of

    cat ~/.oci/oci_api_key_public.pem

into OCI user > API keys > Add API key

11 install mysqlsh

    sudo yum install mysql-shell -y
    mysqlsh --version


### download data:

1 on VM, download data from https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2

    cd ~/mysql-heatwave-demo
    wget https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD --output-document=data/crimes.csv

takes a few minutes to download 2Gb file containing 8M records

2 put csv into ObjStore

    ns=<your-ObjStore-namespace>
    bucket_name=<your-bucket-name>
    source_file=data/crimes.csv
    destination_file=chicago/crimes.csv
    oci os object put --bucket-name $bucket_name --file $source_file --name $destination_file -ns $ns --force --auth instance_principal

3 view first few lines

    oci os object get -ns $ns -bn $bucket_name --name $destination_file --auth instance_principal --file - | head

and note column names:

    ID,Case Number,Date,Block,IUCR,Primary Type,Description,Location Description,Arrest,Domestic,Beat,District,Ward,Community Area,FBI Code,X Coordinate,Y Coordinate,Year,Updated On,Latitude,Longitude,Location

4 create par to object crimes.csv


### create MySQL-Heatwave cluster:

1 launch mysql heatWave with these settings

    Development
    Name=<your-MYSQL-name>
    standalone
    mysql heatwave
    admin=admin
    password=<MYSQL-password>
    VCN=<your-VCN>
    Subnet=private subnet-<your-VCN> (Regional)
    cpu=16
    memory=512Gb
    storage=1Tb
    Disable backup plan
    Advanced options > Connections > Hostname=<your-MYSQL-name> #needed to connect OAC to mysql
    FQDN=<your-MYSQL-FQDN>
    Private ip=<mysql-private-ip>
    Mysql port=3306

2 add heatwave cluster with

    cpu=16
    memory=512Tb
    nodes=1
    MySQL HeatWave Lakehouse enabled


### use VM to load crime data into database table:

1 use cloud shell to ssh into VM

    ssh opc@<VM-Public-IP>

3 use mysqlsh to connect to db

    mysqlsh --user=admin --password1=<MYSQL-password> --host=<mysql-private-ip> --sql
    show databases;

4 use mysqlsh to execute load_data.sql, which loads csv from ObjStore into mysql table

    mysqlsh --user=admin --password1=<MYSQL-password> --host=<mysql-private-ip> --sql < load_data.sql


### prep data for ML & train model:

1 use cloud shell to ssh into VM

    ssh opc@<VM-Public-IP>
    cd mysql-heatwave-demo

2 use mysqlsh on VM to tell database to execute prep_data.sql script, which preps data for ML model training

    mysqlsh --user=admin --password1=<MYSQL-password> --host=<mysql-private-ip>  --sql < prep_data.sql

3 exit mysqlsh

    \q


### train & validate ML model:

1 start mysqlsh session on VM

    ssh opc@<VM-Public-IP>
    cd mysql-heatwave-demo
    mysqlsh --user=admin --password1=<MYSQL-password> --host=<mysql-private-ip> --database=Chicago --sql

2 tell AutoML to swiftly train model using LinearRegression and RandomForestRegressor algos only, in 1min

    CALL sys.ML_TRAIN(
        'Chicago.train', 
        'N_next_noisy', 
        JSON_OBJECT(
            'task', 'regression',
            'optimization_metric', 'neg_mean_squared_error',
            'exclude_column_list', JSON_ARRAY('ID', 'Date', 'N_next'),
            'model_list', JSON_ARRAY('LinearRegression')
        ), 
        @next_model_swift
    );

2alt tell AutoML to train better model in 5 minutes

    CALL sys.ML_TRAIN(
        'Chicago.train', 
        'N_next_noisy', 
        JSON_OBJECT(
            'task', 'regression',
            'optimization_metric', 'neg_mean_squared_error',
            'exclude_column_list', JSON_ARRAY('ID', 'Date', 'N_next'),
            'exclude_model_list', JSON_ARRAY('LinearRegression', 'XGBRegressor', 'SVR')
        ), 
        @next_model
    );

by telling automl not to consider LR and XGBR, we trick it into searching for a better model

3 generate report about model training progress

    SELECT QEXEC_TEXT FROM performance_schema.rpd_query_stats WHERE QUERY_TEXT='ML_TRAIN' ORDER BY QUERY_ID DESC limit 1;

4 get optimized model's settings

    set @next_model='Chicago.train_admin_<model_id>';
    select * from ML_SCHEMA_admin.MODEL_CATALOG where (model_handle=@next_model);

where <model_id> refers to the ID of the best performing model that was trained by AutoML.
The above also shows that AutoML's preferred algo is LGBMRegressor and that it didnt drop any features since
"selected_column_names": "N_change", "N_current", "Primary_Type", "Ward", "Week", "Year"]

5 get model's R2=0.90

    CALL sys.ML_MODEL_LOAD(@next_model, NULL);
    CALL sys.ML_SCORE('Chicago.test', 'N_next', @next_model, 'r2', @score, NULL);
    select @score;

6 -1x mean absolute error=-2.7

    CALL sys.ML_SCORE('Chicago.test', 'N_next', @next_model, 'neg_mean_absolute_error', @score, NULL); select @score;

7 compare test and train predictions, to check for overfitting

    CALL sys.ML_SCORE('Chicago.test',  'N_next', @next_model, 'neg_mean_absolute_error', @score, NULL); select @score;
    CALL sys.ML_SCORE('Chicago.train', 'N_next', @next_model, 'neg_mean_absolute_error', @score, NULL); select @score;
    CALL sys.ML_SCORE('Chicago.test', 'N_next', @next_model, 'neg_mean_squared_error', @score, NULL); select @score;
    CALL sys.ML_SCORE('Chicago.train', 'N_next', @next_model, 'neg_mean_squared_error', @score, NULL); select @score;
    select sqrt(18.1), sqrt(12.2);

which shows only slight overfitting

8 append predictions column to test table

    drop table if exists test_predict_json;
    CALL sys.ML_PREDICT_TABLE('Chicago.test', @next_model, 'Chicago.test_predict_json', NULL);
    select * from Chicago.test_predict_json limit 5;

and note that the predictions are also embedded in a json column

9 extract N_next_predict from above

    drop table if exists test_predict;
    create table test_predict as
        select ID, Date, Year, Week, Ward, Primary_Type, N_change, N_current, N_next, Prediction as N_next_predict from test_predict_json;
    drop table if exists test_predict_json;
    select * from test_predict limit 5;

10 compute model's median fractional error=0.27

    drop table if exists fractional_errors;
    create table fractional_errors as
        (select ID, N_next, abs((N_next + 0.5) - (N_next_predict + 0.5))/abs(N_next + 0.5) as fractional_error from test_predict)
        order by fractional_error;
    select count(*)/2 from fractional_errors;
    select * from 
        (select * from 
             (select * from fractional_errors limit 107938) as T
        order by fractional_error desc) as TT
    limit 1;
    drop table fractional_errors;

11 append predictions to validation sample

    drop table if exists valid_predict_json;
    CALL sys.ML_PREDICT_TABLE('Chicago.valid', @next_model,'Chicago.valid_predict_json', NULL);
    drop table if exists valid_predict;
    create table valid_predict as
         select ID, Date, Year, Week, Ward, Primary_Type, N_change, N_current, N_next, Prediction as N_next_predict from valid_predict_json;
    drop table if exists valid_predict_json;
    select * from valid_predict order by rand(17) limit 5;

12 show all available models

    SELECT model_id, model_handle, train_table_name FROM ML_SCHEMA_admin.MODEL_CATALOG;

13 delete an older model

    DELETE FROM ML_SCHEMA_admin.MODEL_CATALOG WHERE model_id=1;
    SELECT model_id, model_handle, train_table_name FROM ML_SCHEMA_admin.MODEL_CATALOG;

14 unload model

    CALL sys.ML_MODEL_UNLOAD(@next_model);

15 exit mysqlsh

    \q


### model explanations

1 ssh from cloud shell to VM and start mysqlsh session

    ssh opc@<VM-Public-IP>
    mysqlsh --user=admin --password1=<MYSQL-password> --host=<mysql-private-ip> --database=Chicago --sql

2 extract feature importance from model

    set @next_model='Chicago.train_admin_<model_id>'';
    CALL sys.ML_MODEL_LOAD(@next_model, NULL);
    SELECT model_explanation FROM ML_SCHEMA_admin.MODEL_CATALOG WHERE (model_handle=@next_model);

model_explanation={"permutation_importance": {"Ward": 0.1071, "Week": 0.0298, "Year": 0.0614, "N_change": 0.0282, "N_current": 0.9055, "Primary_Type": 0.1667}}

3 extract feature scores

    select json_extract(json, '$.Ward') as Ward, json_extract(json, '$.Week') as Week, json_extract(json, '$.Year') as Year, json_extract(json, '$.N_change') as N_change, 
        json_extract(json, '$.N_current') as N_current, json_extract(json, '$.Primary_Type') as Primary_Type from 
            (select json_extract(model_explanation, '$.permutation_importance') as json from 
                (SELECT model_explanation FROM ML_SCHEMA_admin.MODEL_CATALOG WHERE (model_handle=@next_model)) as T
            ) as TT;

4 pivot the above and store in table so that it can be charted in OAC

    drop table if exists model_feature_importance;
    create table model_feature_importance as
        select 'Ward' as feature_, cast(json_extract(json, '$.Ward') as float(8)) as score_ from 
                (select json_extract(model_explanation, '$.permutation_importance') as json from 
                    (SELECT model_explanation FROM ML_SCHEMA_admin.MODEL_CATALOG WHERE (model_handle=@next_model)) as T) as TT
        union
        select 'Week' as feature_, cast(json_extract(json, '$.Week') as float(8)) as score_ from 
                (select json_extract(model_explanation, '$.permutation_importance') as json from 
                    (SELECT model_explanation FROM ML_SCHEMA_admin.MODEL_CATALOG WHERE (model_handle=@next_model)) as T) as TT
        union
        select 'Year' as feature_, cast(json_extract(json, '$.Year') as float(8)) as score_ from 
                (select json_extract(model_explanation, '$.permutation_importance') as json from 
                    (SELECT model_explanation FROM ML_SCHEMA_admin.MODEL_CATALOG WHERE (model_handle=@next_model)) as T) as TT
        union
        select 'Primary_Type' as feature_, cast(json_extract(json, '$. Primary_Type') as float(8)) as score_ from 
                (select json_extract(model_explanation, '$.permutation_importance') as json from 
                    (SELECT model_explanation FROM ML_SCHEMA_admin.MODEL_CATALOG WHERE (model_handle=@next_model)) as T) as TT
        union
        select 'N_change' as feature_, cast(json_extract(json, '$.N_change') as float(8)) as score_ from 
                (select json_extract(model_explanation, '$.permutation_importance') as json from 
                    (SELECT model_explanation FROM ML_SCHEMA_admin.MODEL_CATALOG WHERE (model_handle=@next_model)) as T) as TT
        union
        select 'N_current' as feature_, cast(json_extract(json, '$.N_current') as float(8)) as score_ from 
                (select json_extract(model_explanation, '$.permutation_importance') as json from 
                    (SELECT model_explanation FROM ML_SCHEMA_admin.MODEL_CATALOG WHERE (model_handle=@next_model)) as T) as TT
    ;
    select * from model_feature_importance;
    desc model_feature_importance;


4 generate prediction for a manually created record

    SET @row_input=JSON_OBJECT("ID",2220331,"Date","2003-11-02","Year","2003","Week","44","Ward","17","Primary_Type",'THEFT',"N_change",-5,"N_current",41,"N_next",29,"N_next_predict", 35.8087);
    select @row_input;
    SELECT sys.ML_PREDICT_ROW(@row_input, @next_model, NULL);
    SELECT JSON_PRETTY(sys.ML_PREDICT_ROW(@row_input, @next_model, NULL));

5 explain single prediction

    SELECT JSON_PRETTY(sys.ML_EXPLAIN_ROW(@row_input, @next_model, JSON_OBJECT('prediction_explainer', 'permutation_importance')));

6 table-wise explanations are slow, so create a sample of 10 test records

    drop table if exists test_sample;
    create table test_sample as (select * from test order by rand(17) limit 10);
    select * from test_sample;

7 generate explanations for test_sample, with results stored in table Chicago.test_explain

    drop table if exists test_explain;
    CALL sys.ML_EXPLAIN_TABLE('Chicago.test_sample', @next_model, 'Chicago.test_explain', JSON_OBJECT('prediction_explainer', 'permutation_importance'));
    SELECT * FROM Chicago.test_explain where (_id=3) \G;

docs call this permutation_importance explanations

8 generate Shapley explanations

    drop table if exists test_explain_shap;
    CALL sys.ML_EXPLAIN_TABLE('Chicago.test_sample', @next_model, 'Chicago.test_explain_shap', JSON_OBJECT('prediction_explainer', 'permutation_importance', 'model_explainer', 'shap'));
    SELECT * FROM Chicago.test_explain_shap where (_id=3) \G;


### install anaconda python on VM

install anaconda python, adapted from https://docs.oracle.com/en/solutions/machine-learning-sandbox/configuring-your-system1.html#GUID-96EEE1FA-C4DF-43DC-8767-E423953131FF

1 navigate to JoeHahnVCN > Default Security List for JoeHahnVCN > Add Ingress Rule with these settings

    Source=0.0.0.0/0
    IP Protocol=TCP
    destination port=8888
    description=jupyter

1 use cloud shell to ssh into VM

    ssh opc@<VM-Public-IP>

2 update firewall rules

    sudo firewall-cmd --zone=public --add-port=8888/tcp --permanent
    sudo firewall-cmd --reload

2 browse https://repo.anaconda.com/archive/ and select latest Linux version=Anaconda3-2023.07-1-Linux-x86_64.sh 

3 download installer

    wget https://repo.continuum.io/archive/Anaconda3-2023.07-1-Linux-x86_64.sh 

4 install

    bash Anaconda3-2023.07-1-Linux-x86_64.sh -b
    echo -e 'export PATH="$HOME/anaconda3/bin:$PATH"' >> $HOME/.bashrc
    source ~/.bashrc

5 upgrade the above

    conda update -n base -c defaults conda -y

6 configure shell to use "conda activate"

    conda init bash
    source ~/.bashrc

and note that (base) is added to commandline prompt

7 create mysql conda environment

    conda create --name mysql pip python=3.7 -y

8 activate conda

    conda activate mysql

9 install various python packages

    conda install pandas -y
    conda install matplotlib -y
    conda install seaborn -y
    conda install notebook -y
    conda install sqlalchemy -y
    conda install pymysql -y

10 generate jupyter config file

    jupyter notebook --generate-config

11 add the following to the beginning of /home/opc/.jupyter/jupyter_notebook_config.py

    c = get_config()
    c.NotebookApp.ip = '0.0.0.0'
    c.NotebookApp.open_browser = False
    c.NotebookApp.port = 8888

12 add password=<jupyter-password> to notebook

    jupyter notebook password

13 install self-signed cert

    cd ~
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout jupyter-key.key -out jupyter-cert.pem


### use jupyter on VM to query mysql

1 use cloud shell to ssh into VM

    ssh opc@<VM-Public-IP>

2 start jupyter

    cd ~/mysql-heatwave-demo
    conda activate mysql
    jupyter notebook --certfile=~/jupyter-cert.pem --keyfile=~/jupyter-key.key

3 browse jupyter at

    https://<VM-Public-IP>:8888

4 execute predictions_vs_actuals.ipynb notebook to generate a scatterplot of model predictions vs actuals


### oac to assess model

set up OAC and connect to mysql-hw per https://plforacle.github.io/heatwave-turbo//mysql-heatwave-intro/workshops/ocw23-freetier/index.html?lab=introduction

1 launch OAC

    name=<your-OAC>
    access url=https://joehahnoac2-orasenatdpltintegration03-ia.analytics.ocp.oraclecloud.com/ui/
    capacity=2 ocpus

2 configure private access channel

    name=<your-OAC>
    delete 2nd DNS zone

takes 30min

3 create OAC connection to heatwave

    name=<your-MYSQL-name>
    host=<your-MYSQL-FQDN>
    port=3306
    database=Chicago
    user=admin
    password=<MYSQL-password>

4 create OAC datasets on tables test_predict, valid_predict, feature_importance, and crimes_filtered_sub tables.
For crimes_filtered_sub, change latitude and longitude type from measure to attribute.

5 upload & execute workbooks Chicago.dva, and map.dva


### build forecasting model on mysql-hw

1 access mysql

    ssh opc@<VM-Public-IP>
    cd ~/mysql-heatwave-demo
    mysqlsh --user=admin --password1=<MYSQL-password> --host=<mysql-private-ip> --database=Chicago --sql

2 count daily thefts, narcotics, and weapons across chicago

    drop table if exists crimes_daily;
    create table crimes_daily as
    select date, cast(max(year) as char) as year, max(week) as week, cast(max(weekday(date)) as char) as weekday, primary_type, count(id) as N from crimes_filtered_sub 
        group by date, primary_type order by date, primary_type;
    select * from crimes_daily order by rand() limit 5;
    desc crimes_daily;

3 manually pivot the above to get crime counts versus date

    drop table if exists TTV_pivot;
    create table TTV_pivot as
        select T.date, T.year, T.week, T.weekday, T.N as THEFT, N.N as NARCOTICS, W.N as WEAPONS_VIOLATION from 
            (select * from crimes_daily where (primary_type='THEFT')) T
            inner join
            (select * from crimes_daily where (primary_type='NARCOTICS')) N
            on (T.date=N.date)
            inner join
            (select * from crimes_daily where (primary_type='WEAPONS VIOLATION')) W
            on (T.date=W.date)
        order by date;
    drop table crimes_daily;
    select count(*) from TTV_pivot;
    select * from TTV_pivot order by rand(17) limit 10;
    desc TTV_pivot;

4 train-validation split

    drop table if exists forecast_train;
    create table forecast_train as
        select * from TTV_pivot where (date <= str_to_date('2018-10-15', '%Y-%m-%d'));
    drop table if exists forecast_test;
    create table forecast_test as
        select * from TTV_pivot where (date >  str_to_date('2018-10-15', '%Y-%m-%d'));
    select count(*) from forecast_train order by date;
    select count(*) from forecast_test order by date;
    select min(date), max(date) from forecast_train;
    select min(date), max(date) from forecast_test;
    drop table if exists TTV_pivot;
    select * from forecast_test limit 10;

5 alternatively, execute the following mysql script to do the above:

    mysqlsh --user=admin --password1=<MYSQL-password> --host=<mysql-private-ip>  --sql < pivot_data.sql

6 train forecasting model on data from 2014 to late 2018 to predict number of thefts, narcotics, and weapons violations across all of chicago through 2019

    CALL sys.ML_TRAIN(
        'Chicago.forecast_train',
        'THEFT',
        JSON_OBJECT(
            'task', 'forecasting',
            'datetime_index', 'Date',
            'optimization_metric', 'neg_mean_squared_error',
            'endogenous_variables', JSON_ARRAY('THEFT', 'NARCOTICS', 'WEAPONS_VIOLATION'),
            'exogenous_variables', JSON_ARRAY('Year', 'Week', 'Weekday')
        ), 
        @forecast_model
    );

which uses automl to train various sktime algorithms that are nicely summarized at 
https://towardsdatascience.com/why-start-using-sktime-for-forecasting-8d6881c0a518. 
And statsmodels documented at https://www.statsmodels.org/dev/index.html
Training completes in 4min

7 get model_handle

    SELECT QEXEC_TEXT FROM performance_schema.rpd_query_stats WHERE QUERY_TEXT='ML_TRAIN' ORDER BY QUERY_ID DESC limit 1;

and note model_handle="Chicago.forecast_train_admin_<forecast_model_id>"

8 get training report

    set @forecast_model='Chicago.forecast_train_admin_<forecast_model_id>';
    select * from ML_SCHEMA_admin.MODEL_CATALOG where (model_handle=@forecast_model);

9 generate model predictions on validation sample

    drop table if exists forecast_test_predict_json;
    CALL sys.ML_MODEL_LOAD(@forecast_model, NULL);
    CALL sys.ML_PREDICT_TABLE('Chicago.forecast_test', @forecast_model, 'Chicago.forecast_test_predict_json', NULL);
    select * from Chicago.forecast_test_predict_json limit 3;

which are stored in a json column

10 parse json column

    drop table if exists forecast_test_predict;
    create table forecast_test_predict as
        select Date, Year, Week, Weekday, 
            THEFT, cast(json_extract(json, '$.THEFT') as float(8)) as THEFT_predict,
            NARCOTICS, cast(json_extract(json, '$.NARCOTICS') as float(8)) as NARCOTICS_predict,
            WEAPONS_VIOLATION, cast(json_extract(json, '$.WEAPONS_VIOLATION') as float(8)) as WEAPONS_VIOLATION_predict 
            from (select F.*, json_extract(ml_results, '$.predictions') as json from forecast_test_predict_json as F) T;
    drop table if exists forecast_test_predict_json;
    select * from forecast_test_predict order by rand(17) limit 10;
    select * from forecast_test_predict limit 10;
    desc forecast_test_predict;

keeping in mind that OAC doesnt like json type, so recast json columns as floats

11 score model via neg_sym_mean_abs_percent_error metric=-0.52

    CALL sys.ML_MODEL_LOAD(@forecast_model, NULL);
    CALL sys.ML_SCORE('Chicago.forecast_test', 'THEFT', @forecast_model, 'neg_sym_mean_abs_percent_error', @score, NULL);
    select @score;

12 compare mean fractional errors in forecast model to regression model, 16% for thefts, 69% for narcotics, 34% for weapons

    select avg(abs(THEFT - THEFT_predict)/(THEFT + 0.5)) from forecast_test_predict;
    select avg(abs(NARCOTICS - NARCOTICS_predict)/(NARCOTICS + 0.5)) from forecast_test_predict;
    select avg(abs(WEAPONS_VIOLATION - WEAPONS_VIOLATION_predict)/(WEAPONS_VIOLATION + 0.5)) from forecast_test_predict;

13 use OAC to compare regression model predictions to forecasting model predictions, see Chicago.dva



