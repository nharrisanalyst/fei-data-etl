import os
import time
from pathlib import Path
from types import SimpleNamespace
import pandas as pd
import psycopg
from psycopg import sql
from sqlalchemy import create_engine, text
from clean import clean

def pipeline(params):
    time.sleep(20)
    user=params.user
    password=params.password
    db=params.db
    host=params.host
    port=params.port

    ##print('this is the pipeline', user, password, db,host,port)
    conn = psycopg.connect(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    with open('sql_scripts/drop_views.sql','r') as drop_views_script:
        drop_views = drop_views_script.read()
        with conn.cursor() as cur:
            cur.execute(drop_views)
            conn.commit()
    
    
    ##reading all geo data to data frame 
    counties = pd.read_csv('geo_data/county_list.csv')
    cities= pd.read_csv('geo_data/cities.csv')
    zipcodes = pd.read_csv('geo_data/zipcode_data_full.csv')
   
    ##reading yearlydata 
    yearly_2018 = pd.read_csv('yearly_data/all_companies/RPCA_ALL_2018.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    yearly_2019 = pd.read_csv('yearly_data/all_companies/RPCA_ALL_2019.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    yearly_2020 = pd.read_csv('yearly_data/all_companies/RPCA_ALL_2020.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    yearly_2021 = pd.read_csv('yearly_data/all_companies/RPCA_ALL_2021.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    yearly_2022 = pd.read_csv('yearly_data/all_companies/RPCA_ALL_2022.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    yearly_2023 = pd.read_csv('yearly_data/all_companies/RPCA_ALL_2023.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    yearly_dfs = [yearly_2018,yearly_2019,yearly_2020,yearly_2021,yearly_2022,yearly_2023]
    yearly_cleaned = [clean(df) for df in yearly_dfs]
    
    ##reading ppc data 
    ppc_2018 = pd.read_csv('yearly_data/ppc/RPCA_PPC_2018.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    ppc_2019 = pd.read_csv('yearly_data/ppc/RPCA_PPC_2019.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    ppc_2020 = pd.read_csv('yearly_data/ppc/RPCA_PPC_2020.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    ppc_2021 = pd.read_csv('yearly_data/ppc/RPCA_PPC_2021.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    ppc_2022 = pd.read_csv('yearly_data/ppc/RPCA_PPC_2022.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    ppc_2023 = pd.read_csv('yearly_data/ppc/RPCA_PPC_2023.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    ppc_dfs = [ppc_2018,ppc_2019,ppc_2020,ppc_2021,ppc_2022,ppc_2023]
    ppc_cleaned = [clean(df) for df in ppc_dfs]
    
    ##reading fire data 
    fire_2018 = pd.read_csv('yearly_data/fire/RPCA_FIRE_2018.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    fire_2019 = pd.read_csv('yearly_data/fire/RPCA_FIRE_2019.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    fire_2020 = pd.read_csv('yearly_data/fire/RPCA_FIRE_2020.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    fire_2021 = pd.read_csv('yearly_data/fire/RPCA_FIRE_2021.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    fire_2022 = pd.read_csv('yearly_data/fire/RPCA_FIRE_2022.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    fire_2023 = pd.read_csv('yearly_data/fire/RPCA_FIRE_2023.csv',  quotechar='"', dtype=str,  skipinitialspace=True)
    fire_dfs = [fire_2018,fire_2019,fire_2020,fire_2021,fire_2022,fire_2023]
    fire_cleaned = [clean(df) for df in fire_dfs]
    
    ##reading fhsz data 
    fhsz_data = pd.read_csv('zipcode_fire_rankings/fire_zipcodes.csv', quotechar='"')
    #reading in sql data defenition/create table script 
    #connecting to postgres engine 
    ##executes sql and inserts data for pandas db
    with open('sql_scripts/create_tables.sql','r') as start_data_base:
        start_sql = start_data_base.read()
        with conn.cursor() as cur:
            cur.execute(start_sql)
            conn.commit()
            for _, row in counties.iterrows():
                cur.execute(
                    'Insert into counties(county_id,county) values(%s, %s)',
                    (row['county_number'], row['county_name'])
                )
            conn.commit()
            for _, row in cities.iterrows():
                cur.execute('SELECT county_id from counties where county = %s;', (row['county'].strip(),))
                row_sql = cur.fetchone();
                cur.execute(
                    'INSERT into cities(city, county_id) values(%s,%s)',
                    (row['city'].strip(),row_sql[0])
                )
            conn.commit()
            for _,row in zipcodes.iterrows():
                cur.execute('SELECT city_id from cities where city = %s;', (row['city'].strip(),))
                row_sql = cur.fetchone();
                if row_sql is not None:
                    cur.execute(
                        'INSERT into zipcodes(zipcode, city_id) values(%s,%s)',
                        (str(row['zipcode']).strip(),row_sql[0])
                    )
                else:
                    print(row['city'].strip(), 'missing city')
            conn.commit()
            conn.close()

    engine_alch = create_engine(f'postgresql+psycopg://{user}:{password}@{host}:{port}/{db}')         
## we are going to add these tables to the datABASE
    for i, df in enumerate(yearly_cleaned):
        if(i==0):
            df.to_sql("all_companies", engine_alch, if_exists="replace", index=False)
        else:
            df.to_sql("all_companies", engine_alch, if_exists="append", index=False)
        print('uploaded yearly data',i)
    
    for i, df in enumerate(ppc_cleaned):
        if(i==0):
            df.to_sql("ppc", engine_alch, if_exists="replace", index=False)
        else:
            df.to_sql("ppc", engine_alch, if_exists="append", index=False)
        print('uploaded ppc data',i)
    
    for i, df in enumerate(fire_cleaned):
        if(i==0):
            df.to_sql("fire_data", engine_alch, if_exists="replace", index=False)
        else:
            df.to_sql("fire_data", engine_alch, if_exists="append", index=False)
        print('uploaded fire data',i)
    
    ## uploading fhsz data to the postgres database
    fhsz_data.to_sql("fhsz_data", engine_alch, if_exists="replace")  
    
    
    conn_2 = psycopg.connect(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    with open('sql_scripts/make_views.sql','r') as make_views_script:
        make_views = make_views_script.read()
        with conn_2.cursor() as cur:
            cur.execute(make_views)
            conn_2.commit()
        conn_2.close() 
        
## adding data that gives us FHSZ_mean and FHS_majority

    
    
    
if __name__ == '__main__':
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    db = os.environ.get("POSTGRES_DB")
    host = os.environ.get("POSTGRES_HOST")
    port= os.environ.get("POSTGRES_PORT")
    args = SimpleNamespace(**{'user': user, 
                              'password':password, 
                              'db':db,
                              'host':host,
                              'port':port
                              })
    
    pipeline(args)