import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine
from sqlalchemy import text
from time import sleep
import os

def convert_dtypes(df):
    # try to convert obj to numeric /date
    for col in df.columns:
        try:
            if 'date' in col:
                df[col] = pd.to_datetime(df[col])
            else:    
                df[col] = pd.to_numeric(df[col])
        except:
            pass
        
    return df


def process_gt_990_data(ein):
    gt_org_url = f"https://990-infrastructure.gtdata.org/irs-data/990basic120fields?ein={ein}"
    res = requests.get(gt_org_url).json()

    gt_cols = ['FILEREIN','FILERNAME1','TAXYEAR','TAXPERBEGIN','TAXPEREND','TOTREVCURYEA','TOTEXPCURYEA','CYYRRELEEXXP','TOASEOOYY','TOLIEOOYY','NAFBEOY','MEMBERDUESUE','GOVERNGRANTS',
           'ALLOOTHECONT','TOTACASHCONT','TOTPROSERREV','GROINCFUNEVE','FUNDDIREEXPE','TORETORECOOL','TOTFUNEXPTOT','PROGSERVEXPE','MANAGENEEXPE','FUNDRAEXPENS','TOTAEMPLCNTN']

    gt_col_names = ['ein','organization_name','year','year_start','year_end','revenue_total','expenses_total','net_profit_loss','total_assets','total_liabilities','net_assets','membership_dues','revenue_public',
                'revenue_other_contributions','total_contributed_revenue','revenue_earned','revenue_fundraising','fundraising_expense','revenue_total_b','expenses_total_b','expense_program_services',
                'expense_administration','expense_fundraising','num_employees']

    gt_col_mapping = dict(zip(gt_cols, gt_col_names))

    p = pd.DataFrame(res['body']['results'])[gt_cols].rename(columns=gt_col_mapping)
    p = convert_dtypes(p)
    p['year'] = p['year'] + 1
    p['contributed_revenue'] = p['total_contributed_revenue'] - p['revenue_public']
    p['other_revenue'] = p['revenue_total'] - p['total_contributed_revenue'] - p['revenue_earned']
    p['months_of_cash'] = p['total_assets'] / (p['expenses_total'] / 12)
    p['months_of_cash'] = p['months_of_cash'].fillna(0).replace([np.inf, -np.inf], 0)

    cols_to_drop = ['year_start','year_end','revenue_total_b','expenses_total_b','total_contributed_revenue','revenue_other_contributions']
    p = p.drop(columns=cols_to_drop)
    
    return p


# Define connection parameters
db_url = os.environ.get("CONNECTION_STRING")

# Create a database engine
engine = create_engine(db_url)

# Load the table into a pandas DataFrame
table_name = "organizations"
query = f"SELECT * FROM {table_name}"
ext_orgs = pd.read_sql_query(query, engine)

ext_orgs = ext_orgs[['org_id','ein','org_type','org_name']]
ext_orgs['ein_query'] = ext_orgs.ein.apply(lambda x: str(x).replace("-", "").zfill(9))

grantee_990s = []
for e in ext_orgs.ein_query.unique():
    sleep(1)
    try:
        p = process_gt_990_data(e)
        # p['ein'] = e
        grantee_990s.append(p)
    except Exception as ex:
        print(f"Error processing EIN {e}: {ex}")
        continue

grantee_990s = pd.concat(grantee_990s, ignore_index=True)
grantee_990s.fillna(0, inplace=True)
grantee_990s['ein'] = grantee_990s.ein.astype(str)
grantee_990s = grantee_990s.merge(ext_orgs, left_on='ein', right_on='ein_query', how='left')
grantee_990s = grantee_990s.drop(columns=['ein_query','ein_x','organization_name']).rename(columns={'ein_y':'ein'})
grantee_990s['id'] = range(1, len(grantee_990s) + 1)

new_table_name = "external_orgs_990_data"

# Write the DataFrame to the database
grantee_990s.to_sql(new_table_name, engine, if_exists='replace', index=False)

# Set 'id' as the primary key
with engine.connect() as conn:
    conn.execute(text(f"""
        ALTER TABLE public.{new_table_name}
        DROP CONSTRAINT IF EXISTS {new_table_name}_pkey;
        ALTER TABLE public.{new_table_name}
        ADD PRIMARY KEY (id);
    """))
print(f"Table '{new_table_name}' written to the database and 'id' set as primary key.")


