from configparser import ConfigParser, ExtendedInterpolation
import numpy as np
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

from work_dir.scripts.hierarchy_cleaning_funcs import\
    generate_id, trim_parents, update_parents
from work_dir.scripts.secret import filepath


def set_environ(schema_path, data_path, db_user, db_pass, db_connection,
                db_name):
    schema = os.path.join(os.getcwd(), schema_path)
    data = os.path.join(os.getcwd(), data_path)
    engine = create_engine(
        f'postgresql://{db_user}:{db_pass}@{db_connection}/{db_name}'
    )

    return {'schema': schema, 'data': data, 'conn': engine}


def load_schema(schema_path):
    df_schema = pd.read_csv(schema_path, sep="\t", header=1, encoding="latin1")
    df_schema.drop(df_schema.index[0], inplace=True)
    df_schema.drop(df_schema.index[2247:], inplace=True)
    df_schema.index = np.arange(0, len(df_schema))

    return df_schema


def apply_schema(df_schema):
    df_t, hier_dict = update_parents(df_schema, 'Characteristics')
    df_trim = trim_parents(df_t, 'Characteristics')
    df_clean, id_dict = generate_id(df_trim, 'trimmed_child', 'trimmed_parent')

    return df_clean


def prepare_dfs(df_clean, data):
    df_topic = pd.DataFrame()
    df_topic['topic'] = df_clean['Topic'].unique()
    df_topic['topic_id'] = (df_topic.index + 1) * 1000000

    df_category = pd.DataFrame()
    df_category['category'] = df_clean['Characteristics'].unique()
    df_category['category_id'] = (df_category.index + 1) * 100

    df_path = df_clean['full_path'].str.split('//', expand=True)
    df_path = df_path.rename(columns={0: 'subcat1', 1: 'subcat2', 2: 'subcat3',
                                      3: 'subcat4', 4: 'subcat5', 5: 'subcat6',
                                      6: 'subcat7', 7: 'subcat8', 8: 'subcat9'})
    df_path['path'] = df_clean['full_path'].unique()
    df_path['path_id'] = (df_path.index + 1) * 10000000
    df_path = df_path.replace({None: ''})

    df_all = pd.read_csv(data)
    df_all['path_id'] = df_all[
        'Member ID: Profile of Dissemination Areas (2247)'
    ].astype(int) * 10000000
    df_all = pd.merge(df_path, df_all, how='inner', left_on='path_id',
                      right_on='path_id')

    df_geo = pd.DataFrame()
    df_geo['geoname'] = df_all['GEO_NAME'].unique()
    df_geo['alt_geocode'] = df_all['ALT_GEO_CODE'].unique()
    df_geo['geo_id'] = (df_geo.index + 1) * 1000

    df_qual = pd.DataFrame()
    df_qual['quality_flag'] = df_all['DATA_QUALITY_FLAG'].unique()
    df_qual['quality_id'] = (df_qual.index + 1) * 10

    df_desc = pd.DataFrame()
    df_desc['description'] = df_all[
        'Notes: Profile of Dissemination Areas (2247)'
    ].unique()
    df_desc['desc_id'] = (df_desc.index + 1) * 10
    df_desc = df_desc.replace({'description': {np.nan: -1}})
    df_desc = df_desc.replace({'desc_id': {np.nan: -1}})
    df_desc['description'] = df_desc['description'].astype(int)
    df_desc.loc[226] = -2, 0

    df_all = pd.merge(df_geo, df_all, how='inner', left_on='geoname',
                      right_on='GEO_NAME')
    df_all = pd.merge(df_qual, df_all, how='inner', left_on='quality_flag',
                      right_on='DATA_QUALITY_FLAG')
    df_all = pd.merge(df_desc, df_all, how='outer', left_on='description',
                      right_on='Notes: Profile of Dissemination Areas (2247)')

    df_clean = pd.merge(df_topic, df_clean, how='inner', left_on='topic',
                        right_on='Topic')
    df_clean = pd.merge(df_category, df_clean, how='inner', left_on='category',
                        right_on='Characteristics')
    df_clean = pd.merge(df_path, df_clean, how='inner', left_on='path',
                        right_on='full_path')

    df_data = df_clean[[
        'path_id', 'category_id', 'topic_id', 'Male.1', 'Female.1', 'Total.1'
    ]]

    df_alldata = pd.merge(df_data, df_all, how='inner', left_on='path_id',
                          right_on='path_id')
    df_alldata = df_alldata.rename(
        columns={'CENSUS_YEAR': 'year', 'GNR': 'gnr',
                 'Dim: Sex (3): Member ID: [1]: Total - Sex': 'total',
                 'Dim: Sex (3): Member ID: [2]: Male': 'male',
                 'Dim: Sex (3): Member ID: [3]: Female': 'female'})

    df_alldata = df_alldata.drop(
        ['Male.1', 'Female.1', 'Total.1', 'description', 'quality_flag',
         'geoname', 'alt_geocode', 'GEO_CODE (POR)', 'GEO_LEVEL', 'GEO_NAME',
         'GNR_LF', 'DATA_QUALITY_FLAG', 'CSD_TYPE_NAME',
         'DIM: Profile of Dissemination Areas (2247)',
         'Notes: Profile of Dissemination Areas (2247)', 'ALT_GEO_CODE',
         'Member ID: Profile of Dissemination Areas (2247)',
         'subcat1', 'subcat2', 'subcat3', 'subcat4', 'subcat5', 'subcat6',
         'subcat7', 'subcat8', 'subcat9', 'path'],
        axis=1)

    mapping = {'...': -1, 'F': -2, 'x': -3}

    df_alldata = df_alldata.replace(
        {'total': mapping, 'male': mapping, 'female': mapping}
    )

    cols = ['total', 'male', 'female']
    for col in cols:
        df_alldata[col] = df_alldata[col].astype(float)

    df_alldata = df_alldata.replace({'gnr': {np.nan: -1}})
    df_alldata = df_alldata.replace({'desc_id': {np.nan: 0}})

    return {'topic': df_topic, 'category': df_category, 'path': df_path,
            'geo': df_geo, 'qual': df_qual, 'desc': df_desc,
            'stats': df_alldata}


def df_to_sql(conn, schema_name, dfs_tables):
    topic = dfs_tables['topic']
    cat = dfs_tables['category']
    path = dfs_tables['path']
    geo = dfs_tables['geo']
    qual  = dfs_tables['qual']
    desc = dfs_tables['desc']
    stats = dfs_tables['stats']

    topic[0].to_sql(name=topic[1], con=conn, schema=schema_name, index=False,
                    if_exists='append')
    cat[0].to_sql(name=cat[1], con=conn, schema=schema_name, index=False,
                  if_exists='append')
    path[0].to_sql(name=path[1], con=conn, schema=schema_name,index=False,
                   if_exists='append')
    geo[0].to_sql(name=geo[1], con=conn, schema=schema_name,index=False,
                  if_exists='append')
    qual[0].to_sql(name=qual[1], con=conn, schema=schema_name,index=False,
                   if_exists='append')
    desc[0].to_sql(name=desc[1], con=conn, schema=schema_name,index=False,
                   if_exists='append')
    stats[0].to_sql(name=stats[1], con=conn, schema=schema_name,index=False,
                    if_exists='append')


if __name__ == "__main__":
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    parser.read(f'{filepath}/dev.ini')

    schema_path = parser.get('dfs', 'schema_path')
    data_path = parser.get('dfs', 'data_path')

    db_user = parser.get('db', 'db_user')
    db_pass = parser.get('db', 'db_pass')
    db_connection = parser.get('db', 'db_connection')
    db_name = parser.get('db', 'db_name')
    db_schema = parser.get('db', 'db_schema')

    env = set_environ(schema_path, data_path, db_user, db_pass, db_connection,
                      db_name)

    df_schema = load_schema(schema_path)
    df_clean = apply_schema(df_schema)
    dfs = prepare_dfs(df_clean, env['data'])

    tables = dict(topic='topic', category='category', path='fpath',
                  geo='geo', qual='qual', desc='desc', stats='data')

    dfs_tables = dict(
        list(zip(dfs.keys(), list(zip(dfs.values(), tables.values()))))
    )

    df_to_sql(env['conn'], db_schema, dfs_tables)

