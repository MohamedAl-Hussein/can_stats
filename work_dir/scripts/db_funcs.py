import numpy as np
from sqlalchemy.types import BigInteger, Float, Text
from geoalchemy2 import Geometry, WKTElement


class DataFrameToPostgreSQL:
    def __init__(self, user, password, host, port, schema, database):
        self.user = user
        self.password = password
        self.host= host
        self.port = port
        self.schema = schema
        self.database = database
        self.engine = f'postgresql://{user}:{password}@{host}:{port}/{database}'

    def __repr__(self):
        return 'DataFrameToPostgreSQL(user, password, host, port, database)'

    def __str__(self):
        return (f'User: {self.user}\nPassword: ****\nHost: {self.host}'
                f'\nPort: {self.port}\nSchema: {self.schema}'
                f'\nDatabase: {self.database}')

    def to_sql(self, dataframes):
        for df in dataframes.keys():
            dataframes[df].to_sql(
                name=df, con=self.engine, schema=self.schema, index=False,
                if_exists='replace')


class GeoDataFrameToPostgreSQL(DataFrameToPostgreSQL):
    srid = 3347
    geom = 'GEOMETRY'

    def wkt_transformer(self, dataframe):
        dataframe['geom'] = dataframe['geometry'].apply(
            lambda x: WKTElement(x.wkt, srid=self.srid)
        )
        dataframe.drop('geometry', axis=1, inplace=True)

        return dataframe

    def to_sql(self, dataframe, table, dtypes):
        dataframe.to_sql(table, self.engine, self.schema, if_exists='replace',
                         index=False, dtype=dtypes)

    def geopandas_to_sql_type_convertor(self, dataframe):
        #TODO: Add more types and test for accuracy
        #TODO: Add type_conversions as class variable
        #TODO: Create simpler version for pandas dataframes
        dtypes = dict()
        type_conversions = {
            np.dtype('int64'): BigInteger,
            np.dtype('float64'): Float,
            np.dtype('O'): Text
        }
        dataframe_dtypes = dict(dataframe.dtypes)
        for key in dataframe_dtypes:
            if key == 'geom':
                dtypes[key] = Geometry(geometry_type=self.geom, srid=self.srid)
            else:
                dtypes[key] = type_conversions[dataframe_dtypes[key]]

        return dtypes

    def prep_dataframe(from_shapefile, to_shapefile, dataframe):
        pass

    @classmethod
    def modify_srid(cls, srid):
        cls.srid = srid

    @classmethod
    def modify_geometry_type(cls, geom):
        cls.geom = geom

