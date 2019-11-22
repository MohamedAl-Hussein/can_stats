import geopandas as gpd
import os
import pandas as pd


class GeoDataFrameDatabasePrep:
    projection = 'epsg:3347'
    parent_dir = ''

    def clean_dataframe(self, shapefile, from_projection=None,
                        to_projection=None, type_conversions=None,
                        filter_column=None, filter_type=None,
                        filter_value=None):

        df = gpd.read_file(os.path.join(self.parent_dir, shapefile))
        if filter_column is not None:
            df = df[df[filter_column].astype(filter_type) == filter_value]
        if type_conversions is not None:
            df = df.astype(type_conversions)
        if from_projection is not None and to_projection is not None:
            if not df.crs:
                df.crs = {'init': from_projection}
                df.to_crs({'init': to_projection}, inplace=True)
            else:
                df.crs = {'init': self.projection}
        if not df.crs:
            df.crs = {'init': self.projection}
        df['projection'] = list(df.crs.values())[0]

        return df

    def to_shapefile(self, dataframe, file_name):
        dataframe.to_file(os.path.join(self.parent_dir, f'{file_name}.shp'))

    def to_csv(self, dataframe, file_name):
        dfataframe.to_csv(os.path.join(self.parent_dir, f'{file_name}.csv'),
                          index=False)

    @classmethod
    def modify_projection(cls, projection):
        cls.projection = projection

    @classmethod
    def modify_parent_directory(cls, parent_dir):
        cls.parent_dir = parent_dir


class DataFrameDatabasePrep:
    dataframes = dict()

    @staticmethod
    def filter_by_unique_id(dataframe, columns):
        df = pd.DataFrame()
        for key in columns.keys():
            df[key] = dataframe[columns[key]].unique()
        return df

    @classmethod
    def store_databases_to_dict(cls, dfs):
        for df in dfs:
            cls.dataframes[df.name] = df


class DataFrameRelationshipAnalysis:
    def __init__(self, dataframes):
        self.dataframes = dataframes

    def unique_columns_between_dfs(self, df_main):
        u = dict()
        for df in self.dfs:
            unique_set = (set(list(df_main.columns)) &
                          set(list(df[0].columns))) ^ set(list(df[0].columns))
            u[df[1]] = list(unique_set)

        return u

    @staticmethod
    def max_non_intersection(dfs_list, intersect):
        dfs_set = set(dfs_list)
        max_non_intersect = list()

        for key in intersect.keys():
            s = set(list(intersect[key].keys()))
            intersection = dfs_set ^ s
            val = dict()
            val[key] = len(list(intersection))
            max_non_intersect.append(val)

        return max_non_intersect

    def intersections(self):
        intersect = dict()
        for df in self.dfs:
            intersect[df[1]] = self.get_intersections(df[0])

        for i in list(intersect.keys()):
            for j in list(intersect[i].keys()):
                if (j == i) or (not bool(intersect[i][j])):
                     intersect[i].pop(j)

        return intersect

    def get_intersections(self, df):
        cols = list(df.columns)
        cols.remove('geometry')
        dfs = self.dfs.copy()

        matches = dict()

        for d in dfs:
            intersect = set(cols) & set(list(d[0].columns))
            matches[d[1]] = list(intersect)

        return matches

