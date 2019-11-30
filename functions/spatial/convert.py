import geopandas as gpd
import matplotlib.pyplot as plt
from shapely import wkt


gpd.options.display_precision = 16


class GeoPandasAnalysis(object):
    def __init__(self, filename=''):
        self.filename = filename

    def get_dataframe(self, filename):
        df = gpd.read_file(filename)
        print(df.head())
        return df

    def get_centroids(self, df):
        df['centroid_column'] = df.centroid
        df = df.set_geometry('centroid_column')
        return df

    def plot_graph(self, df, column=None):
        if column != None:
            df.plot(column=column)
        else:
            df.plot()
        plt.show()

    def write_to_shp(self, df, filename):
        f = df.to_file(f'{filename}.shp')
        return f

    def convert_projection(self, df, c_output):
        df = df.to_crs({'init': c_output})
        return df

    def create_geometry(self, df, *args):
        df[args[0]] = df[args[0]].astype(float)
        df[args[1]] = df[args[1]].astype(float)
        df['geometry'] = gpd.points_from_xy(df[args[0]], df[args[1]])
        return df

    def point_in_polygon(self, point, polygon):
        df = gpd.sjoin(point, polygon, op="within", how="inner")
        df.drop('index_right', axis=1, inplace=True)
        return df

    def shapefile_to_csv(self, filename, shp_name):
        geo = GeoPandasAnalysis(filename)
        df = geo.get_dataframe(filename)
        geo.write_to_shp(df, shp_name)
        return df

    def csv_to_shapefile(self, filename, shp_name, col_x, col_y, crs_in,
                         crs_out):
        geo = GeoPandasAnalysis(filename)
        df = geo.get_dataframe(filename)
        df = geo.create_geometry(df, col_x, col_y)
        df.crs = {'init': crs_in}
        df = geo.convert_projection(df, crs_out)
        geo.write_to_shp(df, shp_name)
        return df

    def point_by_area(self, filename, dir_name, area_filename, area_shp_name,
                      point_filename, point_shp_name, col_x, col_y, crs_in,
                      crs_out, csv_savename, shp_savename
                      ):
        geo = GeoPandasAnalysis()
        dir_name = filename + dir_name
        area_filename = filename + area_filename
        area_shp_name = dir_name + area_shp_name
        dissemination_blocks = geo.shapefile_to_csv(
            filename=area_filename,
            shp_name=area_shp_name
        )
        point_filename = filename + point_filename
        point_shp_name = dir_name + point_shp_name
        point = geo.csv_to_shapefile(
            filename=point_filename,
            shp_name=point_shp_name,
            col_x=col_x,
            col_y=col_y,
            crs_in=crs_in,
            crs_out=crs_out
        )
        area_df = geo.get_dataframe(area_filename)
        point_area = geo.point_in_polygon(point, area_df)
        point_area.to_csv(
            f'{dir_name}{csv_savename}',
            index=False
        )
        geo.write_to_shp(
            point_area,
            f'{dir_name}{shp_savename}'
        )

