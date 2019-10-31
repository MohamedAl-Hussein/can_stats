import json
import geopandas as gpd
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
from shapely import wkt


class DataFuncs(object):
    def fix_indent(self, x):
        """
        x: list containing column values
        """
        i = 0
        h = 0
        top = None

        while i in range(len(x)):
            h = (len(x[i]) - len(x[i].lstrip())) // 2
            if h == 0:
                top = x[i]
            elif h == 1:
                x[i] = top + '//' + x[i].lstrip()
                first = x[i]
            elif h == 2:
                x[i] = first + '//' + x[i].lstrip()
                second = x[i]
            elif h == 3:
                x[i] = second + '//' + x[i].lstrip()
                third = x[i]
            elif h == 4:
                x[i] = third + '//' + x[i].lstrip()
                fourth= x[i]
            elif h == 5:
                x[i] = fourth + '//' + x[i].lstrip()
                fifth = x[i]
            elif h == 6:
                x[i] = fifth + '//' + x[i].lstrip()
                sixth = x[i]
            elif h == 7:
                x[i] = sixth + '//' + x[i].lstrip()
            i += 1

        return x

    def dataframe_report(self, df):
        """
        df: dataframe to generate report on
        """
        info = df.info(verbose=True)

        print('INFO:', info)

        for col in df.columns:
            unique = len(df[col].unique())
            print(f'Column Name: {col}\nNumber of Unique Values: {unique}')

        for col in df.columns:
            print(f'Value Counts:\n{df[col].value_counts().sort_index()}')

    def extract_census_feature(self, df_census, df_da, crs_in, geo_id,
                                      projection_out):
        """
        df_census: dataframe type
        df_da: dataframe type
        crs_in: str
        """

        df_census.reset_index(inplace=True)
        df_census.rename(columns={'geo_name': geo_id}, inplace=True)
        df_1 = pd.merge(df_census, df_da, on=geo_id)
        df_1['geometry'] = df_1['geometry'].apply(wkt.loads)
        df_1a = gpd.GeoDataFrame(df_1, geometry='geometry')
        df_1a.crs = {'init': crs_in}
        df_1a = df_1a.to_crs({'init': projection_out})
        df_1a[geo_id] = df_1a[geo_id].astype(str)
        df_1a.set_index(geo_id, inplace=True)

        df_1a.columns = [x[0] if type(x) is tuple else x for x in df_1a.columns]
        j = json.loads(df_1a.to_json())
        df_1b = df_1a.drop('geometry', axis=1).copy()
        if geo_id not in df_1b.columns:
            df_1b.reset_index(inplace=True)
        return (df_1b, j)

    def plot_census_map(self, df_1, j, feature_value, colorscale,
                               mapbox_style, geo_id, lat, lon, renderer,
                               mapbox_style, mapbox_token=None):
        """
        mapbox_token: str
        """
        #TODO: add kwargs
        fig = go.Figure(go.Choroplethmapbox(geojson=j,
                                            locations=df_1[geo_id],
                                            z=df_1[feature_value],
                                            colorscale=colorscale,
                                            zauto=True,
                                            marker_opacity=0.5,
                                            marker_line_width=0.5)
                        )

        if mapbox_token is not None:
            fig.update_layout(mapbox_accesstoken=mapbox_token)

        fig.update_layout(mapbox_style=mapbox_style,
                          mapbox_zoom=12,
                          mapbox_center={'lat': lat, 'lon': lon})

        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        # TODO: add chrome as default browser
        #pio.renderers.default = 'browser'
        pio.renderers.default = 'iframe_connected'
        pio.renderers.default = renderer
        return fig

