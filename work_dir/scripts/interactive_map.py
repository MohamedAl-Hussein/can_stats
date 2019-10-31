import json
import geopandas as gpd
import os
import pandas as pd
import plotly.graph_objs as go
from configparser import ConfigParser, ExtendedInterpolation
from shapely import wkt

from data_funcs import DataFuncs
from create_shape import create_circle


parser = ConfigParser(interpolation=ExtendedInterpolation())
parser.read('dev.ini')

with open(parser.get('data', 'data_settings')) as json_file:
    data_settings = json.load(json_file)

with open(parser.get('data', 'map_settings')) as json_file:
    map_settings = json.load(json_file)

with open(parser.get('data', 'coordinates')) as json_file:
    coordinates = json.load(json_file)


if __name__ == "__main__":
#-----------------------------------------------------------------------------#
    # GENERAL SETUP
    epsg_in = map_settings['projection_in']
    colorscale = map_settings['colorscale']
    mapbox_style = map_settings['mapbox_style']
    geo_id = data_settings['geo_id']
    epsg_out = map_settings['projection_out']

    df_1 = pd.read_csv(os.path.join(os.getcwd(), parser.get('data', 'census')))
    df_2 = pd.read_csv(os.path.join(os.getcwd(), parser.get('data', 'stats')))
    df_1a = df_1[[
        'geo_name', 'edited_characteristics', 'total', 'male', 'female'
    ]].copy()

#-----------------------------------------------------------------------------#
    # OTTAWA LIM 0 TO 17 YEARS OLD PERCENTAGE VS TOTAL AMOUNT 
    cat_1 = cat_2 = data_settings['categories'][1]
    div_by = data_settings['categories'][0]
    val_1 = f'total - {cat_1}'
    val_2 = 'percent'
    cats = [(cat_1, val_1), (cat_2, val_2)]

    data = list()

    for cat in cats:
        df_1b = df_1a.copy()
        df_1b = df_1b[df_1b['edited_characteristics'] == cat[0]]

        df_1b = df_1a.pivot(index='geo_name',
                            columns='edited_characteristics',
                            values=['total', 'male', 'female'])
        df_1b['percent'] = (
            df_1b[( 'total', cat[0])] /
            df_1b[( 'total', div_by)]
        )
        df_1b.reset_index(inplace=True)
        df_1b.columns = [x[0] + ' - ' + x[1] if type(x) is tuple and
                         x[1] is not '' else x for x in df_1b.columns]
        df_1b.columns = [x[0] if type(x) is tuple else x for x in df_1b.columns]
        df_1b = df_1b[['geo_name', 'percent', f'total - {cat[0]}']]

        lim = DataFuncs()
        lim_df, lim_json = lim.extract_census_feature(df_1b, df_2, epsg_in,
                                                      epsg_out)

        lim_df = lim_df[lim_df['percent'] < 1]
        lim_df['percent'] = lim_df['percent'].apply(lambda x: round(x, 2))

        data.append(go.Choroplethmapbox(geojson=lim_json,
                                        locations=lim_df[geo_id],
                                        z=lim_df[cat[1]],
                                        colorscale=colorscale,
                                        zauto=True,
                                        marker_opacity=0.5,
                                        marker_line_width=0.5,
                                        visible=False))
    data[0]['visible'] = True

    fig = go.Figure(data=data)

    fig.update_layout(
        width=1920,
        height=1080,
        autosize=True,
        margin=dict(t=0, b=0, l=0, r=0),
        template='plotly_white'
    )

    coord_list = [('x', coordinates['points'][0][::-1]),
                  ('y', coordinates['points'][1][::-1])]

    radii = [('0.5km', 0.5), ('1.0km', 1.0), ('1.5km', 1.5), ('2.0km', 2.0),
             ('2.5km', 2.5), ('3.0km', 3.0)]
    coord_dict = dict()
    show_dict = dict()

    for coord in coord_list:
        for radius in radii:
            lon, lat = create_circle(coord[1][0], coord[1][1], radius[1])

            k = coord[0] + ' - ' + radius[0]
            coord_dict[k] = go.Scattermapbox(
                fill='toself',
                lon=lon, lat=lat,
                marker=dict(size=0, color='orange'),
                visible=False
            )

            switch = coord[0] + ' - ' + radius[0]
            show_dict[switch] = [False for i in range(len(coord_list *
                                                          len(radii)))]
            show_dict[switch].insert(0, True)
            show_dict[switch].insert(1, False)
            if coord_list.index(coord) == 0:
                show_dict[switch][coord_list.index(coord) +
                                  radii.index(radius) +
                                  len(coord_list)] = True
                show_dict[switch][coord_list.index(coord) +
                                  radii.index(radius) +
                                  len(coord_list) +
                                  len(radii)] = True
            else:
                show_dict[switch][coord_list.index(coord) +
                                  radii.index(radius) +
                                  len(coord_list) +
                                  len(radii) -
                                  1] = True

    for k in coord_dict.keys():
        fig.add_trace(coord_dict[k])

    steps = list()
    for i in range(len(fig.data[2:8])):
        step = dict(
            method='restyle',
            args=['visible', [True, False] + [False] * len(fig.data[2:])],
        )
        step['args'][1][i + 2] = True
        step['args'][1][i + 8] = True
        steps.append(step)

    sliders = [dict(
        active=7,
        steps=steps[:6]
    )]

    fig.update_layout(
        sliders=sliders
    )

    button_layer_1_height = 1.08
    fig.update_layout(
        updatemenus=[
            go.layout.Updatemenu(
                buttons=list([
                    dict(
                        args=['visible', [True, False]],
                        label=data_settings['labels'][0],
                        method='restyle'
                    ),
                    dict(
                        args=['visible', [False, True]],
                        label=data_settings['labels'][1],
                        method='restyle'
                    )
                ]),
                direction='down',
                pad=dict(r=10, t=10),
                showactive=True,
                x=0.1,
                xanchor='left',
                y=button_layer_1_height,
                yanchor='top'
            ),
            go.layout.Updatemenu(
                buttons=list([
                    dict(
                        args=['colorscale', colorscale],
                        label='Viridis',
                        method='restyle'
                    ),
                    dict(
                        args=['colorscale', 'Cividis'],
                        label='Cividis',
                        method='restyle'
                    ),
                    dict(
                        args=['colorscale', 'Blues'],
                        label='Blues',
                        method='restyle'
                    ),
                ]),
                direction='down',
                pad=dict(r=10, t=10),
                showactive=True,
                x=0.37,
                xanchor='left',
                y=button_layer_1_height,
                yanchor='top'
            ),
            go.layout.Updatemenu(
                buttons=list([
                    dict(
                        args=['reversescale', False],
                        label='False',
                        method='restyle'
                    ),
                    dict(
                        args=['reversescale', True],
                        label='True',
                        method='restyle'
                    ),
                ]),
                direction='down',
                pad=dict(r=10, t=10),
                showactive=True,
                x=0.58,
                xanchor='left',
                y=button_layer_1_height,
                yanchor='top'
            ),
            go.layout.Updatemenu(
                active=0,
                buttons=list([
                    dict(
                        args=[dict(
                            visible=show_dict['x - 0.5km'],
                            marker=dict(
                                color='LightSkyBlue',
                                opacity=0.2,
                                size=0
                            )
                        )],
                        label='0.5km Radii',
                        method='update'
                    ),
                    dict(
                        args=[dict(visible=show_dict['x - 1.0km'])],
                        label='1km Radii',
                        method='update'
                    ),
                    dict(
                        args=[dict(visible=show_dict['x - 1.5km'])],
                        label='1.5km Radii',
                        method='update'
                    ),
                    dict(
                        args=[dict(visible=show_dict['x - 2.0km'])],
                        label='2km Radii',
                        method='update'
                    ),
                    dict(
                        args=[dict(visible=show_dict['x - 2.5km'])],
                        label='2.5km Radii',
                        method='update'
                    ),
                    dict(
                        args=[dict(visible=show_dict['x - 3.0km'])],
                        label='3km Radii',
                        method='update'
                    ),
                ]),
                direction='down',
                pad=dict(r=10, t=10),
                showactive=True,
                x=0.78,
                xanchor='left',
                y=button_layer_1_height,
                yanchor='top'
            )
        ]
    )

    fig.update_layout(
        annotations=[
            go.layout.Annotation(text='Data Measure', x=0, xref='paper',
                                 y=1.06, yref='paper', align='left',
                                 showarrow=False),
            go.layout.Annotation(text='colorscale', x=0.25, xref='paper',
                                 y=1.07, yref='paper', align='left',
                                 showarrow=False),
            go.layout.Annotation(text='Reverse<br>Colorscale', x=0.54,
                                 xref='paper', y=1.06, yref='paper',
                                 align='left', showarrow=False),
            go.layout.Annotation(text='Target Radius', x=0.75,
                                 xref='paper', y=1.06, yref='paper',
                                 align='left', showarrow=False),
        ]
    )

    fig.update_layout(
        mapbox=dict(style=mapbox_style,
                    zoom=10,
                    center=dict(
                        lat=map_settings['center'][0],
                        lon=map_settings['center'][1]
                    ))

    )

    fig.show()

