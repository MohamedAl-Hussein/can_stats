import numpy as np
import json
import geog


def create_circle(x, y, radius, n_points=360):
    p = geometry.Point([x, y])
    n_points = 360
    d = radius * 1000
    angles = np.linspace(0, 360, n_points)
    polygon = geog.propagate(p, angles, d)

    shape = json.dumps(geometry.mapping(geometry.Polygon(polygon)))
    shape = json.loads(shape)

    lon = list()
    lat = list()

    for coord in range(len(shape['coordinates'][0])):
        lon.append(shape['coordinates'][0][coord][0])
        lat.append(shape['coordinates'][0][coord][1])

    return (lon, lat)

