def point_to_polygon(p):
    """
    Convert array of coordinates into a polygon to use as geometry column
    in dataframe.
    NOTE: column name must be geometry

    p: array containing points
    """

    poly = geometry.Polygon(zip(p[0], p[1]))
    return poly


def in_radius(p, c):
    return p.within(c)


def distance_to_club(p, c):
    return c.distance(p) / 10 ** 6


def travel_path(*args):
    return geometry.LineString([(arg.x, arg.y) for arg in args])
