def point_to_polygon(p):
    poly = geometry.Polygon(zip(p[0], p[1]))
    return poly


def in_radius(p, c):
    return p.within(c)


def distance_to_club(p, c):
    return c.distance(p) / 10 ** 6


def travel_path(*args):
    return geometry.LineString([(arg.x, arg.y) for arg in args])
