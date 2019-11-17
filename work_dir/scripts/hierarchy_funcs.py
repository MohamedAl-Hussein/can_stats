from numba import jit
import pandas as pd


@jit(nopython=False)
def update_parents(df, col):
    """
    Collapse tabular based hierarchy into single row for each unique
    relationship.

    e.g.
    Total
      0 to 14
        0 to 4

    Total//0 to 14//0 to 4
    """
    df = df.copy()
    df['parents'] = ""
    hierarchy_dict = dict()
    i = 0
    while i < len(list(df[col])):
        hierarchy_value = len(str(df.iloc[i][col])) -\
                          len(str(df.iloc[i][col]).lstrip())
        df, hierarchy_dict = find_parents(hierarchy_dict, i, hierarchy_value,
                                          df)
        i += 1

    return df, hierarchy_dict


@jit(nopython=False)
def find_parents(hierarchy_dict, hierarchy_index, hierarchy_value, df):
    """
    Deal with labelling of most-recent parent/hierarchy for each row, to be
    used by update_parents.

    e.g.
    hierarchy = {0: 12, 2: 14, ... }
    """
    if hierarchy_value not in hierarchy_dict.keys():
        hierarchy_dict[hierarchy_value] = hierarchy_index
    elif hierarchy_index > hierarchy_dict[hierarchy_value]:
        hierarchy_dict[hierarchy_value] = hierarchy_index

    if hierarchy_value == 0:
        df.at[hierarchy_index, 'parents'] = (
            df.iloc[hierarchy_dict[hierarchy_value]]['Topic']
        )
    else:
        df.at[hierarchy_index, 'parents'] = (
            df.iloc[hierarchy_dict[hierarchy_value - 2]]['parents'] +
            "//" +
            df.iloc[
                hierarchy_dict[hierarchy_value - 2]
            ]['Characteristics'].strip()
        )

    return df, hierarchy_dict


@jit(nopython=False)
def trim_parents(df, col):
    """
    Remove each end of the complete paths produced by update_parents in order
    to assign unique indeces for parents and children.

    e.g.
    Total//0 to 14//0 to 4

    parent = Total
    child = 0 to 4
    """
    df = df.copy()
    df['full_path'] = ''
    df['trimmed_child'] = ''
    df['trimmed_parent'] = ''
    i = 0
    while i < len(list(df[col])):
        df.at[i, 'full_path'] = (
            df.iloc[i]['parents'] + "//" + df.iloc[i]['Characteristics'].strip()
        )
        dash = df.iloc[i]['full_path'].rfind("//")
        child = df.iloc[i]['full_path']
        parent = df.iloc[i]['full_path'][:dash]

        df.at[i, 'trimmed_parent'] = parent
        df.at[i, 'trimmed_child'] = child
        i += 1

    return df


@jit(nopython=False)
def generate_id(df, child, parents):
    """
    Use the trimmed paths produced by trim_parents to produce unique ids
    that will point to each row's parent and child.

    e.g.
    ids = {'Total': 10, '0 to 14': 12, ... }
    child    parent  primary_key  foreign_key
    0 to 14  Total       12          10
    Total    0 to 14     10          8
    """
    df = df.copy()
    df['primary_key'] = None
    df['foreign_key'] = None
    id_counter = 1
    i = 0
    j = 0
    id_dict = dict()
    while i < len(list(df[child])):
        if str(df.iloc[i][child]).strip() not in id_dict.keys():
            id_dict[str(df.iloc[i][child]).strip()] = id_counter
            id_counter += 1
        if str(df.iloc[i][parents]).strip() not in id_dict.keys():
            id_dict[str(df.iloc[i][parents]).strip()] = id_counter
            id_counter += 1

        i += 1

    while j < len(list(df[child])):
        df.at[j, 'primary_key'] = id_dict[str(df.iloc[j][child]).strip()]
        df.at[j, 'foreign_key'] = id_dict[str(df.iloc[j][parents]).strip()]

        j += 1

    return df, id_dict

