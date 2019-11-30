def replace_num_typos(x, mapping):
    for k in mapping.keys():
        x = x.replace(k, mapping[k])
    return x
