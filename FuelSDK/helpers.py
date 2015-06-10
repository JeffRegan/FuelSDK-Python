
def prune_dict(dict):
    res = {}
    res.update((a, b) for a, b in dict.iteritems() if b is not None)
    return res
