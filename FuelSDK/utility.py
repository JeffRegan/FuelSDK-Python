
def prune_dict(dictionary):
    res = {}
    res.update((a, b) for a, b in dictionary.iteritems() if b is not None)
    return res
