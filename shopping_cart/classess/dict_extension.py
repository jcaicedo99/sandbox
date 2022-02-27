class dict_extension(dict):
    def __missing__(self, key):
        msg = "Key value [{0}] not found in dictionary".format(key)
        raise KeyError(msg)
    
    

d = dict_extension()
print(d["x"])
