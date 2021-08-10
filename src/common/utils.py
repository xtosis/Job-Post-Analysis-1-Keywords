import pickle


def PICKLE_LOAD(path):
    with open(path, 'rb') as f:
        ret = pickle.load(f)
    return ret


def PICKLE_SAVE(path, data):
    with open(path, 'wb') as f:
        pickle.dump(data, f)
