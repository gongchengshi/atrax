import pickle
from base64 import b64encode, b64decode
from atrax.frontier.exceptions import FrontierMessageCorrupt


def pack_message(url_info):
    return b64encode(pickle.dumps(url_info))


def unpack_message(m):
    try:
        return pickle.loads(b64decode(m))
    except Exception:
        raise FrontierMessageCorrupt(m)
