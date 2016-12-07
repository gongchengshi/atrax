def D(urlInfo, resp):
    pass


def Ignored(urlInfo, resp):
    # Save headers
    pass


def Successful(urlInfo, resp):
    pass


def Created(urlInfo, resp):
    pass


def Unknown(urlInfo, resp):
    pass


ResponseHandlers = {
    100: Ignored, # continue
    101: Ignored, # switching protocols
    200: Successful,
    201: Created,
    202: Ignored, # accepted
    203: Successful,
    204: Ignored, # no content
    205: Ignored, # reset content
    206: Ignored, # partial content
    300: D,
    301: D,
    302: D,
    303: D,
    304: D,
    305: D,
    306: D,
    307: D,
    400: D,
    401: D,
    402: D,
    403: D,
    404: D,
    405: D,
    406: D,
    407: D,
    408: D,
    409: D,
    410: D,
    411: D,
    412: D,
    413: D,
    414: D,
    415: D,
    416: D,
    417: D,
    500: D,
    501: D,
    502: D,
    503: D,
    504: D,
    505: D
}


def HandleResponse(urlInfo, resp):
    if resp.getcode() not in ResponseHandlers:
        Unknown(urlInfo, resp)
    else:
        ResponseHandlers[resp.getcode()](urlInfo, resp)
