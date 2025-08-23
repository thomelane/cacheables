class WriteException(Exception):
    pass


class ReadException(Exception):
    pass


class LoadException(Exception):
    pass


class DumpException(Exception):
    pass


class InputKeyNotFoundError(Exception):
    pass


class CacheNotEnabledError(Exception):
    pass
