

class ConfigException(Exception):
    '''There was an error in pulling in the config information'''
    pass


class RsyncMirroringException(Exception):
    """Failed to rsync one of the configured items"""
    pass