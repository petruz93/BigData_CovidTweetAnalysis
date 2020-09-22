import configparser
from os import path


conf = configparser.ConfigParser()
conf.read(path.join(path.dirname(__file__), 'config.cfg'))

def getConfigValue(section, key):
    return conf.get(section, key)
