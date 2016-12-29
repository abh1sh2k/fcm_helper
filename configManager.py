import os, sys
import jprops


class configManager():

    """
    Initializer method
    """
    def __init__(self):
        self._properties = None

        self.getProperties()
    """
    get properties from <xyz>.properties file
    """
    def getProperties(self):
        filepath = 	os.path.dirname(os.path.realpath(__file__))
        filename = filepath + '/config.properties'

        with open(filename) as fp:
            self._properties = jprops.load_properties(fp)


