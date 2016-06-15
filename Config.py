import ConfigParser
import os

class Config(object):

    def __init__(self):
        self.section = []

    def ConfigSectionMap(self, section):
        config = ConfigParser.ConfigParser()
        #print(os.path.join(os.getcwd(),'properties'))

        with open(os.path.join(os.getcwd(),'properties'),'r') as configfile:
           config.readfp(configfile)
        dict = {}
        options = config.options(section)
        for option in options:
            try:
                dict[option] = config.get(section, option)
                if dict[option] == -1:
                    DebugPrint("skip: %s" % option)
            except:
                print("exception on %s!" % option)
                dict[option] = None
        return dict