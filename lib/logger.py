class Log4j(object):
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("sbdl")

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)

#
# Spark uses Log4J for logging. So it is a good idea to reuse the same log4j that Spark uses.
#And this package does the necessary things to get the Log4J instance from Spark and set it up for our application.
# We will be using this logger in our application. The Log4J also requires a log4j.properties file.