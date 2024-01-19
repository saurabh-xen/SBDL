import configparser
from pyspark import SparkConf


def get_config(env):
    config = configparser.ConfigParser()
    config.read("conf/sbdl.conf")
    conf = {}
    for (key, val) in config.items(env):
        conf[key] = val
    return conf


def get_spark_conf(env):
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("conf/spark.conf")

    for (key, val) in config.items(env):
        spark_conf.set(key, val)
    return spark_conf


def get_data_filter(env, data_filter):
    conf = get_config(env)
    return "true" if conf[data_filter] == "" else conf[data_filter]

#ConfigParser is a Python standard library module that allows parsing configuration files in INI format.

#The get_config() function will read the sbdl.conf file, place all the configurations in a python directory, and return it.
#The idea is to read sbdl.conf only once at the start of the application and keep all the configurations in memory so we can use it wherever needed.
#The get_config() also takes the current environment as input. The values for the environment could be local, QA, or prod.
# The function will read the appropriate section from the sbdl.conf according to the current environment.

#the get_spark_conf(). This guy is similar to the get_config().
# This function creates a SparkConf object and adds all the configurations in SparkConf.We are not keeping the spark.conf values in a Python dictionary.
# Spark configurations are used only once at the time of creating a SparkSession. And the SparkSession directly uses the SparkConf object.
# So we are directly loading it into the SparkConf object.

#the get_data_filter(). It will help us to build a where clause at runtime basically filter the data.