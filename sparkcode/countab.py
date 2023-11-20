"""Trivial program to count occurances of ORLANDO and OLIVER in "As You Like It."

Usage:

../bin/spark-submit  --master spark://spark-master:7077 countab.py
"""
from pyspark.sql import SparkSession

logFile = "/opt/data/as-you-like-it.txt"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('ORLANDO')).count()
numBs = logData.filter(logData.value.contains('OLIVER')).count()

print("Lines with Orlando: %i, lines with Oliver: %i" % (numAs, numBs))

spark.stop()