from typing import Union

from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

@app.get("/sparktest")
def spark_test():
    from pyspark.sql import SparkSession
    spark_session = SparkSession.builder\
    .appName("rest-test")\
    .master("spark://spark-master:7077")\
    .config("spark.cores.max", 2)\
    .getOrCreate()
    try:
        wc2 = spark_session.read.parquet('/opt/warehouse/wordcounts.parquet/')
        wc2.createOrReplaceTempView("wordcounts")
        query = "SELECT * FROM wordcounts WHERE LEN(word) > 4 ORDER BY count DESC"
        ans = spark_session.sql(query).limit(10)
        list_of_dicts = ans.rdd.map(lambda row: row.asDict()).collect()
        return list_of_dicts
    finally:
        spark_session.stop()
