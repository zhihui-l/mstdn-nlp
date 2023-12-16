from typing import Union

from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.ml.linalg import VectorUDT
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from typing import Union, List, Dict, Any # for Swagger 
from fastapi.testclient import TestClient



app = FastAPI()

# WAREHOUSE_PATH = "/opt/warehouse"
# schema = StructType([
#     StructField("id", StringType(), True),
#     StructField("username", StringType(), True),
#     StructField("filtered_words", ArrayType(StringType()), True),
#     StructField("features", VectorUDT(), True)
# ])

# spark_session = SparkSession.builder\
#     .appName("RestAPI")\
#     .master("spark://spark-master:7077")\
#     .config("spark.executor.instances", 1) \
#     .config("spark.cores.max", 2)\
#     .config("spark.sql.warehouse.dir", WAREHOUSE_PATH) \
#     .getOrCreate()
# tfidf_df = spark_session.read.schema(schema).parquet(WAREHOUSE_PATH)
@app.get("/api/v1/accounts/")
# @app.get("/api/v1/accounts/", response_model=List[Dict[str, Union[str, int]]]) # for Swagger API
def get_accounts():
    # tfidf_df = spark_session.read.schema(schema).parquet(WAREHOUSE_PATH)
    
    # # Convert TF-IDF matrix to Pandas DataFrame for easier manipulation
    # tfidf_pd = tfidf_df.toPandas()
    # accounts = tfidf_pd[['username', 'id']].to_dict(orient='records')
    # return accounts
    print("llllllllllllllll")
    return {"Hello": "World"}

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}

# @app.get("/sparktest")
# def spark_test():
#     from pyspark.sql import SparkSession
#     spark_session = SparkSession.builder\
#     .appName("rest-test")\
#     .master("spark://spark-master:7077")\
#     .config("spark.cores.max", 2)\
#     .getOrCreate()
#     try:
#         wc2 = spark_session.read.parquet('/opt/warehouse/wordcounts.parquet/')
#         wc2.createOrReplaceTempView("wordcounts")
#         query = "SELECT * FROM wordcounts WHERE LEN(word) > 4 ORDER BY count DESC"
#         ans = spark_session.sql(query).limit(10)
#         list_of_dicts = ans.rdd.map(lambda row: row.asDict()).collect()
#         return list_of_dicts
#     finally:
#         spark_session.stop()
# client = TestClient(app)
# response = client.get("/api/v1/accounts/")
# print(response.status_code)
# print(response.json())
