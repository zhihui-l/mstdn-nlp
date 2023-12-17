##########################################
# Some codes are referenced from ChatGPT #
##########################################
from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.ml.linalg import VectorUDT, SparseVector, Vectors
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from typing import Union, List, Dict, Any # for Swagger 
import logging



logger = logging.getLogger("uvicorn")


app = FastAPI()

WAREHOUSE_PATH = "/opt/warehouse"

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("username", StringType(), True),
    StructField("filtered_words", ArrayType(StringType()), True),
    StructField("features", VectorUDT(), True)
])

spark_session = SparkSession.builder\
    .appName("RestAPI")\
    .master("spark://spark-master:7077")\
    .config("spark.executor.instances", 1) \
    .config("spark.cores.max", 2)\
    .config("spark.sql.warehouse.dir", WAREHOUSE_PATH) \
    .getOrCreate()

# Global variable to store the cached DataFrame
cached_df = None

def get_or_cache_df():
    global cached_df
    if cached_df is None:
        cached_df = spark_session.read.schema(schema).parquet(WAREHOUSE_PATH).cache()
    return cached_df

@app.get("/api/v1/accounts/")
async def get_accounts():
    tfidf_df = get_or_cache_df()
    # tfidf_df.printSchema()
    # Keep one instance of each duplicate account
    tfidf_df = tfidf_df.select('username', 'user_id')
    unique_accounts_df = tfidf_df.dropDuplicates()
    # Convert the DataFrame with unique accounts to Pandas DataFrame for easier manipulation
    unique_accounts_pd = unique_accounts_df.toPandas()
    # logger.info(unique_accounts_pd.to_string())
    accounts = unique_accounts_pd[['username', 'user_id']].to_dict(orient='records')
    return accounts

@app.get("/api/v1/tf-idf/user-ids/{user_id}")
def get_tfidf_for_user(user_id: str):
    # Read Parquet file with the defined schema
    tfidf_df = get_or_cache_df()
    tfidf_pd = tfidf_df.toPandas()
    # Filter DataFrame for the specified user_id
    user_df = tfidf_pd[tfidf_pd['user_id'] == user_id]

    if user_df.empty:
        raise HTTPException(status_code=404, detail="The user does not exist. Please enter a valid user ID.")

    # Aggregate TF-IDF vectors
    aggregated_vector = None
    for row in user_df.itertuples():
        if aggregated_vector is None:
            aggregated_vector = row.features
        else:
            # Element-wise addition of vectors
            aggregated_vector = Vectors.dense(aggregated_vector.toArray() + row.features.toArray())

    # Create a dictionary for words and their aggregated TF-IDF values
    tfidf_dict = {}
    if aggregated_vector is not None:
        for word, tfidf_value in zip(user_df['filtered_words'].explode(), aggregated_vector.toArray()):
            tfidf_dict[word] = tfidf_dict.get(word, 0) + tfidf_value

    return tfidf_dict

@app.get("/api/v1/tf-idf/user-ids/{user_id}/neighbors")
def get_neighbors_for_user(user_id: str):
    tfidf_df = get_or_cache_df()
    tfidf_pd = tfidf_df.toPandas()
    # Filter DataFrame for the specified user_id
    user_df = tfidf_pd[tfidf_pd['user_id'] == user_id]

    if user_df.empty:
        raise HTTPException(status_code=404, detail="The user does not exist. Please enter a valid user ID.")
    
    # Filter out the target user's TF-IDF vector
    target_user_vector = user_df['features'].iloc[0].toArray().reshape(1, -1)

    # Filter out the target user from the DataFrame and prepare the rest of the user vectors
    other_users = tfidf_pd[tfidf_pd['user_id'] != user_id]

    def pad_vector(vector, max_length):
        """ Pad the vector to max_length with zeros """
        current_length = vector.size
        if current_length < max_length:
            return np.append(vector, np.zeros(max_length - current_length))
        return vector
    
    # Find the maximum vector length
    max_vector_length = max(tfidf_pd['features'].apply(lambda vec: vec.size))

    # Pad vectors to the same length
    target_user_vector_padded = pad_vector(target_user_vector, max_vector_length).reshape(1, -1)
    other_user_vectors_padded = np.array(other_users['features'].apply(lambda vec: pad_vector(vec, max_vector_length)).tolist())

    # Compute cosine similarity
    similarities = cosine_similarity(target_user_vector_padded, other_user_vectors_padded)[0]

    # Get the indices of the top 10 similar users
    top_indices = np.argsort(similarities)[::-1][:10]

    # Retrieve and return the user_ids of the top similar users
    nearest_neighbors = list(other_users.iloc[top_indices]['user_id'].values)
    return nearest_neighbors
