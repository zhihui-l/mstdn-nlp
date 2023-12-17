from pyspark.sql import SparkSession
import os, json, time
from pyspark.sql.functions import col, concat_ws, collect_list, lower, regexp_replace, udf, lit
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF, CountVectorizer, NGram
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.linalg import SparseVector, DenseVector
from pyspark.sql.types import StructType, StructField, StringType, Row
from pyspark.mllib.feature import HashingTF

def main():
    spark_session = SparkSession.builder \
            .appName("TF-IDF Calculator") \
            .master("spark://spark-master:7077") \
            .config("spark.executor.instances", 1) \
            .config("spark.cores.max", 2) \
            .config("spark.sql.warehouse.dir", "/opt/warehouse/") \
            .getOrCreate()
    while True:
        for filename in os.listdir('/opt/datalake'):
            file_path = os.path.join('/opt/datalake', filename)
            
            # Check if the current object is a file
            if os.path.isfile(file_path):
                # print(f"Processing file: {file_path}")
                
                df = spark_session.read.json(file_path)
                df = df.select(col("account.id").alias("user_id"), 
                    col("account.username").alias("username"), 
                    lower(regexp_replace("content", "[^\\w\\s]", "")).alias("content"))
                # df = df.groupBy("user_id", "username").agg(concat_ws(" ", collect_list("content")).alias("aggregated_content"))
                # Drop missing values
                df = df.na.drop(subset=["user_id", "username", "content"])
                # Tokenize the content
                tokenizer = Tokenizer(inputCol="content", outputCol="words")
                wordsData = tokenizer.transform(df)
                
                # Remove stopwords
                remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
                wordsData = remover.transform(wordsData)
                
                # Count the frequency of each word and filter out rare words
                cv = CountVectorizer(inputCol="filtered_words", outputCol="cv_features", minDF=2)
                cvModel = cv.fit(wordsData)
                filteredWordsData = cvModel.transform(wordsData)
                # filteredWordsData.show()
                # Compute TF-IDF
                idf = IDF(inputCol="cv_features", outputCol="features")
                idfModel = idf.fit(filteredWordsData)
                rescaledData = idfModel.transform(filteredWordsData)
                # rescaledData.show()
                # Select the required columns (user id, username, and features)
                tfidf_matrix = rescaledData.select("user_id", "username", "filtered_words", "features")
                tfidf_matrix.show(truncate=False)
                tfidf_matrix.write.mode("append").parquet("/opt/warehouse")
                # Remove processed file
                os.remove(file_path)
        time.sleep(300)
        
if __name__ == "__main__":
    main()

