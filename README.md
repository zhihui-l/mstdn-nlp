# mstdn-nlp
This is the supporting repository for the SYSEN5260 `mstdn-nlp` project. 

Please see [ASSIGNMENT.md](./ASSIGNMENT.md). This is the document that describes what you need to do in this project.

# Usage
To get started, run:
```
docker-compose up
```

This will start up:

* [spark-master](http://localhost:8080/) -- This is your local Spark Cluster Manager.  To understand the deployment architecture of Spark, see [Cluster Overview](https://spark.apache.org/docs/3.4.0/cluster-overview.html).
* **spark-worker** -- This is a machine that spark-master delegates tasks to.  By default we just start one worker, but we can scale that up with `docker-compose scale spark-worker=4` ..for example.
* [Jupyter](http://localhost:8888/) -- This is an instance of JupyterLab that includes the latest Spark.  Inside JupyerLab, you can navigate to `sparkcode/hello-pyspark.ipynb` to see a demonstration of some basic Spark operations on your local cluster.  This should give you a headstart building out your analytics code.
* [REST](http://localhost:8000) -- This is a starting-point REST service for you to build on.  This uses FastAPI and contains an additional endpoint to demonstrate spark connectivity from the web-service: http://localhost:8000/sparktest
    * Note: You need to run the `hello-pyspark` notebook first before this service works: It depends on some data popupated by this notebook.


