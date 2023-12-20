# mstdn-nlp
In this project your team will build a data-pipeline to perform the following steps:

1. Extract recent public "toots" from Mastodon servers and store those into a data-lake.
3. Use Hadoop/Spark to perform a TF-IDF analysis on the "toots".
4. Build a REST service to recommend Mastodon users to follow.

![components](mstdn-nlp.drawio.png)

In this project you will get everything running in a as docker containers on your local machine.  

* **Note:** Time and budget permitting, we'll deploy these components to AWS to see it run at scale. We'll want to keep this in mind as we develop to make sure our systems will port to the cloud without too much difficulty.


# What You Will Hand In
You will fork this repository and implement each of the components of this system as a separate docker-compose service. I should be able to run `docker-compose up` to bring up your entire system and see the Mastodon extractors start running, use the REST API to calculate a TF-IDF matrix, and POST a set of keywords and get a list of recommended Mastodon users.

You will also hand in a short written section, described below.

# Building the system
This section provides some more detail on the 3 main components that you will build and integrate.

## Toot Extractor
You will create a new docker-compose service called `extractor`. This will run a program that fetches the timeline from one or more Mastodon servers and adds it to our "data-lake." For now the data-lake will be implemented as a shared docker-volume called `datalake`. 

### Component Requirements

* This service should fetch the public-timeline object from one of the mastodon servers -- e.g. <https://mastodon.social/api/v1/timelines/public>, <https://fosstodon.org/api/v1/timelines/public>, etc.
* This service should perform a fetch every 30 seconds and add the new records to the data-lake.
* You should store these files in some way that makes it easy to load in aggregate by Spark (e.g. JSON files in a sub-directory with timstamp in the name. 

### Additional Hints

* The program your container runs should not exit or else the container shuts down. You need to make your initial command retrieve results, then wait in an infinite loop.
*  I recommend you write your program in a language that is supported by AWS Lambda because thisi is how we will deploy this component to the cloud.
* Organize your data to be easily consumed by Spark. For example, take advantage of the fact that spark-functions like  `spark.read.json()` accept wildcards in their paths to load all files and concatenate into a single dataframe.


## TF-IDF Matrix Calculator
You will write a pyspark application that reads the contents of your data-lake, and computes the TF-IDF matrix and store it in our `warehouse` volume -- e.g. as a parquet file.  See the Backgrounder below for more information on TF-IDF.


### Component Requirements

* This program should generate a matrix where each row represents the a Mastodon-user and each column represents a word from our vocabulary.
* You will implement a docker-service that uses pyspark to recompute the TF-IDF every 5 minutes.

### Additional Hints

* Consider what's easier: aggregating all text for each user before performing the term-count, or \
performing the term-count on each toot, then aggregating by user afterwards. 
* Also, consider filtering your text to remove terms that are not useful for the task of categorizing. Here are some ideas to try:
    * turn all input into lower-case.
    * Remove obvious non-words -- i.e. numbers, errant punctuation, etc.
    * Remove extremely rare words -- i.e. those that appears in only one document.
* Like your extractor, this program should run in an infinite loop.
* In order to run Spark you should base this container off of the spark image. You can look at `jupyter` or `rest` for an example to follow.
* You may need to store other metadata, e.g. to map rows to user-ids. All data you store should end up in the `warehouse` volume.  



## REST API
You will implement a REST API that exposes this system to our data-science team who will use this to analyze relationships between Mastodon users.

### Component Requirements
The RESTful resources you will implement include:
* `mstdn-nlp/api/v1/accounts/` -- Lists all of the known matsodon accounts in our data-set.  This should return a list of dictionaries with username and id for each known account -- e.g.
    ```
    [
        {
            "username": "dahlia",
            "id": "109246474478239584"
        },
        {
            "username": "retrohondajunki",
            "id": "109940330642741479"
        },
        ...
    ]
    ```
* `/api/v1/tf-idf/user-ids/<user_id>` -- This should return the TF-IDF matrix row for the given mastodon user as a dictionary with keys = vocabulary words, and values = TF-IDF values.
* `/api/v1/tf-idf/user-ids/<user_id>/neighbors` -- This should return the 10 nearest neighbors, as measured by the cosine-distance between the users's TF-IDF matrix rows.

### Additional Hints

* You are provided with a `rest` service as a starting-point that demonstrates how to make Spark calls from a FastAPI service. Feel free to use this as a starting-point. If you have a different REST framework that you prefer, feel free to use that instead.


# Written Section
In addition to the working docker-compose stack, please also hand in a written report addressing the following points.

## How does your solution use Map-Reduce?
PySpark abstracts away many of the details of the map-reduce architecture to provide either a SQL-like or a DataFrame-like interface to the data-scientist.

Based on your implementation, please try to identify what specific steps performed by mappers, reducers, combiners, etc.

## Spark Data Sharing
You'll notice that if you try to perform Spark operations on files that are not in `/opt/datalake` or `/opt/warehouse`, the operations typically fail.  For example if you were to try to perform analysis on a file `/tmp/as-you-like-it.txt` Spark complains that the file is not found.  Why is that so?

## Scaling up
In your local environment, you can increase the number of worker containers by running:

```
docker-compose scale spark-worker=6
```

Try running a task with one worker active.  Then try again with 6 workers active.

What is the impact of the extra workers on the task's completion time?  Can you explain why?

## Online Processing
You'll notice that, although the spark session does allow us to get responses somewhat quickly, these tools seem better suited to batch processes.  Can you identify some other open-source systems that may help improve our REST response times?  How would you change the architecture to incorporate these systems?


# Grading

## How You'll Get an A
Here are some guidelines on how I'll grade this:

* I can clone your repo, run `docker-compose up` and see:
    * Your extractor add new data to the data-lake every 30 seconds.
    * Your TF-IDF calculator updates every 5 minutes.
    * I can access your REST service at `http://localhost:9090/api/v1/tf-idf/user-ids/<user_id>` and `/api/v1/tf-idf/user-ids/<user_id>/neighbor`.
* Your REST resources behave as expected.
* Thoughtful answers to the written section.
* Your REST resources can handle error-conditions -- e.g. They return appropriate status-codes, and don't cause any bad side-effects (e.g. REST server crashing!)
* You make good use of Spark functionality:
    * Your PySpark code is scalable because it takes advantage of the map-reduce architecture:  Imagine as the size of the toot data-lake grows, we want to be able to achieve constant run-time by increasing the size of our cluster.
    * You make good choices about what Spark features to use to solve your problem.
* Your system and all of its components are as simple as possible.
* Your results are reasonably correct.
* You cite any references you used to come up with your solution.  You don't have to invent everything yourself, but the solutions you take from books, blogs, StackOverflow, ChatGPT, etc. all need to be cited.

## How You'll Earn Extra Credit
* Add Swagger docs to your REST service. (See [here](https://fastapi.tiangolo.com/advanced/extending-openapi/).)
* Make your REST service always respond within 1 second. Some ideas:
    * Maybe a connection-pool pattern for the SparkSession object would eliminate needless start-up time. 
    * If the task is still slow consider redesigning the API to use a Resource-Collection pattern -- e.g. The client posts calculation-requests to the `neighbor-calcs/`, then polls on completion.  The work still takes a long time but no endpoint holds a socket for more than a second.
* Add addtional extractors that pull from different Mastodon servers. (See the backgrounder below if that doesn't make sense.)

# Backgrounder
This section provides some additional background on this project.

## What is Mastodon?
Mastodon is a free open-source social-networking platform that provides an experience similar to Twitter.  Instead of "Tweets" Mastodon has "Toots."  Because it's free, the data is publicly available which makes it nice for projects like this.

Also unlike Twitter, there are many Mastodon servers that all run and exchange messages. You can find a list of popular servers [here](https://joinmastodon.org/servers).

For a more detailed understanding, have a look at the [Mastodon Wikipedia Page](https://en.wikipedia.org/wiki/Mastodon_(social_network)),

## What is TF-IDF?
TF-IDF is a Natural Language Processing technique used to tag a large collection of documents by topic.  TF refers to "term-frequency" where "term" means "word."  We can calculate a Term-Frequency Matrix by counting the number of times each word appears in each document.  This gives us a matrix with one row per document, and one column per word in our vocabulary.

The "Document Frequency" refers to the number of documents a term appears in.  For example we expect a word like "the" or "as" to appear in just about every document. As such we divide the term-frequencies by the Document Frequency to deemphasize common words.  

Jurafsky and Martin's excellent and open-source ["Speech and Language Processing"](https://web.stanford.edu/~jurafsky/slp3/) book provides an introduction to this topic in [Section 6.5](https://web.stanford.edu/~jurafsky/slp3/6.pdf).

We are using TF-IDF because it is a natural fit for a map-reduce architecture.  As such you should find many examples and resources on the internet showing you how to apply Spark to the TF-IDF problem. 


