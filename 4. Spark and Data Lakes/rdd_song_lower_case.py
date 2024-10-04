 # this is part of Udacity Data Engineering Nanodegree Coursework

from pyspark.sql import SparkSession

# Because we aren't running on a spark cluster, the session is just for development
spark = SparkSession \
    .builder \
    .appName("Maps and Lazy Evaluation Example") \
    .getOrCreate()


# Starting off with a regular python list
log_of_songs = [
        "Despacito",
        "Nice for what",
        "No tears left to cry",
        "Despacito",
        "Havana",
        "In my feelings",
        "Nice for what",
        "despacito",
        "All the stars"
]

# parallelize the log_of_songs to use with Spark
# distributed_song_log_rdd is an RDD (Reslient Distributed Dataset)
distributed_song_log_rdd = spark.sparkContext.parallelize(log_of_songs)

# notice we DO NOT use the .collect() method. What is the difference between
# .collect() and .foreach() ? 
# .collect() forces all the data from the entire RDD on all nodes 
# to be collected from ALL the nodes, which kills productivity, and could crash
# .foreach() allows the data to stay on each of the independent nodes

# show the original input data is preserved
distributed_song_log_rdd.foreach(print)

def convert_song_to_lowercase(song):
    return song.lower()

print(convert_song_to_lowercase("Havana"))

lower_case_songs=distributed_song_log_rdd.map(convert_song_to_lowercase)
lower_case_songs.foreach(print)

# Show the original input data is still mixed case
distributed_song_log_rdd.foreach(print)

# Use lambda functions instead of named functions to do the same map operation
distributed_song_log_rdd.map(lambda song: song.lower()).foreach(print)
distributed_song_log_rdd.map(lambda x: x.lower()).foreach(print)
