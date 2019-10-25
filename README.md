# ID2221_Project

Final Project of KTH Course ID2221. 

It is a Spark Streaming application. It utilizes the Twitter API to stream tweets based on a filter, transforms the DStream, applies sentiment analysis and stores the analysis on Cassandra. The results are fetched from Cassandra and visualized.


Run with command "sbt run -Dsbt.classloader.close=false" on the root folder of the project.
