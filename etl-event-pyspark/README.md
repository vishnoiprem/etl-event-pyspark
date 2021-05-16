ETL pipeline using Apache Spark SQL


install : hive to local 

https://luckymrwang.github.io/2018/03/14/Install-hive-on-Mac-with-Homebrew/


Why Spark SQL?
Spark SQL brings native support for SQL to Spark and streamlines the process of querying data stored both in RDDs and in external sources. Spark SQL conveniently blurs the lines between RDDs and relational tables. Unifying these powerful abstractions makes it easy for developers to intermix SQL commands querying external data with complex analytics, all within in a single application. Spark SQL is used in this case study to:

Run SQL queries over imported JSON data and existing RDDs
Easily write RDDs out to Hive tables


Scalability
Spark SQL also includes a cost-based optimizer, columnar storage, and code generation to make queries fast. At the same time, it scales to thousands of nodes and multi-hour queries using the Spark engine, which provides full mid-query fault tolerance, without having to worry about using a different engine for historical data.

Implementation
This project aims to do 3 sets of transformations. A cleansing task is done initially inorder to maintain uniformity in data pattern. (Urls were not following a uniform pattern where https:// is missing for a few records)


spark-submit --class com.etl.exec.EventTransform {jar_location} {input_data_location} {output_table_location}

Example: spark-submit --class com.etl.exec.EventTransform /Users/mathewir/Desktop/Irene/hobby-projects/etl-spark/target/scala-2.11/etl-spark-assembly-0.1.jar /Users/mathewir/Desktop/source_event_data.json /Users/mathewir/Desktop/output/