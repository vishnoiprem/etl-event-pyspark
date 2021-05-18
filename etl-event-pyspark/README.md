ETL pipeline using Apache Spark SQL


1)install requirements.txt file

sudo pip3 install -r requirements.txt




2)install : hive to local ( if want to check data )

https://luckymrwang.github.io/2018/03/14/Install-hive-on-Mac-with-Homebrew/

First step is validate json data 
cd /etl-event-pyspark

python src/main/validate_json.py ${json_data_file} ${json_schema_file}

eg:
python src/main/validate_json.py "/Users/vishnoiprem/OwnProject/OwnPoc/Learning/17-etl/etl-event-pyspark/src/main/resources/input/source_event_data.json" "/Users/vishnoiprem/OwnProject/OwnPoc/Learning/17-etl/etl-event-pyspark/src/main/resources/input/source_data_schema.json"



3)
Why Spark SQL?

Spark SQL brings native support for SQL to Spark and streamlines the process of querying data stored both in DataFrame  and in external sources. 
Spark SQL conveniently blurs the lines between Dataframe and relational tables.
Unifying these powerful abstractions makes it easy for developers to intermix SQL commands querying external data with complex analytics, 
all within in a single application.
Spark SQL is used in this case study to:

Run SQL queries over imported JSON data and existing dataframes
Easily write RDDs out to Hive tables


Scalability:
Spark SQL also includes a cost-based optimizer, columnar storage, and code generation to make queries fast. At the same time, it scales to thousands of nodes and multi-hour queries using the Spark engine, which provides full mid-query fault tolerance, without having to worry about using a different engine for historical data.

Implementation:
This project aims to do 3 sets of transformations. A cleansing task is done initially inorder to maintain uniformity in data pattern. (Urls were not following a uniform pattern where https:// is missing for a few records)

4)



spark-submit src/main/EventTransform.py  ${input_data_location} ${outpuETL pipeline using Apache Spark SQL



Run SQL queries over imported JSON data and existing dataframes

spark-submit src/main/EventTransform.py  ${input_data_file} ${output_table_location}

spark-submit src/main/EventTransform.py "/Users/vishnoiprem/OwnProject/OwnPoc/Learning/17-etl/etl-event-pyspark/src/main/resources/input/source_event_data.json" "/Users/vishnoiprem/tmp/etl"
