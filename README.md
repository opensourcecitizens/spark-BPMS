This is a kafka consumer implemented in spark streaming API. It handles message consumption, parsing avro messages, rule engine and end point interfacing. 
End points include HDFS, HBase Apache Phoenix, Restful services (registry).

Executing the main class in HDP:
sudo su spark
nohup spark-submit  --class "com.neustar.iot.spark.kafka.SparkKafkaConsumer" --master local  ds-kafkaprod-sparkconsumer-jar-with-dependencies.jar testexternaltopic 3 &


Observer progress in spark admin UI.