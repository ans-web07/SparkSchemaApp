1.	JSON doesn’t have a header.
2.	JSON reader will arrange the fields in lexicographical order.
3.	Parquet comes with the schema info already included in data file thus specification of schema is not required explicitly. However, your data file should contain the correct schema.
4.	Parquet format is default and recommended format for Apache Spark.
5.	Schema can be defined in 2 ways: Programmatically and DDL 
6.	Programmatically: StructType refers to dataframe row structure and StructField is column definition.
