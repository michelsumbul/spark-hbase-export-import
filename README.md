Spark job that export/import efficiently a hbase table. You can choose to compress the output of the export to bzip2.

The project has been developped because the native hbase mapreduce export/import job can be quiet slow on very big hbase table. Especially if the table has enable compression wit ha high % compression rate

The code have been tested on HDP3.1 with Spark 2.3.2 and HBase 2.0.2

To run an export use the following command:
Synthax: 
sparkHBaseExportImport.sparkhbaseexportimport.exportHbase --num-executors <num_exec> --executor-memory <size_mem_exec> --master yarn --deploy-mode client sparkHBaseExportImport-1.0-jar-with-dependencies.jar <table_name> <hdfs_destination_folder> <boolean_compression true|false>

Example:

/usr/hdp/current/spark2-client/bin/spark-submit --class sparkHBaseExportImport.sparkhbaseexportimport.exportHbase --num-executors 6 --executor-memory 10G --master yarn --deploy-mode client sparkHBaseExportImport-1.0-jar-with-dependencies.jar test_table /tmp/test2/ true

You will have to choose the right number of executor and the memory per executor depending on the size of the table, the ressource available on the cluster and how quick you want to execute the export. 
