Spark job that export/import efficiently a hbase table. You can choose to compress the output of the export to bzip2.

The project has been developped because the native hbase mapreduce export/import job can be quiet slow on very big hbase table. Especially if the table has enable compression wit ha high % compression rate
