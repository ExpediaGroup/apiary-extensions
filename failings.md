```
2026-08-13 14:43:12,023 INFO  [main] com.expediagroup.apiary.extensions.gluesync.cli.GlueSyncCli - Syncing table: hadoop_dm_cust_ops_call_bkg_detail in database: onprem_conversation
2026-08-13 14:43:12,192 INFO  [main] com.expediagroup.apiary.extensions.gluesync.listener.ApiaryGlueSync - hadoop_dm_cust_ops_call_bkg_detail table already exists in glue, updating....
Exception in thread "main" java.lang.OutOfMemoryError: GC overhead limit exceeded
	at com.expediagroup.apiary.extensions.gluesync.listener.service.HiveToGlueTransformer.extractColumns(HiveToGlueTransformer.java:174)
	at com.expediagroup.apiary.extensions.gluesync.listener.service.HiveToGlueTransformer.transformPartition(HiveToGlueTransformer.java:117)
	at com.expediagroup.apiary.extensions.gluesync.listener.service.GluePartitionService.batchUpdatePartitions(GluePartitionService.java:389)
	at com.expediagroup.apiary.extensions.gluesync.listener.service.GluePartitionService.synchronizePartitions(GluePartitionService.java:322)
	at com.expediagroup.apiary.extensions.gluesync.cli.GlueSyncCli.syncTable(GlueSyncCli.java:171)
	at com.expediagroup.apiary.extensions.gluesync.cli.GlueSyncCli.syncAll(GlueSyncCli.java:127)
	at com.expediagroup.apiary.extensions.gluesync.cli.GlueSyncCliParser.main(GlueSyncCliParser.java:80)
```

aws glue get-partitions --database-name onprem_conversation --table-name hadoop_dm_cust_ops_call_bkg_detail --region us-east-1 --max-results 1 | jq                                 
