package Hdfs;


import org.apache.hadoop.conf.Configuration;


public class SqoopTest {
    SqoopTool sqoopTool = SqoopTool.getTool("import");
    SqoopOptions sqoopOptions = new SqoopOptions();
				sqoopOptions.setConnectString("xxx");
 				sqoopOptions.setUsername("xxx");
                sqoopOptions.setPassword("xxx");
                sqoopOptions.setNumMappers(1);
                sqoopOptions.setNullStringValue("\\\\N");
                sqoopOptions.setNullNonStringValue("\\\\N");
                sqoopOptions.setFieldsTerminatedBy('\001');
                sqoopOptions.setTargetDir("/data/hive/warehouse/ods_cmis.db/ods_" + hiveTableName.toLowerCase());
                sqoopOptions.setCodeOutputDir("sqoopjavafile");
                sqoopOptions.setJarOutputDir("sqoopcompilefile/" + CommonUtil.getUUID() + "/");
                sqoopOptions.setHiveDropDelims(true);
                sqoopOptions.setSqlQuery(querySql);
                sqoopOptions.setAppendMode(true);
                sqoopOptions.setClassName(hiveTableName + CommonUtil.getUUID());
               sqoopOptions.setSqlQuery(querySql);
                sqoopOptions.setAppendMode(true);
                sqoopOptions.setClassName(hiveTableName + CommonUtil.getUUID());
    Configuration conf= new Configuration();
        conf.set("fs.defaultFS","hdfs://xxx:8020");
    Sqoop sqoop = new Sqoop(sqoopTool, SqoopTool.loadPlugins(conf), sqoopOptions);
                Sqoop.runSqoop(sqoop, new String[]{});
}
