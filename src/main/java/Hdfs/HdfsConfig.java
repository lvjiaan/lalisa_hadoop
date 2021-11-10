//package Hdfs;
//
///**
// * @program: bigdata
// * @description:
// * @author: cz
// * @create: 2020-03-09 16:11
// **/
//
///**
// * HDFS相关配置
// *
// * @author adminstrator
// * @since 1.0.0
// */
//@Configuration
//public class HdfsConfig {
////    private String defaultHdfsUri = "hdfs://192.168.4.181:8020";
//    private String defaultHdfsUri = "hdfs://192.168.0.216:8020";
//
//
//    @Bean
//    public HdfsService getHbaseService(){
//        System.setProperty("HADOOP_USER_NAME","hdfs");
//        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
//        conf.set("fs.defaultFS",defaultHdfsUri);
//        return new HdfsService(conf,defaultHdfsUri);
//    }
//}
//
