package API;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;


/**
 * HBase DDL和DML相关API操作
 */
public class HBaseApi {

    Configuration conf=null;

    @Before
    public void before(){
        conf= HBaseConfiguration.create();
        //通过连接zookeeper，获取hbase的连接地址
        //可以多写几个zookeeper节点，一个节点可能会有宕机的可能，导致连接不上
        conf.set("hbase.zookeeper.quorum","192.168.0.216:2181,192.168.0.217:2181,192.168.0.218:2181");
        conf.set("zookeeper.znode.parent", "/HBase");
    }

    //1 创建表
    @Test
    public void createTable() throws IOException {
        //连接HBase
        //Configuration conf=HBaseConfiguration.create();
        //通过连接zookeeper获取HBase的连接地址
        //conf.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");//担心一个节点挂掉不保险，可以全写上
        //连接HBase
        HBaseAdmin admin=new HBaseAdmin(conf);
        //创建表
        //准备HTableDescriptor
        HTableDescriptor table=new HTableDescriptor(TableName.valueOf("employer"));//指定表名
        //准备列族
        HColumnDescriptor basic=new HColumnDescriptor("basic");
        HColumnDescriptor other=new HColumnDescriptor("info");
        //表添加列族
        table.addFamily(basic);table.addFamily(other);
        //创建表
        admin.createTable(table);
        //关闭管理权
        admin.close();
    }


    //4 查询数据
    //a 字段来查
    @Test
    public void getData() throws IOException {
        HTable table=new HTable(conf,TableName.valueOf("lt_patent_main_tomodel_tohbase"));
        //获取数据
        Get get=new Get("CN201180069636.6".getBytes());
        Result result = table.get(get);
        //指定列族family和列名qualifier
        byte[] name = result.getValue("basic".getBytes(), "name".getBytes());
        byte[] age = result.getValue("basic".getBytes(), "age".getBytes());
        //打印
        System.err.println(new String(name)+":"+new String(age));
        //关流
        table.close();
    }

    //b 全表来查
    @Test
    public void scanData() throws IOException {
        HTable table=new HTable(conf,TableName.valueOf("employer"));
        //获取扫描器
        //Scan scan=new Scan();//全表扫描
        Scan scan=new Scan("e8848".getBytes(),"e8849".getBytes());//指定起始和结束rowKey，部分扫描

        ResultScanner scanner = table.getScanner(scan);
        //将扫描器转换为迭代器后才能遍历
        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result result = iterator.next();
            //从result中拿值
            byte[] value = result.getValue("basic".getBytes(), "number".getBytes());
            System.err.println(new String(value));
        }
        table.close();
    }

    /**
     * 创建表是否存在
     * @return 存在返回true，不存在返回false
     * @throws IOException
     */
    @Test
    public void ExistTable() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        boolean exists =  admin.tableExists(TableName.valueOf("lt_patent_main_tomodel_tohbase"));
        System.out.println( exists);
    }


}
