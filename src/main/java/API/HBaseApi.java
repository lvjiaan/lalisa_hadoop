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
 * HBase DDL��DML���API����
 */
public class HBaseApi {

    Configuration conf=null;

    @Before
    public void before(){
        conf= HBaseConfiguration.create();
        //ͨ������zookeeper����ȡhbase�����ӵ�ַ
        //���Զ�д����zookeeper�ڵ㣬һ���ڵ���ܻ���崻��Ŀ��ܣ��������Ӳ���
        conf.set("hbase.zookeeper.quorum","192.168.0.216:2181,192.168.0.217:2181,192.168.0.218:2181");
        conf.set("zookeeper.znode.parent", "/HBase");
    }

    //1 ������
    @Test
    public void createTable() throws IOException {
        //����HBase
        //Configuration conf=HBaseConfiguration.create();
        //ͨ������zookeeper��ȡHBase�����ӵ�ַ
        //conf.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");//����һ���ڵ�ҵ������գ�����ȫд��
        //����HBase
        HBaseAdmin admin=new HBaseAdmin(conf);
        //������
        //׼��HTableDescriptor
        HTableDescriptor table=new HTableDescriptor(TableName.valueOf("employer"));//ָ������
        //׼������
        HColumnDescriptor basic=new HColumnDescriptor("basic");
        HColumnDescriptor other=new HColumnDescriptor("info");
        //���������
        table.addFamily(basic);table.addFamily(other);
        //������
        admin.createTable(table);
        //�رչ���Ȩ
        admin.close();
    }


    //4 ��ѯ����
    //a �ֶ�����
    @Test
    public void getData() throws IOException {
        HTable table=new HTable(conf,TableName.valueOf("lt_patent_main_tomodel_tohbase"));
        //��ȡ����
        Get get=new Get("CN201180069636.6".getBytes());
        Result result = table.get(get);
        //ָ������family������qualifier
        byte[] name = result.getValue("basic".getBytes(), "name".getBytes());
        byte[] age = result.getValue("basic".getBytes(), "age".getBytes());
        //��ӡ
        System.err.println(new String(name)+":"+new String(age));
        //����
        table.close();
    }

    //b ȫ������
    @Test
    public void scanData() throws IOException {
        HTable table=new HTable(conf,TableName.valueOf("employer"));
        //��ȡɨ����
        //Scan scan=new Scan();//ȫ��ɨ��
        Scan scan=new Scan("e8848".getBytes(),"e8849".getBytes());//ָ����ʼ�ͽ���rowKey������ɨ��

        ResultScanner scanner = table.getScanner(scan);
        //��ɨ����ת��Ϊ����������ܱ���
        Iterator<Result> iterator = scanner.iterator();
        while(iterator.hasNext()){
            Result result = iterator.next();
            //��result����ֵ
            byte[] value = result.getValue("basic".getBytes(), "number".getBytes());
            System.err.println(new String(value));
        }
        table.close();
    }

    /**
     * �������Ƿ����
     * @return ���ڷ���true�������ڷ���false
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
