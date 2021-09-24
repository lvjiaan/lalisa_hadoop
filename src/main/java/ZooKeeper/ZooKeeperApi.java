package ZooKeeper;

/**
 * @Describe:
 * @Author��lvja
 * @Date��2021/2/3 10:19
 * @Modifier��
 * @ModefiedDate:
 */
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * ����˵����ZooKeeper Api ������
 * ������Ա��@author liusha
 * �������ڣ�2020/1/14 9:58
 * �����������Ự�������ڵ㴴����ɾ�������ݶ�ȡ�����£�Ȩ�޿��Ƶ�
 */
public class ZooKeeperApi implements Watcher {
    private static Stat stat = new Stat();
    private static ZooKeeper zooKeeper = null;
    private static final String host = "192.168.0.216:2181";
    private static CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final String hosts = "192.168.0.216:2181,192.168.0.217:2181,192.168.0.218:2181";

    /**
     * ZooKeeper CreateMode�ڵ�����˵����
     * 1.PERSISTENT���־���
     * 2.PERSISTENT_SEQUENTIAL���־�˳����
     * 3.EPHEMERAL����ʱ��
     * 4.EPHEMERAL_SEQUENTIAL����ʱ˳����
     * <p>
     * 1��2�����Ϳͻ��˶Ͽ��󲻻���ʧ
     * 3��4�����Ϳͻ��˶Ͽ���ʱʱ����û���µ����ӽڵ㽫����ʧ
     */

    /**
     * ZooKeeper ZooDefs.IdsȨ������˵����
     * OPEN_ACL_UNSAFE����ȫ���ŵ�ACL���κ����ӵĿͻ��˶����Բ���������znode
     * CREATOR_ALL_ACL��ֻ�д����߲���ACLȨ��
     * READ_ACL_UNSAFE��ֻ�ܶ�ȡACL
     */

    /**
     * ZooKeeper EventType�¼�����˵����
     * NodeCreated���ڵ㴴��
     * NodeDataChanged���ڵ�����ݱ��
     * NodeChildrenChanged���ӽڵ�����ݱ��
     * NodeDeleted���ӽڵ�ɾ��
     */

    /**
     * ZooKeeper KeeperState״̬����˵����
     * Disconnected������ʧ��
     * SyncConnected�����ӳɹ�
     * AuthFailed����֤ʧ��
     * Expired���Ự����
     * None����ʼ״̬
     */

    /**
     * �����¼�֪ͨ
     *
     * @param event �¼�֪ͨ
     */
    @Override
    public void process(WatchedEvent event) {
        System.out.println("Receive WatchedEvent��" + event);
        try {
            if (Event.KeeperState.SyncConnected == event.getState()) {
                System.out.println("֪ͨ���Ự���ӳɹ�");
                if (Event.EventType.None == event.getType() && null == event.getPath()) {
                    System.out.println("����Ự��ʼ״̬");
                    // �ͷ����еȴ����߳�
                    countDownLatch.countDown();
                } else if (event.getType() == Event.EventType.NodeCreated) {
                    System.out.println("�ڵ㴴��֪ͨ��" + event.getPath());
                    zooKeeper.exists(event.getPath(), true);
                } else if (event.getType() == Event.EventType.NodeDataChanged) {
                    System.out.println("�ڵ�����ݱ��֪ͨ��" + new String(zooKeeper.getData(event.getPath(), true, stat)));
                    System.out.println("czxid=" + stat.getCzxid() + "��mzxid=" + stat.getMzxid() + "��version=" + stat.getVersion());
                    zooKeeper.exists(event.getPath(), true);
                } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    System.out.println("�ӽڵ�����ݱ��֪ͨ��" + zooKeeper.getChildren(event.getPath(), true));
                    zooKeeper.exists(event.getPath(), true);
                } else if (event.getType() == Event.EventType.NodeDeleted) {
                    System.out.println("�ڵ�ɾ��֪ͨ��" + event.getPath());
                    zooKeeper.exists(event.getPath(), true);
                } else {
                    System.out.println("δ֪�¼�֪ͨ���ͣ�" + event.getType());
                    zooKeeper.exists(event.getPath(), true);
                }
            } else if (Event.KeeperState.Disconnected == event.getState()) {
                System.out.println("֪ͨ���Ự����ʧ��");
            } else if (Event.KeeperState.AuthFailed == event.getState()) {
                System.out.println("֪ͨ���Ự��֤ʧ��");
            } else if (Event.KeeperState.Expired == event.getState()) {
                System.out.println("֪ͨ���Ự����");
            } else {
                System.out.println("δ֪��֪ͨ״̬��" + event.getState());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * �����Ự���������ʵ����
     *
     * @throws Exception Exception
     */
    @Test
    public void constructor_usage_simple() throws Exception {
        zooKeeper = new ZooKeeper(hosts, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper.state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();
        System.out.println("ZooKeeper session�Ự������ɡ�");
    }

    /**
     * �����Ự���ɸ���sessionId��ʵ����
     *
     * @throws Exception Exception
     */
    @Test
    public void constructor_usage_SID_PWD() throws Exception {
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper.state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();
        long sessionId = zooKeeper.getSessionId();
        byte[] sessionPasswd = zooKeeper.getSessionPasswd();
        System.out.println(String.format("�״λ�ȡsessionId��%s��sessionPasswd��%s", sessionId, sessionPasswd));
        // ʹ�ò���ȷ��sessionId
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi(), 1L, "123".getBytes());
        System.out.println("ZooKeeper.state err session��" + zooKeeper.getState());
        // ʹ����ȷ��sessionId
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi(), sessionId, sessionPasswd);
        System.out.println("ZooKeeper.state session��" + zooKeeper.getState());
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * �����ڵ㣨ͬ����
     *
     * @throws Exception Exception
     */
    @Test
    public void create_API_sync() throws Exception {
        String path = "/zk-create-znode-test-";
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper.state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();

        String path1 = zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("�ڵ㴴���ɹ���" + path1);
        String path2 = zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("�ڵ㴴���ɹ���" + path2);
    }

    /**
     * �����ڵ㣨�첽��
     * ͬ���ӿڴ����ڵ�ʱ��Ҫ���ǽӿ��׳��쳣�������
     * �첽�ӿڵ��쳣�����ڻص�������ResultCode��Ӧ���У���ͬ���ӿڸ���׳��
     *
     * @throws Exception Exception
     */
    @Test
    public void create_API_async() throws Exception {
        String path = "/zk-create-znode-test-";
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper.state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();

        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                new CreateCallBack(), "ZooKeeper async create znode.");
        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                new CreateCallBack(), "ZooKeeper async create znode.");
        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,
                new CreateCallBack(), "ZooKeeper async create znode.");
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * �����ڵ��첽�ص�
     */
    class CreateCallBack implements AsyncCallback.StringCallback {
        /**
         * @param rc   �������Ӧ�� 0���ӿڵ��óɹ���-4���ͻ��������������ѶϿ���-110��ָ���ڵ��Ѵ��ڣ�-112���Ự�ѹ���
         * @param path ���ýӿ�ʱ����Ľڵ�·����ԭ�������
         * @param ctx  ���ýӿ�ʱ�����ctxֵ��ԭ�������
         * @param name ʵ���ڷ���˴����Ľڵ���
         */
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            System.out.println("���������rc=" + rc + "��path=" + path + "��ctx=" + ctx + "��name=" + name);
            switch (rc) {
                case 0:
                    System.out.println("�ڵ㴴���ɹ���" + name);
                    break;
                case -4:
                    System.out.println("�ͻ��������������ѶϿ�");
                    break;
                case -110:
                    System.out.println("ָ���ڵ��Ѵ���");
                    break;
                case -112:
                    System.out.println("�Ự�ѹ���");
                    break;
                default:
                    System.out.println("�������Ӧ��" + rc + "δ֪");
                    break;
            }
        }
    }

    /**
     * ɾ���ڵ㣨ͬ����
     * ע��ֻ����ɾ��Ҷ�ӽڵ㣬����ֱ��ɾ�����ڵ�
     *
     * @throws Exception Exception
     */
    @Test
    public void delete_API_sync() throws Exception {
        String path = "/zk-delete-znode-test";
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();
        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zooKeeper.delete(path, -1);
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * ɾ���ڵ㣨�첽��
     * ע��ֻ����ɾ��Ҷ�ӽڵ㣬����ֱ��ɾ�����ڵ�
     *
     * @throws Exception Exception
     */
    @Test
    public void delete_API_async() throws Exception {
        String path = "/zk-delete-znode-test";
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();
        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zooKeeper.delete(path, -1, new DeleteCallBack(), "ZooKeeper async delete znode");
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * ɾ���ڵ��첽�ص�
     */
    class DeleteCallBack implements AsyncCallback.VoidCallback {
        /**
         * @param rc   �������Ӧ�� 0���ӿڵ��óɹ���-4���ͻ��������������ѶϿ���-110��ָ���ڵ��Ѵ��ڣ�-112���Ự�ѹ���
         * @param path ���ýӿ�ʱ����Ľڵ�·����ԭ�������
         * @param ctx  ���ýӿ�ʱ�����ctxֵ��ԭ�������
         */
        @Override
        public void processResult(int rc, String path, Object ctx) {
            System.out.println("ɾ�������rc=" + rc + "��path=" + path + "��ctx=" + ctx);
            switch (rc) {
                case 0:
                    System.out.println("�ڵ�ɾ���ɹ�");
                    break;
                case -4:
                    System.out.println("�ͻ��������������ѶϿ�");
                    break;
                case -112:
                    System.out.println("�Ự�ѹ���");
                    break;
                default:
                    System.out.println("�������Ӧ��" + rc + "δ֪");
                    break;
            }
        }
    }

    /**
     * ��ȡ�ӽڵ㣨ͬ����
     *
     * @throws Exception Exception
     */
    @Test
    public void getChildren_API_sync() throws Exception {
        String path = "/solr/configs/lt_patent_main_tomodel3";
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();
        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create(path + "/children1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        List<String> childrenList = zooKeeper.getChildren(path, true);
        System.out.println("��ȡ�ӽڵ㣺" + childrenList);
        zooKeeper.create(path + "/children2", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * ��ȡ�ӽڵ㣨�첽��
     *
     * @throws Exception Exception
     */
    @Test
    public void getChildren_API_async() throws Exception {
        String path = "/zk-getChildren-async-test";
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();
        zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zooKeeper.create(path + "/children1", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zooKeeper.getChildren(path, true, new ChildrenCallBack(), "�첽��ȡ�ӽڵ�");
        zooKeeper.create(path + "/children2", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * ��ȡ�ӽڵ��첽�ص�
     */
    class ChildrenCallBack implements AsyncCallback.Children2Callback {
        /**
         * @param rc           �������Ӧ�� 0���ӿڵ��óɹ���-4���ͻ��������������ѶϿ���-110��ָ���ڵ��Ѵ��ڣ�-112���Ự�ѹ���
         * @param path         ���ýӿ�ʱ����Ľڵ�·����ԭ�������
         * @param ctx          ���ýӿ�ʱ�����ctxֵ��ԭ�������
         * @param childrenList �ӽڵ��б�
         * @param stat         �ڵ�״̬���ɷ���������Ӧ����stat�滻
         */
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> childrenList, Stat stat) {
            System.out.println("��ȡ�����rc=" + rc + "��path=" + path + "��ctx=" + ctx + "��childrenList=" + childrenList + "��stat=" + stat);
            switch (rc) {
                case 0:
                    System.out.println("�ӽڵ��ȡ�ɹ���" + childrenList);
                    break;
                case -4:
                    System.out.println("�ͻ��������������ѶϿ�");
                    break;
                case -112:
                    System.out.println("�Ự�ѹ���");
                    break;
                default:
                    System.out.println("�������Ӧ��" + rc + "δ֪");
                    break;
            }
        }
    }

    /**
     * ��ȡ�ڵ����ݣ�ͬ����
     * ���½ڵ����ݣ�ͬ����
     *
     * @throws Exception Exception
     */
    @Test
    public void getData_API_sync() throws Exception {
        String path = "/zk-getData-sync-test";
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();
        zooKeeper.create(path, "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("�ڵ����ݣ�" + new String(zooKeeper.getData(path, true, stat)));
        System.out.println("czxid=" + stat.getCzxid() + "��mzxid=" + stat.getMzxid() + "��version=" + stat.getVersion());
        zooKeeper.setData(path, "test".getBytes(), -1);
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * ��ȡ�ڵ����ݣ��첽��
     * ���½ڵ����ݣ�ͬ����
     *
     * @throws Exception Exception
     */
    @Test
    public void getData_API_async() throws Exception {
        String path = "/zk-getData-async-test";
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();
        zooKeeper.create(path, "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zooKeeper.getData(path, true, new DataCallBack(), "�첽��ȡ�ڵ�����");
        System.out.println("czxid=" + stat.getCzxid() + "��mzxid=" + stat.getMzxid() + "��version=" + stat.getVersion());
        Stat stat1 = zooKeeper.setData(path, "test".getBytes(), -1);
        System.out.println("czxid=" + stat1.getCzxid() + "��mzxid=" + stat1.getMzxid() + "��version=" + stat1.getVersion());
        Stat stat2 = zooKeeper.setData(path, "test123".getBytes(), stat1.getVersion());
        System.out.println("czxid=" + stat2.getCzxid() + "��mzxid=" + stat2.getMzxid() + "��version=" + stat2.getVersion());
        try {
            zooKeeper.setData(path, "test123456".getBytes(), stat1.getVersion());
        } catch (KeeperException e) {
            System.out.println("Error Code��" + e.code() + "��" + e.getMessage());
        }
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * ��ȡ�ڵ������첽�ص�
     */
    class DataCallBack implements AsyncCallback.DataCallback {
        /**
         * @param rc   �������Ӧ�� 0���ӿڵ��óɹ���-4���ͻ��������������ѶϿ���-110��ָ���ڵ��Ѵ��ڣ�-112���Ự�ѹ���
         * @param path ���ýӿ�ʱ����Ľڵ�·����ԭ�������
         * @param ctx  ���ýӿ�ʱ�����ctxֵ��ԭ�������
         * @param data �ڵ�����
         * @param stat �ڵ�״̬���ɷ���������Ӧ����stat�滻
         */
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            System.out.println("��ȡ�����rc=" + rc + "��path=" + path + "��ctx=" + ctx + "��data=" + new String(data) + "��stat=" + stat);
            System.out.println("czxid=" + stat.getCzxid() + "��mzxid=" + stat.getMzxid() + "��version=" + stat.getVersion());
            switch (rc) {
                case 0:
                    System.out.println("�ڵ����ݻ�ȡ�ɹ���" + new String(data));
                    break;
                case -4:
                    System.out.println("�ͻ��������������ѶϿ�");
                    break;
                case -112:
                    System.out.println("�Ự�ѹ���");
                    break;
                default:
                    System.out.println("�������Ӧ��" + rc + "δ֪");
                    break;
            }
        }
    }

    /**
     * ���½ڵ����ݣ��첽��
     *
     * @throws Exception Exception
     */
    @Test
    public void setData_API_async() throws Exception {
        String path = "/zk-setData-test";
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        System.out.println("ZooKeeper state��" + zooKeeper.getState());
        // �����߳�ִ�����
        countDownLatch.await();
        zooKeeper.exists(path, true);
        zooKeeper.create(path, "test".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zooKeeper.setData(path, "test123456".getBytes(), -1, new StatCallBack(), "�첽���½ڵ�����");
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * ���½ڵ������첽�ص�
     */
    class StatCallBack implements AsyncCallback.StatCallback {
        /**
         * @param rc   �������Ӧ�� 0���ӿڵ��óɹ���-4���ͻ��������������ѶϿ���-110��ָ���ڵ��Ѵ��ڣ�-112���Ự�ѹ���
         * @param path ���ýӿ�ʱ����Ľڵ�·����ԭ�������
         * @param ctx  ���ýӿ�ʱ�����ctxֵ��ԭ�������
         * @param stat �ڵ�״̬���ɷ���������Ӧ����stat�滻
         */
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            System.out.println("���½����rc=" + rc + "��path=" + path + "��ctx=" + ctx + "��stat=" + stat);
            System.out.println("czxid=" + stat.getCzxid() + "��mzxid=" + stat.getMzxid() + "��version=" + stat.getVersion());
            switch (rc) {
                case 0:
                    System.out.println("�ڵ��������óɹ�");
                    break;
                case -4:
                    System.out.println("�ͻ��������������ѶϿ�");
                    break;
                case -112:
                    System.out.println("�Ự�ѹ���");
                    break;
                default:
                    System.out.println("�������Ӧ��" + rc + "δ֪");
                    break;
            }
        }
    }

    /**
     * Ȩ�޿���
     *
     * @throws Exception Exception
     */
    @Test
    public void auth_control_API() throws Exception {
        String path = "/zk-setData-test";
        zooKeeper = new ZooKeeper(host, 5000, new ZooKeeperApi());
        zooKeeper.addAuthInfo("digest", "zoo:true".getBytes());
        zooKeeper.create(path, "init".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.EPHEMERAL);
        // 1����Ȩ����Ϣ����
//    ZooKeeper zooKeeper1 = new ZooKeeper(host, 5000, new ZooKeeperApi());
//    System.out.println("���ʽ����" + new String(zooKeeper1.getData(path, true, stat)));
        // 2������Ȩ����Ϣ����
//    ZooKeeper zooKeeper2 = new ZooKeeper(host, 5000, new ZooKeeperApi());
//    zooKeeper2.addAuthInfo("digest", "zoo:false".getBytes());
//    System.out.println("���ʽ����" + new String(zooKeeper2.getData(path, true, stat)));
        // 3����ȷȨ����Ϣ����
        ZooKeeper zooKeeper3 = new ZooKeeper(host, 5000, new ZooKeeperApi());
        zooKeeper3.addAuthInfo("digest", "zoo:true".getBytes());
        System.out.println("���ʽ����" + new String(zooKeeper3.getData(path, true, stat)));
        Thread.sleep(Integer.MAX_VALUE);
    }

}
