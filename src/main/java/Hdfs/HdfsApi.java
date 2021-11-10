package Hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * TODO
 *
 * @author lvja
 * @date 2021/11/2 0002 16:23
 */
public class HdfsApi {
    private static final Logger logger = LogManager.getLogger(HdfsApi.class);


    public static void main(String[] args) throws Exception {
        String path="/lvja/test.txt";
        createFile(path);

    }





    public static void createFile(String targetPath) throws IOException, InterruptedException, Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.4.181:8020"),conf,"hdfs");
        //目标路径
        Path dstPath = new Path(targetPath);
        //打开一个输出流
//        FSDataOutputStream outputStream = fs.create(dstPath);


//        String str="lalalisa";
//
//
//
//        outputStream.write(str.getBytes());
//        outputStream.flush();
//        outputStream.close();
//        fs.close();



        boolean result = fs.mkdirs(dstPath);
        System.out.println(result);
        logger.info("文件创建成功！");

    }





}
