package MapReduce.udft;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Describe:
 * @Author：lvja
 * @Date：2021/3/3 14:52
 * @Modifier：
 * @ModefiedDate:
 */
@Description(name = "split_rank", value = "传入字符串和分隔符，返回序号+分割后的值；\n 例: split_rank('陈奕迅;杨千嬅') return: \n 1 陈奕迅\n 2 杨千嬅")
public class split_rank extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentLengthException("请传两个参数 1、处理的字符串 2、分隔符");
        }
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("col1");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        if (args[0]==null||args[1]==null)return;
        String input = args[0].toString();
        String regex=args[1].toString();
        String[] list = input.split(regex);
        for(int i=0; i<list.length; i++) {
            try {
                List<String> relist =new ArrayList<String>();
                relist.add(String.valueOf(i+1));
                relist.add(list[i]);
                forward(relist);
            } catch (Exception e) {
                continue;
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
