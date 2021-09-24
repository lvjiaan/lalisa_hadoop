package MapReduce.udft;
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

import static java.lang.Math.min;

/**
 * @Describe:
 * @Author：lvja
 * @Date：2021/3/3 14:52
 * @Modifier：
 * @ModefiedDate:
 */
public class split_match extends GenericUDTF {


    @Override
    public StructObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 3) {
            throw new UDFArgumentLengthException("请传三个参数 1、处理的字符串1 2、处理的字符串2 3、分隔符");
        }
//        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
//            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
//        }

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
        String input1 = args[0].toString();
        String input2 = args[1].toString();
        String regex=args[2].toString();
        String[] list1 = input1.split(regex);
        String[] list2 = input2.split(regex);
        int size =min(list1.length,list2.length);
        for(int i=0; i<size; i++) {
            try {
                List<String> relist =new ArrayList<String>();
                relist.add(list1[i]);
                relist.add(list2[i]);
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
