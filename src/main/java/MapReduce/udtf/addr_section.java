package MapReduce.udtf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;


/**
 * @Describe:
 * @Author：lvja
 * @Date：2021/3/3 14:52
 * @Modifier：
 * @ModefiedDate:
 */
@Description(name = "addr_section", value = "rose")
public class addr_section extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 3) {
            throw new UDFArgumentLengthException("[rose] please put 3 params 1.json 2.originaladdr 3.applydate");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("address");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("startdate");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("enddate");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args){
        String json = args[0].toString();
        String originalAddr = args[1].toString();
        String applyDate = args[2].toString();
        //将json解析成数组
        JSONArray jsonArray = new JSONArray(json);
        List<Map<String, Object>> changeList = new ArrayList<>();
        //将json解析成List<Map<String, Object>>
        for (int i = 0; i < jsonArray.length(); i++) {
            String jsonObjStr = jsonArray.get(i).toString();
            JSONObject jsonObject = new JSONObject(jsonObjStr);

            String transferDateStr = String.valueOf(jsonObject.get("transferdate"));
            String reg = "\\d*";
            if (!transferDateStr.matches(reg)) {
                continue;
            }

            int transferDate=Integer.parseInt(transferDateStr);
            String beforeChangeAddr = (String) jsonObject.get("beforechangeaddr");
            String afterChangeAddr = (String) jsonObject.get("afterchangeaddr");

            Map<String, Object> map = new HashMap<>();
            map.put("transferDate",transferDate);
            map.put("beforeChangeAddr",beforeChangeAddr);
            map.put("afterChangeAddr",afterChangeAddr);
            changeList.add(map);
        }


        int nextIndex = -1;
        String lastAddr = originalAddr;
        int nextDate = 20991231;
        int lastDate = Integer.parseInt(applyDate);

        List<String[]> outList = new ArrayList<>();

        while (changeList.size() > 0) {
            boolean matchBiggerLastDateAndSmallerNextDate=false;

            //得到匹配到的最小的
            int tempDate=20991231;
            int tempIndex=-1;
            for (int i =0;i<changeList.size();i++){
                Map<String,Object> data=changeList.get(i);
                if (data.get("beforeChangeAddr").equals(lastAddr)&&nextDate>(int)data.get("transferDate")&&lastDate<(int)data.get("transferDate")){
                    matchBiggerLastDateAndSmallerNextDate=true;
                    if ((int)data.get("transferDate")<tempDate){
                        tempDate=(int)data.get("transferDate");
                        tempIndex=i;
                    }
                }
            }
            if (matchBiggerLastDateAndSmallerNextDate){
                nextIndex=tempIndex;
                nextDate=tempDate;
                String addr = lastAddr;
                String startDate = String.valueOf(lastDate);
                String endDate = String.valueOf(tempDate);
                String[] outArr = {addr, startDate, endDate};
                outList.add(outArr);

                Map<String, Object> map = changeList.get(nextIndex);
                lastDate = nextDate;
                nextDate = 20991231;
                lastAddr = (String) map.get("afterchangeaddr");
                changeList.remove(nextIndex);
            }
        }

        String addr = lastAddr;
        String startDate = String.valueOf(lastDate);
        String endDate = "20991231";
        String[] outArr = {addr, startDate, endDate};
        outList.add(outArr);


        for (String[] strings : outList) {
            try {
                List<String> relist = Arrays.asList(strings);
                forward(relist);
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public void close() throws HiveException {

    }
}
