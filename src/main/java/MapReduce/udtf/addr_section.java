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

        Map<String, Object> originalMap = new HashMap<>();
        originalMap.put("transferDate",Integer.parseInt(applyDate));
        originalMap.put("beforeChangeAddr","");
        originalMap.put("afterChangeAddr",originalAddr);
        changeList.add(originalMap);



        List<String[]> outList = new ArrayList<>();


        List<Map<String,Object>> rowList=new ArrayList<>();

        int size2099=0;

        for (Map<String,Object> data:changeList){
            String addr= (String) data.get("afterChangeAddr");
            int startDate= (int) data.get("transferDate");
            getIndexByLast(addr,startDate,changeList,rowList,size2099);
        }

        if (size2099>1){
            for (Map<String,Object> data:rowList){
                if ((int)data.get("endDate")==20991231){
                    int endDate=20991231;
                    for (Map<String,Object> data2:rowList){
                        if ((int)data2.get("startDate")>(int)data.get("startDate")&&(int)data2.get("startDate")<endDate){
                            endDate=(int)data2.get("startDate");
                        }
                    }
                    data.put("endDate",endDate);
                }
            }
        }

        for (Map<String,Object> data:rowList){
            String[] outArr = {(String) data.get("addr"), String.valueOf(data.get("startDate")), String.valueOf(data.get("endDate"))};
            outList.add(outArr);
        }

        for (String[] strings : outList) {
            try {
                List<String> relist = Arrays.asList(strings);
                forward(relist);
            } catch (Exception ignored) {
            }
        }
    }

    private void getIndexByLast(String addr, int startDate, List<Map<String, Object>> changeList, List<Map<String, Object>> rowList, int size2099) {
        int endDate=20991231;
        boolean matchBiggerLastDate=false;

        for (int i=0;i<changeList.size();i++){
            Map<String,Object> data=changeList.get(i);
                if (data.get("beforeChangeAddr").equals(addr)&&startDate<(int)data.get("transferDate")){
                    matchBiggerLastDate=true;
                    if ((int)data.get("transferDate")<endDate){
                        endDate=(int)data.get("transferDate");
                    }
                }
        }

        if (!matchBiggerLastDate){
            boolean matchAddr=false;
            for (int i=0;i<changeList.size();i++){
                Map<String,Object> data=changeList.get(i);
                if (data.get("beforeChangeAddr").equals(addr)){
                    matchAddr=true;
                    if ((int)data.get("transferDate")<endDate){
                        endDate=(int)data.get("transferDate");
                        startDate=endDate;
                    }
                }
            }
        }


        Map<String,Object> result=new HashMap<>();
        result.put("addr",addr);
        result.put("startDate",startDate);
        result.put("endDate",endDate);

        if (endDate==20991231){
            size2099+=1;
        }

        rowList.add(result);




    }

    @Override
    public void close() throws HiveException {

    }
}
