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
@Description(name = "patentee_valid", value = "lalalisa love me")
public class patentee_valid extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 3) {
            throw new UDFArgumentLengthException("[lalalalisa1108] please put 3 params 1.json 2.grantName 3.grantDate");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("panentee");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("startdate");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("enddate");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        String json = args[0].toString();
        String grantName = args[1].toString();
        String grantDate = args[2].toString();
        JSONArray jsonArray = new JSONArray(json);

        List<Map<String, Object>> changeList = new ArrayList<>();
        for (int i = 0; i < jsonArray.length(); i++) {
            String jsonObjStr = jsonArray.get(i).toString();
            JSONObject jsonObject = new JSONObject(jsonObjStr);

            String announcementDate = String.valueOf(jsonObject.get("announcementdate"));
            String effectiveDate = String.valueOf(jsonObject.get("effectivedate"));
            String beforeChangePerson = (String) jsonObject.get("beforechangeperson");
            String afterChangePerson = (String) jsonObject.get("afterchangeperson");

            String reg = "\\d*";
            if (!announcementDate.matches(reg) || !effectiveDate.matches(reg) || !beforeChangePerson.matches(reg) || !afterChangePerson.matches(reg)) {
                continue;
            }

            Map<String, Object> map = new HashMap<>();
            if (announcementDate != null) {
                if ("".equals(announcementDate)) {
                    map.put("announcementDate", 0);
                } else {
                    map.put("announcementDate", Integer.parseInt(announcementDate));
                }
            } else {
                map.put("announcementDate", 0);
            }

            if (effectiveDate != null) {
                if ("".equals(effectiveDate)) {
                    map.put("effectiveDate", 0);
                } else {
                    map.put("effectiveDate", Integer.parseInt(effectiveDate));
                }
            } else {
                map.put("effectiveDate", 0);
            }

            if (beforeChangePerson != null) {
                map.put("beforeChangePerson", beforeChangePerson);
            } else {
                map.put("beforeChangePerson", "");
            }

            if (afterChangePerson != null) {
                map.put("afterChangePerson", afterChangePerson);
            } else {
                map.put("afterChangePerson", "");
            }
            changeList.add(map);

        }


        int nextIndex = 0;
        int lastDate = Integer.parseInt(grantDate);
        String lastPerson = grantName;
        int nextDate = 20991231;

        List<String[]> outList = new ArrayList<>();

        while (changeList.size() > 0) {
            boolean isNameMatch = false;
            for (int i = 0; i < changeList.size(); i++) {
                Map<String, Object> map = changeList.get(i);
                String beforeName = (String) map.get("beforeChangePerson");
                if (beforeName.equals(lastPerson)) {
//                    isNameMatch=true;
                    int effectiveDate = (int) map.get("effectiveDate");
                    if (effectiveDate != 0) {
                        if (effectiveDate > lastDate && effectiveDate < nextDate) {
                            nextDate = effectiveDate;
                            nextIndex = i;
                            isNameMatch = true;
                        }
                    } else {
                        int announcementDate = (int) map.get("announcementDate");
                        if (announcementDate > lastDate && announcementDate < nextDate) {
                            nextDate = announcementDate;
                            nextIndex = i;
                            isNameMatch = true;
                        }
                    }
                }
            }

            if (!isNameMatch) {
                boolean isMinDateMatch = false;
                //找到>lastDate的最小的记录
                for (int i = 0; i < changeList.size(); i++) {
                    Map<String, Object> map = changeList.get(i);
                    int effectiveDate = (int) map.get("effectiveDate");
                    if (effectiveDate != 0) {

                        if (effectiveDate > lastDate && effectiveDate < nextDate) {
                            isMinDateMatch = true;
                            nextDate = effectiveDate;
                            nextIndex = i;
                        }
                    } else {
                        int announcementDate = (int) map.get("announcementDate");
                        if (announcementDate > lastDate && announcementDate < nextDate) {
                            isMinDateMatch = true;
                            nextDate = announcementDate;
                            nextIndex = i;
                        }
                    }

                }


                if (isMinDateMatch) {
                    String person = lastPerson;
                    String startDate = String.valueOf(lastDate);
                    String endDate = String.valueOf(nextDate);
                    String[] outArr = {person, startDate, endDate};
                    outList.add(outArr);

                    Map<String, Object> map = changeList.get(nextIndex);

                    String person2 = (String) map.get("beforeChangePerson");
                    String[] outArr2 = {person2, endDate, endDate};
                    outList.add(outArr2);

                    lastDate = nextDate;
                    nextDate = 20991231;
                    lastPerson = (String) map.get("afterChangePerson");
                    changeList.remove(nextIndex);
                } else {
                    int minDate = 20991231;
                    //找到>lastDate的最小的记录
                    for (int i = 0; i < changeList.size(); i++) {
                        Map<String, Object> map = changeList.get(i);
                        int effectiveDate = (int) map.get("effectiveDate");
                        if (effectiveDate != 0) {

                            if (effectiveDate < minDate) {
                                minDate = effectiveDate;
                                nextIndex = i;
                            }
                        } else {
                            int announcementDate = (int) map.get("announcementDate");
                            if (announcementDate < minDate) {
                                minDate = announcementDate;
                                nextIndex = i;
                            }
                        }

                    }

                    String person = lastPerson;
                    String startDate = String.valueOf(lastDate);
                    String endDate = String.valueOf(lastDate);
                    String[] outArr = {person, startDate, endDate};
                    outList.add(outArr);

                    Map<String, Object> map = changeList.get(nextIndex);

                    String person2 = (String) map.get("beforeChangePerson");
                    String[] outArr2 = {person2, endDate, endDate};
                    outList.add(outArr2);

                    lastDate = Integer.parseInt(endDate);
                    nextDate = 20991231;
                    lastPerson = (String) map.get("afterChangePerson");
                    changeList.remove(nextIndex);
                }

            } else {
                String person = lastPerson;
                String startDate = String.valueOf(lastDate);
                String endDate = String.valueOf(nextDate);
                String[] outArr = {person, startDate, endDate};
                outList.add(outArr);

                Map<String, Object> map = changeList.get(nextIndex);
                lastDate = nextDate;
                nextDate = 20991231;
                lastPerson = (String) map.get("afterChangePerson");
                changeList.remove(nextIndex);
            }

        }

        String person = lastPerson;
        String startDate = String.valueOf(lastDate);
        String endDate = "20991231";
        String[] outArr = {person, startDate, endDate};
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
