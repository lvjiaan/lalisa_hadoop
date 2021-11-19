package lvjaTest;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * @Describe:
 * @Author：lvja
 * @Date：2021/3/10 15:07
 * @Modifier：
 * @ModefiedDate:
 */
public class test {
    public static void main(String[] args) {

        String json = "[{\"announcementdate\":\"20120829\",\"effectivedate\":\"\",\"beforechangeperson\":\"中原光电测控技术公司\",\"afterchangeperson\":\"河南中原光电测控技术有限公司\",\"beforechangeaddress\":\"河南省郑州市郑东新区博学路1000号\",\"afterchangeaddress\":\"郑州市郑东新区博学路1000号\"},{\"announcementdate\":\"20120104\",\"effectivedate\":\"20111125\",\"beforechangeperson\":\"中国电子科技集团公司第二十七研究所\",\"afterchangeperson\":\"中原光电测控技术公司\",\"beforechangeaddress\":\"河南省郑州市郑东新区博学路1000号\",\"afterchangeaddress\":\"河南省郑州市郑东新区博学路1000号\"},{\"announcementdate\":\"20120104\",\"effectivedate\":\"\",\"beforechangeperson\":\"信息产业部电子第27研究所\",\"afterchangeperson\":\"中国电子科技集团公司第二十七研究所\",\"beforechangeaddress\":\"河南省郑州市航海中路71号\",\"afterchangeaddress\":\"河南省郑州市郑东新区博学路1000号\"}]";
        String grantName = "信息产业部电子第27研究所";
        String grantDate = "20040128";
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
            if (!announcementDate.matches(reg) || !effectiveDate.matches(reg)) {
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
                        }else{
                            //todo 存在effectiveDate，但上一个没有 且本次effectiveDate<上次announcementDate
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
                System.out.println(relist.toString());
            } catch (Exception ignored) {
            }
        }


    }
}
