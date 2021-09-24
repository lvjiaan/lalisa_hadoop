package MapReduce.udft;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class concat_collect_sort extends UDAF {
    public static class MaxiNumberIntUDAFEvaluator implements UDAFEvaluator {
        private class Item {
            String text;
            int date;

            Item(String text, int date) {
                this.text = text;
                this.date = date;
            }

            public String getText() {
                return text;
            }

            public void setText(String text) {
                this.text = text;
            }

            public int getDate() {
                return date;
            }

            public void setDate(int date) {
                this.date = date;
            }
        }

        List<Item> sortList = new ArrayList<Item>();
        //最终结果
        private String result;

        //负责初始化计算函数并设置它的内部状态，result是存放最终结果的
        public void init() {
            result = null;
        }

        //每次对一个新值进行聚集计算都会调用iterate方法
        public boolean iterate(String text, int date) {
            Item item = new Item(text, date);
            sortList.add(item);
            return true;
        }

        //Hive需要部分聚集结果的时候会调用该方法
        //会返回一个封装了聚集计算当前状态的对象
        public String terminatePartial() {
            return result;
        }

        //合并两个部分聚集值会调用这个方法
        public boolean merge(String text, int date) {
            return iterate(text, date);
        }

        //Hive需要最终聚集结果时候会调用该方法
        public String terminate() {
            Collections.sort(sortList, new Comparator<Item>() {
                public int compare(Item a, Item b) {
                    int valA1 = 0;
                    int valA2 = 0;
                    try {
                        valA1 = a.getDate();
                        valA2 = b.getDate();

                    } catch (Exception e) {
                        System.out.println(e);
                    }
                    // 设置排序规则
                    int i = valA2 - valA1;

                    return i;
                }
            });
            String rtn="";
            for (int i = 0; i < sortList.size(); i++) {
                rtn+=sortList.get(i).getText()+",";
            }
            rtn=rtn.substring(0,rtn.length()-1);

            return rtn;
        }
    }


}
