package lvjaTest;

import java.util.ArrayList;
import java.util.List;

/**
 * @Describe:
 * @Author：lvja
 * @Date：2020/12/21 14:45
 * @Modifier：
 * @ModefiedDate:
 */
public class lambadaaaaa {
    public static void main(String[] args) {
        //常见的函数式接口：Runnable、 Comparable--排序(是一个函数式接口吗？)

        Comparable<Integer> comparable=new Comparable<Integer>() {
            @Override
            public int compareTo(Integer o) {
                return 0;
            }
        };

        //Lambada表达式的方法

        Comparable<Integer> com=(a)->a;
        int i = com.compareTo(3);

        System.out.println(i);





//        List<String> strings = new ArrayList<String>();
//        strings.add("lvja");
//        strings.add(" is");
//        strings.add(" your father");
//
//        strings.forEach(item ->
//            System.out.print(item)
//        );

//        //匿名内部类的形式开启一个线程
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println("4564！");
//            }
//        }).start();
//
//        //Lambada表达式创建匿名内部类开启一个线程
//        new Thread(() -> System.out.println("-------------")).start();



    }


}
