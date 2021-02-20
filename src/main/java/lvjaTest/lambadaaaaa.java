package lvjaTest;

import java.util.ArrayList;
import java.util.List;


public class lambadaaaaa {
    public static void main(String[] args) {
        //

        Comparable<Integer> comparable=new Comparable<Integer>() {
            @Override
            public int compareTo(Integer o) {
                return 0;
            }
        };

        //Lambada���ʽ�ķ���

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

//        //�����ڲ������ʽ����һ���߳�
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println("4564��");
//            }
//        }).start();
//
//        //Lambada���ʽ���������ڲ��࿪��һ���߳�
//        new Thread(() -> System.out.println("-------------")).start();



    }


}
