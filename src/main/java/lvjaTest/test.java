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

        String reg="\\d*";
        String str="2012.4612";
        if (str.matches(reg)){
            System.out.println("true");
        }


    }
}
