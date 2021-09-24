package lvjaTest;

import java.text.MessageFormat;

/**
 * @Describe:
 * @Author：lvja
 * @Date：2021/3/10 15:07
 * @Modifier：
 * @ModefiedDate:
 */
public class test {
    public static void main(String[] args) {
        String express= MessageFormat.format("(TMDB=CN AND 申请人区域代码=330200 AND 申请日期 = {0})","fuck").toString();
        System.out.println(express);
    }
}
