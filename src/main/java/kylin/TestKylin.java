package kylin;

import org.apache.kylin.jdbc.Driver;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * @author lvja
 * @version 1.0
 * @description: TODO
 * @date 2021/8/13 0013 15:12
 */
public class TestKylin {
    public static void main(String[] args) throws Exception {
        Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
        Properties info = new Properties();
        info.put("user", "ADMIN");
        info.put("password", "KYLIN");
        Connection conn = driver.connect("jdbc:kylin://192.168.4.181:7070/TEST", info);
        PreparedStatement ps = conn.prepareStatement("SELECT COUNT(DISTINCT APPLY_NUM) FROM FT_OWNER T1;");
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            System.out.println(resultSet.getInt(1));
        }

    }
}
