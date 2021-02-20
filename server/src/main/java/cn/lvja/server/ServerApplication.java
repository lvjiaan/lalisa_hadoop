package cn.lvja.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;


//@SpringBootApplication(exclude = DruidDataSourceAutoConfigure.class)
@SpringBootApplication()
//@MapperScan(value = {"com.lvja.server.dao"})
@EnableCaching
public class ServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServerApplication.class, args);
    }

}
