package cn.lvjaserver.server.api;

/**
 * @Describe:
 * @Author：lvja
 * @Date：2021/2/5 11:28
 * @Modifier：
 * @ModefiedDate:
 */

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;

@FeignClient(name = "server-service", contextId = "TestServiceApi")
public interface TestServiceApi {

    @RequestMapping("/test/get")
    public String get(String name);
}
