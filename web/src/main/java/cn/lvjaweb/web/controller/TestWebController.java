package cn.lvjaweb.web.controller;

/**
 * @Describe:
 * @Author：lvja
 * @Date：2021/2/5 12:05
 * @Modifier：
 * @ModefiedDate:
 */

import cn.lvjaserver.server.api.TestServiceApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestWebController {

    @Autowired
    private TestServiceApi testServicesApi;

    @RequestMapping("/getTest")
    public String getTest() {
        return testServicesApi.get("通过web调用的");
    }
}
