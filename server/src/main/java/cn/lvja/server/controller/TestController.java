package cn.lvja.server.controller;

/**
 * @Describe:
 * @Author：lvja
 * @Date：2021/2/5 11:27
 * @Modifier：
 * @ModefiedDate:
 */

import cn.lvja.server.service.TestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
public class TestController {

    @Autowired
    private TestService testService;

    @RequestMapping("/get")
    public String get(String name) {
        return testService.get(name);
    }
}
