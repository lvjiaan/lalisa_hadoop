package cn.lvja.server.service.impl;

/**
 * @Describe:
 * @Author：lvja
 * @Date：2021/2/5 11:26
 * @Modifier：
 * @ModefiedDate:
 */

import cn.lvja.server.service.TestService;
import org.springframework.stereotype.Service;

@Service
class TestServicesImpl implements TestService {

    @Override
    public String get(String name) {
        return "参数name：" + name;
    }
}
