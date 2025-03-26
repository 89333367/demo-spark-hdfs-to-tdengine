package sunyu.demo.test;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import sunyu.demo.Main;
import sunyu.util.TDengineUtil;

import java.util.List;
import java.util.Map;

public class Tests {
    Log log = LogFactory.get();

    @Test
    void t() {
        Main.main(null);
    }

    @Test
    void testDS() {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:spring.xml");
        String sql = "select * from db_test.stb_test limit 5";
        TDengineUtil tdengineUtil = TDengineUtil.builder().dataSource(applicationContext.getBean(HikariDataSource.class)).build();
        List<Map<String, Object>> rows = tdengineUtil.executeQuery(sql);
        log.info("查询结果: {}", rows);
        tdengineUtil.close();
    }

}
