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
        HikariDataSource datasource = applicationContext.getBean(HikariDataSource.class);
        log.info("datasource: {}", datasource.getJdbcUrl());
        TDengineUtil tdUtil = TDengineUtil.builder().dataSource(datasource).setMaxConcurrency(10).build();
        String sql = "show databases";
        List<Map<String, Object>> rows = tdUtil.executeQuery(sql);
        log.info("查询结果: {}", rows);
        tdUtil.close();
    }

}
