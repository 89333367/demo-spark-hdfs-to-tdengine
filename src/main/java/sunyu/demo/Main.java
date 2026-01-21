package sunyu.demo;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.setting.dialect.Props;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import sunyu.util.TDengineUtil;
import uml.tech.bigdata.sdkconfig.ProtocolSdk;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class Main {
    static Log log = LogFactory.get();
    static Props props = new Props("application.properties");
    static ProtocolSdk sdk = new ProtocolSdk(props.getStr("config.url"));
    static ApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:spring.xml");
    static TDengineUtil tdUtil = TDengineUtil.builder()
            .dataSource(applicationContext.getBean(HikariDataSource.class))
            .setMaxConcurrency(10)
            .build();

    public static void main(String[] args) {
        String hdfsPath;
        SparkConf sparkConf = new SparkConf();
        if (args == null) {
            //端口使用 HDFS的 NameNode 端口

            //开发
            hdfsPath = "hdfs://cdh1:8020/spark/farm_can/2026/01/21/part-000111768926600000";
            //hdfsPath = "hdfs://cdh2:8020/spark/farm_can/2025/11/22/part-000041763803800000";

            //生产
            //hdfsPath = "hdfs://master012:9020/spark/farm_can/2024/08/26/part-000151724731200000";
            //hdfsPath = "hdfs://master020:9020/spark/farm_can/2025/11/30/part-000001764520200000";

            sparkConf.setAppName("local test");
            //sparkConf.setMaster("local[1]");
            sparkConf.setMaster("local[*]");
        } else {
            hdfsPath = args[0];
        }

        log.info("args参数 {}", JSONUtil.toJsonStr(args));

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext
                .textFile(hdfsPath)
                .foreachPartition((VoidFunction<Iterator<String>>) stringIterator -> {
                    stringIterator.forEachRemaining(s -> {
                        log.debug("读取到内部协议 {}", s);
                        TreeMap<String, String> m = sdk.parseProtocolString(s);
                        if (MapUtil.isNotEmpty(m) && m.get("3014") != null && m.get("did") != null
                                && m.get("2601") != null && m.get("2602") != null) {
                            //String did = m.get("did");
                            String did = "test";
                            String gpsTime = DateUtil.parse(m.get("3014")).toString("yyyy-MM-dd HH:mm:ss");
                            log.debug("解析后数据 {} {} {}", gpsTime, did, m);

                            Map<String, Object> row = new HashMap<>();
                            row.put("3014", DateUtil.parse(m.get("3014")).toString("yyyy-MM-dd HH:mm:ss"));
                            row.put("did", m.get("did"));
                            row.put("2601", m.get("2601"));
                            row.put("2602", m.get("2602"));
                            //异步写入
                            tdUtil.asyncInsertRow("frequent", "d_p", did, row);
                        }
                    });
                    //等待分区全部写入完毕
                    tdUtil.awaitAllTasks();
                    log.debug("{}", tdUtil.executeQuery("show databases"));
                });

        javaSparkContext.close();
        tdUtil.close();
    }

}