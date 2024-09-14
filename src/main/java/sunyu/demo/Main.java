package sunyu.demo;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.setting.dialect.Props;
import com.zaxxer.hikari.HikariDataSource;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import sunyu.demo.domain.Params3Enum;
import sunyu.util.RedisUtil;
import sunyu.util.TDengineUtil;
import uml.tech.bigdata.sdkconfig.ProtocolSdk;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Main {
    static Log log = LogFactory.get();
    static Props props = new Props("application.properties");
    static ProtocolSdk sdk = new ProtocolSdk(props.getStr("config.url"));
    static ApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:spring.xml");
    static TDengineUtil tdUtil = TDengineUtil.builder()
            .dataSource(applicationContext.getBean(HikariDataSource.class))
            .maxPoolSize(10).maxWorkQueue(10).build();
    static RedisUtil redisUtil = RedisUtil.builder().build();
    static StatefulRedisClusterConnection<String, String> cluster = redisUtil
            .cluster(Arrays.stream(props.getStr("spring.redis.cluster.nodes").split(","))
                    .map(s -> s.split(":"))
                    .map(arr -> StrUtil.format("redis://{}:{}", arr[0], arr[1]))
                    .collect(Collectors.toList()));
    static Map<String, List<Map<String, String>>> relationsMap = new HashMap<>();

    public static void main(String[] args) {
        // args
        //# day killData
        String hdfsPath;
        String day;
        boolean killData;
        String beginDataTime = "20160101000000";
        String endDateTime = "20260101000000";

        SparkConf sparkConf = new SparkConf();

        if (args == null) {
            //端口使用 HDFS的 NameNode 端口

            //开发
            //hdfsPath = "hdfs://cdh1:8020/spark/farm_can/2024/08/26/part-000151724731200000";
            //hdfsPath = "hdfs://cdh2:8020/spark/farm_can/2024/08/26/part-000151724731200000";

            //生产
            //hdfsPath = "hdfs://master012:9020/spark/farm_can/2024/08/26/part-000151724731200000";
            //hdfsPath = "hdfs://master020:9020/spark/farm_can/2024/08/26/part-000151724731200000";
            day = "20240826";
            killData = false;

            sparkConf.setAppName("local test");
            sparkConf.setMaster("local[*]");
        } else {
            //hdfsPath = StrUtil.format("/spark/farm_can/{}/{}/{}/*");
            day = args[0];
            killData = Convert.toBool(args[1], false);
        }

        log.info("args参数 {}", JSONUtil.toJsonStr(args));

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        DateTime dateTime = DateUtil.parse(day);
        log.info("准备处理日期 {}", dateTime);
        hdfsPath = StrUtil.format("/spark/farm_can/{}/{}/{}/*", dateTime.toString("yyyy"), dateTime.toString("MM"), dateTime.toString("dd"));
        if (args == null) {
            hdfsPath = "hdfs://cdh2:8020" + hdfsPath;
        }
        log.info("准备处理文件 {}", hdfsPath);

        if (killData) {
            log.info("删除数据开始");
            javaSparkContext.parallelize(Arrays.asList(0), 1).foreach((VoidFunction<Integer>) integer -> {
                //做数据库删除操作，此操作比较耗时
                log.info("删除v_c和d_p数据开始，如遇60秒超时无需理会，会删除成功");

                ThreadUtil.execute(() -> {
                    String sql = StrUtil.format("delete from frequent.v_c where _rowts>='{} 00:00:00' and _rowts<='{} 23:59:59'"
                            , dateTime.toString(DatePattern.NORM_DATE_FORMAT), dateTime.toString(DatePattern.NORM_DATE_FORMAT));
                    log.info(sql);
                    tdUtil.executeUpdate(sql, 0, null);
                });

                ThreadUtil.sleep(1000);

                while (true) {
                    String sql = "select count(*) c from performance_schema.perf_queries where sql like 'delete from frequent.v_c %'";
                    if (tdUtil.executeQuery(sql).size() == 0) {
                        break;
                    }
                    ThreadUtil.sleep(1000);
                }

                ThreadUtil.execute(() -> {
                    String sql = StrUtil.format("delete from frequent.d_p where _rowts>='{} 00:00:00' and _rowts<='{} 23:59:59'"
                            , dateTime.toString(DatePattern.NORM_DATE_FORMAT), dateTime.toString(DatePattern.NORM_DATE_FORMAT));
                    log.info(sql);
                    tdUtil.executeUpdate(sql, 0, null);
                });

                ThreadUtil.sleep(1000);

                while (true) {
                    String sql = "select count(*) c from performance_schema.perf_queries where sql like 'delete from frequent.d_p %'";
                    if (tdUtil.executeQuery(sql).size() == 0) {
                        break;
                    }
                    ThreadUtil.sleep(1000);
                }

                log.info("删除v_c和d_p数据结束");
            });
            log.info("删除数据结束");
        }

        javaSparkContext
                .textFile(hdfsPath)
                .foreachPartition((VoidFunction<Iterator<String>>) stringIterator -> {
                    AtomicLong dp = new AtomicLong();
                    AtomicLong vc = new AtomicLong();
                    stringIterator.forEachRemaining(protocolString -> {
                        if (StrUtil.isNotBlank(protocolString)) {
                            String newProtocolString = removeNonSpaceInvisibleChars(protocolString);
                            if (newProtocolString.length() < 4000) {
                                Map<String, String> m = sdk.parseProtocolString(newProtocolString);
                                if (MapUtil.isNotEmpty(m) && m.containsKey("did") && m.containsKey("3014") && m.containsKey("TIME")) {
                                    try {
                                        String did = m.get("did");
                                        DateTime gpsTime = DateUtil.parse(m.get("3014"), DatePattern.PURE_DATETIME_FORMAT);
                                        DateTime gatewayTime = DateUtil.parse(m.get("TIME"), DatePattern.PURE_DATETIME_FORMAT);
                                        if (beginDataTime.compareTo(gpsTime.toString(DatePattern.PURE_DATETIME_FORMAT)) <= 0
                                                && gpsTime.toString(DatePattern.PURE_DATETIME_FORMAT).compareTo(endDateTime) <= 0
                                                && beginDataTime.compareTo(gatewayTime.toString(DatePattern.PURE_DATETIME_FORMAT)) <= 0
                                                && gatewayTime.toString(DatePattern.PURE_DATETIME_FORMAT).compareTo(endDateTime) <= 0) {
                                            // todo insert frequent.d_p
                                            tdUtil.insertRow("frequent", "d_p", StrUtil.format("d_p_{}", did), new HashMap<String, Object>() {{
                                                put("3014", gpsTime.toString(DatePattern.NORM_DATETIME_FORMAT));//_rowts
                                                put("protocol", newProtocolString);
                                                put("did", did);//tag
                                            }});
                                            dp.getAndIncrement();

                                            // todo insert frequent.v_c
                                            String vId = getVid(did, gpsTime.toString(DatePattern.PURE_DATETIME_FORMAT));
                                            if (StrUtil.isNotBlank(vId)) {//找到了vid
                                                tdUtil.insertRow("frequent", "v_c", StrUtil.format("v_c_{}", vId), new HashMap<String, Object>() {{
                                                    //超插入的列，列名必须存在于上面列的定义中
                                                    put("3014", gpsTime.toString(DatePattern.NORM_DATETIME_FORMAT));
                                                    put("TIME", gatewayTime.toString(DatePattern.NORM_DATETIME_FORMAT));
                                                    put("did", did);
                                                    put("params3", Params3Enum.valueOf(m.getOrDefault("params3", "ERROR")).getCode());
                                                    put("2204", Convert.toFloat(m.get("2204"), null));
                                                    put("2205", Convert.toDouble(m.get("2205"), null));
                                                    put("2206", Convert.toInt(m.get("2206"), null));
                                                    Integer p2601 = Convert.toInt(m.get("2601"), -1);
                                                    if (p2601 == 0 || p2601 == 1) {
                                                        put("2601", p2601);
                                                    }
                                                    put("2602", Convert.toDouble(m.get("2602"), null));
                                                    put("2603", Convert.toDouble(m.get("2603"), null));
                                                    Integer p3020 = Convert.toInt(m.get("3020"), -1);
                                                    if (p3020 == 0 || p3020 == 1) {
                                                        put("3020", p3020);
                                                    }
                                                    put("3040", Convert.toFloat(m.get("3040"), null));
                                                    put("3328", Convert.toDouble(m.get("3328"), null));
                                                    put("4023", Convert.toDouble(m.get("4023"), null));
                                                    Integer p4031 = Convert.toInt(m.get("4031"), -1); // 4031 范围0-2, 其它数值无效
                                                    if (p4031 >= 0 && p4031 <= 2) {
                                                        put("4031", p4031);
                                                    }
                                                    put("4035", Convert.toDouble(m.get("4035"), null));
                                                    put("4101", Convert.toDouble(m.get("4101"), null));
                                                    put("4106", Convert.toInt(m.get("4106"), null));
                                                    put("4108", Convert.toDouble(m.get("4108"), null));
                                                    put("4118", Convert.toDouble(m.get("4118"), null));
                                                    put("4141", Convert.toInt(m.get("4141"), null));
                                                    put("4163", Convert.toInt(m.get("4163"), null));
                                                    put("4173", Convert.toInt(m.get("4173"), null));
                                                    put("4174", Convert.toDouble(m.get("4174"), null));
                                                    put("4175", Convert.toDouble(m.get("4175"), null));
                                                    put("4177", Convert.toInt(m.get("4177"), null));
                                                    put("4180", Convert.toDouble(m.get("4180"), null));
                                                    put("4181", Convert.toDouble(m.get("4181"), null));
                                                    put("4247", Convert.toInt(m.get("4247"), null));
                                                    put("4319", Convert.toInt(m.get("4319"), null));
                                                    put("4592", Convert.toDouble(m.get("4592"), null));
                                                    put("4609", Convert.toDouble(m.get("4609"), null));
                                                    put("4617", Convert.toDouble(m.get("4617"), null));
                                                    put("4618", Convert.toDouble(m.get("4618"), null));
                                                    put("4619", Convert.toInt(m.get("4619"), null));
                                                    put("4620", Convert.toInt(m.get("4620"), null));
                                                    put("4621", Convert.toInt(m.get("4621"), null));
                                                    put("4623", Convert.toInt(m.get("4623"), null));
                                                    put("4624", Convert.toInt(m.get("4624"), null));
                                                    put("4625", Convert.toInt(m.get("4625"), null));
                                                    put("4655", Convert.toInt(m.get("4655"), null));
                                                    Integer p4862 = Convert.toInt(m.get("4862"), -1);
                                                    if (p4862 == 0 || p4862 == 1) {
                                                        put("4862", p4862);
                                                    }
                                                    put("4865", Convert.toDouble(m.get("4865"), null));
                                                    put("4905", Convert.toDouble(m.get("4905"), null));
                                                    put("4906", Convert.toDouble(m.get("4906"), null));
                                                    put("4907", Convert.toDouble(m.get("4907"), null));
                                                    put("4955", Convert.toDouble(m.get("4955"), null));
                                                    put("5056", m.get("5056"));
                                                    put("5057", m.get("5057"));
                                                    Integer p5298 = Convert.toInt(m.get("5298"), -1);
                                                    if (p5298 >= 0 && p5298 <= 4) {
                                                        put("5298", p5298);
                                                    }
                                                    put("5362", Convert.toDouble(m.get("5362"), null));
                                                    put("protocol", newProtocolString);
                                                }});
                                                vc.getAndIncrement();
                                            }
                                        } else {
                                            log.error("{} 超出数据时间范围 3014:{} TIME:{} 此数据抛弃，不予存储", m.get("did"), m.get("3014"), m.get("TIME"));
                                        }
                                    } catch (Exception e) {
                                        log.error("{} 时间有问题 3014:{} TIME:{} 此数据抛弃", m.get("did"), m.get("3014"), m.get("TIME"));
                                    }
                                }
                            } else {
                                log.error("内部协议长度超过4000字节，数据抛弃 {}", newProtocolString);
                            }
                        }
                    });
                    tdUtil.awaitExecution();
                    log.info("当前分区对应关系缓存 {} 个 插入 {} 条dp 插入 {} 条vc", relationsMap.size(), dp.get(), vc.get());
                });

        javaSparkContext.close();
        tdUtil.close();
        redisUtil.close();
    }

    private static String removeNonSpaceInvisibleChars(String str) {
        if (str == null) return "";
        StringBuilder sb = new StringBuilder();
        for (char c : str.toCharArray()) {
            // 去掉不可见字符，但是保留空格
            if (c == ' ' || (!Character.isWhitespace(c) && !Character.isISOControl(c))) {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static String getVid(String did, String yyyyMMddHHmmss) {
        List<Map<String, String>> relations = null;
        if (relationsMap.containsKey(did)) {
            relations = relationsMap.get(did);
        } else {//在redis中查询对应关系
            String v = cluster.sync().get(StrUtil.format("p:r:d:{}", did));
            if (JSONUtil.isTypeJSONObject(v)) {
                /**
                 * {
                 *   "deviceId": 100000652,
                 *   "platform": "njqz",
                 *   "relations": [
                 *     {
                 *       "platform": "njqz",
                 *       "vin": "31910488",
                 *       "tenantId": 0,
                 *       "groupPath": "b8d2d42c017911e8ad67000c292ded3c#66101a5e8b0a4989a3d860ec0ede44c9",
                 *       "groupId": "66101a5e8b0a4989a3d860ec0ede44c9",
                 *       "vehicleTypeId": "8cdb04ff538640029882e05b7637a147",
                 *       "startTime": "20191224165907",
                 *       "endTime": "20991231000000",
                 *       "vId": 100005373
                 *     }
                 *   ]
                 * }
                 */
                //将设备的对应关系存储
                relations = new ArrayList<>();
                for (JSONObject j : JSONUtil.parseObj(v).getJSONArray("relations").toList(JSONObject.class)) {
                    relations.add(new HashMap<String, String>() {{
                        put("startTime", j.getStr("startTime"));
                        put("endTime", j.getStr("endTime"));
                        put("vId", j.getStr("vId"));
                    }});
                }
                relationsMap.put(did, relations);
                //log.info("在redis中 {} 找到了 {} 个对应关系 分区缓存大小 {} 已读redis缓存大小 {}", did, relations.size(), relationsMap.size(), readRedisSet.size());
            } else {
                relationsMap.put(did, null);//设置null避免下次还要查询redis
                //log.warn("在redis中 {} 找不到对应关系，不会写入frequent.v_c表中", did);
            }
        }
        if (CollUtil.isNotEmpty(relations)) {
            for (Map<String, String> relation : relations) {
                String startTime = relation.get("startTime");
                String endTime = relation.get("endTime");
                if (startTime.compareTo(yyyyMMddHHmmss) <= 0 && yyyyMMddHHmmss.compareTo(endTime) <= 0) {
                    return relation.get("vId");
                }
            }
        }
        return null;
    }

}