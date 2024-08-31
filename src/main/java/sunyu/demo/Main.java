package sunyu.demo;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import cn.hutool.setting.dialect.Props;
import com.zaxxer.hikari.HikariDataSource;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import scala.Tuple2;
import sunyu.demo.domain.Params3Enum;
import sunyu.util.RedisUtil;
import sunyu.util.TDengineUtil;
import uml.tech.bigdata.sdkconfig.ProtocolSdk;

import java.util.*;
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
    static Map<String, List<JSONObject>> relationsMap = new HashMap<>();
    static Set<String> readRedisSet = new HashSet<>();

    public static void main(String[] args) {
        // args
        // hdfsPath partitions
        String hdfsPath;
        int partitions;

        SparkConf sparkConf = new SparkConf();

        if (args == null) {
            //端口使用 HDFS的 NameNode 端口

            //开发
            //hdfsPath = "hdfs://cdh1:8020/spark/farm_can/2024/08/26/part-000151724731200000";
            //hdfsPath = "hdfs://cdh2:8020/spark/farm_can/2024/08/26/part-000151724731200000";

            //生产
            //hdfsPath = "hdfs://master012:9020/spark/farm_can/2024/08/26/part-000151724731200000";
            hdfsPath = "hdfs://master020:9020/spark/farm_can/2024/08/26/part-000151724731200000";
            partitions = 100;

            sparkConf.setAppName("local test");
            sparkConf.setMaster("local[*]");
        } else {
            hdfsPath = args[0];
            partitions = Convert.toInt(args[1], 100);
        }

        log.info("args参数 {}", JSONUtil.toJsonStr(args));

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        javaSparkContext.parallelize(Arrays.asList(0), 1).foreach((VoidFunction<Integer>) integer -> {
            //做数据库删除操作，此操作比较耗时
            DateTime day = DateUtil.parse(DateUtil.parse(ReUtil.getGroup1(".*/spark/farm_can/(\\d{4}/\\d{2}/\\d{2})/.*", hdfsPath)).toString("yyyyMMdd"));
            log.info("删除v_c和d_p数据开始");

            ThreadUtil.execute(() -> {
                String sql = StrUtil.format("delete from frequent.v_c where _rowts>='{} 00:00:00' and _rowts<='{} 23:59:59'"
                        , day.toString(DatePattern.NORM_DATE_FORMAT), day.toString(DatePattern.NORM_DATE_FORMAT));
                log.info(sql);
                tdUtil.executeUpdate(sql, 0, null);
            });

            ThreadUtil.sleep(5000);

            while (true) {
                String sql = "select count(*) c from performance_schema.perf_queries where sql like 'delete from frequent.v_c %'";
                if (tdUtil.executeQuery(sql).size() == 0) {
                    break;
                }
                log.info("v_c还未删除完毕");
                ThreadUtil.sleep(5000);
            }

            ThreadUtil.execute(() -> {
                String sql = StrUtil.format("delete from frequent.d_p where _rowts>='{} 00:00:00' and _rowts<='{} 23:59:59'"
                        , day.toString(DatePattern.NORM_DATE_FORMAT), day.toString(DatePattern.NORM_DATE_FORMAT));
                log.info(sql);
                tdUtil.executeUpdate(sql, 0, null);
            });

            ThreadUtil.sleep(5000);

            while (true) {
                String sql = "select count(*) c from performance_schema.perf_queries where sql like 'delete from frequent.d_p %'";
                if (tdUtil.executeQuery(sql).size() == 0) {
                    break;
                }
                log.info("d_p还未删除完毕");
                ThreadUtil.sleep(5000);
            }

            log.info("删除v_c和d_p数据结束");
        });
        javaSparkContext
                //读取hdfs文件
                .textFile(hdfsPath)
                //转换键值对，键是设备号，值是内部协议字符串
                .mapToPair((PairFunction<String, String, String>) s -> {
                    if (s.length() < 4000) {
                        Map<String, String> m = sdk.parseProtocolString(s);
                        if (MapUtil.isNotEmpty(m) && m.containsKey("3014")) {
                            try {
                                DateUtil.parse(m.get("3014").toString(), DatePattern.PURE_DATETIME_FORMAT);
                                return new Tuple2<>(m.get("did"), s);
                            } catch (Exception e) {
                                log.error("{} GPS时间有问题 {} 此数据抛弃", m.get("did"), m.get("3014"));
                            }
                        }
                    } else {
                        log.error("内部协议长度超过4000字节，数据抛弃 {}", s);
                    }
                    return null; // 如果数据不符合条件，则返回null
                })
                //过滤不符合的数据
                .filter((Function<Tuple2<String, String>, Boolean>) v1 -> v1 != null)
                //按照设备号预分区
                .partitionBy(new HashPartitioner(partitions))
                //按照设备号分组
                .groupByKey()
                //循环分区
                .foreachPartition((VoidFunction<Iterator<Tuple2<String, Iterable<String>>>>) tuple2Iterator -> {
                    //循环分组
                    tuple2Iterator.forEachRemaining(stringIterableTuple2 -> {
                        String did = stringIterableTuple2._1;
                        Iterable<String> datas = stringIterableTuple2._2;

                        // todo insert frequent.d_p
                        for (String data : datas) {
                            TreeMap<String, String> m = sdk.parseProtocolString(data);
                            String p3014 = m.get("3014");
                            tdUtil.insertRow("frequent", "d_p", StrUtil.format("d_p_{}", did), new HashMap<String, Object>() {{
                                put("3014", formatDateTime(p3014));//_rowts
                                put("protocol", data);
                                put("did", did);//tag
                            }});
                        }

                        // todo insert frequent.v_c
                        for (String data : datas) {
                            TreeMap<String, String> m = sdk.parseProtocolString(data);
                            String p3014 = m.get("3014");
                            String vId = getVid(did, p3014, readRedisSet, relationsMap);
                            if (StrUtil.isNotBlank(vId)) {//找到了vid
                                tdUtil.insertRow("frequent", "v_c", StrUtil.format("v_c_{}", vId), new HashMap<String, Object>() {{
                                    //超插入的列，列名必须存在于上面列的定义中
                                    put("3014", formatDateTime(m.get("3014")));
                                    put("TIME", formatDateTime(m.get("TIME")));
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
                                    put("protocol", data);
                                }});
                            }
                        }
                    });
                    //等待分区所有sql执行完毕
                    tdUtil.awaitExecution();
                });

        javaSparkContext.close();
        tdUtil.close();
        redisUtil.close();
    }


    private static String formatDateTime(String yyyyMMddHHmmss) {
        if (yyyyMMddHHmmss == null || yyyyMMddHHmmss.length() != 14) {
            throw new IllegalArgumentException("Invalid date format: " + yyyyMMddHHmmss);
        }
        StringBuilder sb = new StringBuilder(yyyyMMddHHmmss.length() + 5);
        sb.append(yyyyMMddHHmmss.substring(0, 4)).append("-")
                .append(yyyyMMddHHmmss.substring(4, 6)).append("-")
                .append(yyyyMMddHHmmss.substring(6, 8)).append(" ")
                .append(yyyyMMddHHmmss.substring(8, 10)).append(":")
                .append(yyyyMMddHHmmss.substring(10, 12)).append(":")
                .append(yyyyMMddHHmmss.substring(12, 14));
        return sb.toString();
    }

    private static String getVid(String did, String p3014, Set<String> readRedisSet, Map<String, List<JSONObject>> relationsMap) {
        String vId = null;
        List<JSONObject> relations = null;
        if (readRedisSet.contains(did)) {//如果set里面存在这个did，说明已经在redis查询过对应关系
            relations = relationsMap.get(did);
        } else {//在redis中查询对应关系
            readRedisSet.add(did);//标志查过redis了
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
                relations = JSONUtil.parseObj(v).getJSONArray("relations").toList(JSONObject.class);
                relationsMap.put(did, relations);
                log.info("在redis中 {} 找到了 {} 个对应关系", did, relations.size());
            } else {
                log.warn("在redis中 {} 找不到对应关系，不会写入frequent.v_c表中", did);
            }
        }
        if (CollUtil.isNotEmpty(relations)) {
            for (JSONObject relation : relations) {
                String startTime = relation.getStr("startTime");
                String endTime = relation.getStr("endTime");
                if (p3014.compareTo(startTime) >= 0 && p3014.compareTo(endTime) <= 0) {
                    vId = relation.getStr("vId");
                    break;
                }
            }
        }
        return vId;
    }

}