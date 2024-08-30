package sunyu.demo.test;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ReUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import org.junit.jupiter.api.Test;
import sunyu.demo.Main;

public class Tests {
    Log log = LogFactory.get();

    @Test
    void t() {
        Main.main(null);
    }

    @Test
    void t001() {
        String hdfsPath = "/spark/farm_can/2024/08/27/part-000151724727600000";
        log.info("{}", DateUtil.parse(ReUtil.getGroup1(".*/spark/farm_can/(\\d{4}/\\d{2}/\\d{2})/.*", hdfsPath)).toString("yyyyMMdd"));
        hdfsPath = "hdfs://master020:9020/spark/farm_can/2024/08/27/part-000151724727600000";
        log.info("{}", DateUtil.parse(ReUtil.getGroup1(".*/spark/farm_can/(\\d{4}/\\d{2}/\\d{2})/.*", hdfsPath)).toString("yyyyMMdd"));
        hdfsPath = "/spark/farm_can/2024/08/27/*";
        log.info("{}", DateUtil.parse(ReUtil.getGroup1(".*/spark/farm_can/(\\d{4}/\\d{2}/\\d{2})/.*", hdfsPath)).toString("yyyyMMdd"));
        hdfsPath = "hdfs://master020:9020/spark/farm_can/2024/08/27/*";
        log.info("{}", DateUtil.parse(ReUtil.getGroup1(".*/spark/farm_can/(\\d{4}/\\d{2}/\\d{2})/.*", hdfsPath)).toString("yyyyMMdd"));
    }

    @Test
    void t002() {
        String hdfsPath = "/spark/farm_can/2024/08/27/*";
        DateTime day = DateUtil.parse(ReUtil.getGroup1(".*/spark/farm_can/(\\d{4}/\\d{2}/\\d{2})/.*", hdfsPath));
        log.info("处理 {} 异常数据", day.toString("yyyy-MM-dd"));
        for (DateTime dateTime : DateUtil.range(day, DateUtil.endOfDay(day), DateField.HOUR)) {
            log.info("{} {}", dateTime.toString(), dateTime.offsetNew(DateField.HOUR, 1).toString());
        }
    }

    @Test
    void t003() {
        String startTime = "20191224165907";
        String endTime = "20991231000000";
        String testTime = "20180828152600";

        boolean isWithinRange = isTimeWithinRange(testTime, startTime, endTime);
        log.info("Is the time within range? " + isWithinRange);
    }


    public static boolean isTimeWithinRange(String time, String startTime, String endTime) {
        return time.compareTo(startTime) >= 0 && time.compareTo(endTime) <= 0;
    }

    @Test
    void t004() {
        DateTime day = DateTime.now();
        for (DateTime dateTime : DateUtil.range(DateUtil.beginOfDay(day), DateUtil.endOfDay(day), DateField.HOUR_OF_DAY)) {
            log.info("{} {}", dateTime, DateUtil.offsetHour(dateTime, 1));
        }
    }
}
