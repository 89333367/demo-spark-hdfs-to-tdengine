package sunyu.demo.domain;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import org.apache.spark.Partitioner;

import java.util.HashMap;
import java.util.Map;

/**
 * key分区器
 * <p>
 * 一个key一个分区
 *
 * @author 孙宇
 */
public class TimePartitioner extends Partitioner {
    private Map<Object, Integer> keyPartitionMap;

    public TimePartitioner() {
        keyPartitionMap = new HashMap<>();
        int i = 0;
        for (DateTime dateTime : DateUtil.range(DateUtil.beginOfDay(DateTime.now()), DateUtil.endOfDay(DateTime.now()), DateField.MINUTE)) {
            keyPartitionMap.put(dateTime.toString("HHmm"), i);
            i++;
        }
    }

    @Override
    public int numPartitions() {
        return keyPartitionMap.size();
    }

    @Override
    public int getPartition(Object key) {
        return keyPartitionMap.get(key);
    }
}