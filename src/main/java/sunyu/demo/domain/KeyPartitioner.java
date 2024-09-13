package sunyu.demo.domain;

import org.apache.spark.Partitioner;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * key分区器
 * <p>
 * 一个key一个分区
 *
 * @author 孙宇
 */
public class KeyPartitioner extends Partitioner {
    private Map<Object, Integer> keyPartitionMap;

    public KeyPartitioner(Collection<String> keys) {
        keyPartitionMap = new HashMap<>();
        int i = 0;
        for (String key : keys) {
            keyPartitionMap.put(key, i);//给每个设备一个编号
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