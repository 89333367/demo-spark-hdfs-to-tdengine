package sunyu.demo.util;

import org.apache.spark.Partitioner;

public class DevicePartitioner extends Partitioner {
    private final int numPartitions;

    public DevicePartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        String deviceId = key.toString();
        // 使用 Jenkins Hash 函数生成哈希值
        int hash = MyHash.hash(deviceId);
        // 将哈希值模上分区数得到分区号
        return (hash & 0x7fffffff) % numPartitions;
    }
}
