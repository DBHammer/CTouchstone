package ecnu.db.joininfo;

import ecnu.db.utils.exception.TouchstoneException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static java.lang.Math.*;

public class JoinInfoTable implements Externalizable {
    /**
     * 最大可以容纳的链表长度，超过该长度会触发压缩
     */
    private static int maxListSize = 1000;
    /**
     * 记录Reservoir sampling Algorithm L 中的状态数据结构，对于每个status list记录三个状态值，按照顺序为
     * 1. 当前list被add的次数n
     * 2. 当前list下一个允许被add的item的index
     * 3. 随机概率值W
     * 使用double记录次数可以保证，在 2^52≈4.5E15 范围内的整数的次数可以被准确的记录，而数据生成的任务量，一般不会达到这个量级
     */
    private final Map<Long, double[]> counters = new HashMap<>();
    /**
     * 复合主键的数量
     */
    private int primaryKeySize;
    /**
     * join info table，map status -> key list
     */
    private Map<Long, List<int[]>> joinInfo = new HashMap<>();


    public JoinInfoTable() {
    }

    public JoinInfoTable(int primaryKeySize) {
        this.primaryKeySize = primaryKeySize;
    }

    public static void setMaxListSize(int maxListSize) {
        JoinInfoTable.maxListSize = maxListSize;
    }

    public void mergeJoinInfo(JoinInfoTable toMergeTable) throws TouchstoneException {
        if (primaryKeySize != toMergeTable.primaryKeySize) {
            throw new TouchstoneException("复合主键的size不同");
        }
        toMergeTable.joinInfo.forEach((k, v) -> {
            joinInfo.merge(k, v, (v1, v2) -> {
                v1.addAll(v2);
                return v1;
            });
        });
    }

    /**
     * 根据join status获取符合条件的主键值
     *
     * @param status join status
     * @return 一个复合主键
     */
    public int[] getPrimaryKey(long status) {
        List<int[]> keyList = joinInfo.get(status);
        return keyList.get(ThreadLocalRandom.current().nextInt(keyList.size()));
    }

    /**
     * 根据join status获取符合条件的主键值
     *
     * @param status join status
     * @return 所有复合主键
     */
    public List<int[]> getAllKeys(long status) {
        return joinInfo.get(status);
    }

    /**
     * 插入符合这一status的一组KeyId
     * reservoir sampling following Algorithm L in "Reservoir-Sampling Algorithms of Time Complexity O(n(1+log(N/n)))"
     *
     * @param status join status
     * @param key    一个复合主键
     */
    public void addJoinInfo(long status, int[] key) {
        //如果不存在该status，初始化计数器，predict id和权重w
        double[] counter = counters.computeIfAbsent(status, k -> {
                    double w = exp(log(1 - ThreadLocalRandom.current().nextDouble()) / maxListSize);
                    return new double[]{0, maxListSize + predictIdOffset(w), w};
                }
        );
        counter[0]++;
        if (counter[0] <= maxListSize) {
            joinInfo.computeIfAbsent(status, k -> new ArrayList<>(maxListSize)).add(key);
        } else {
            if (counter[0] >= counter[1]) {
                counter[1] += predictIdOffset(counter[2]);
                joinInfo.get(status).set(ThreadLocalRandom.current().nextInt(maxListSize), key);
                counter[2] *= exp(log(1 - ThreadLocalRandom.current().nextDouble()) / maxListSize);
            }
        }
    }

    /**
     * 计算下一个可以被加入的index
     *
     * @param w 权重weight
     * @return 下一个可以被加入的index
     */
    private double predictIdOffset(double w) {
        return floor(log(1 - ThreadLocalRandom.current().nextDouble()) / log(1 - w)) + 1;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(primaryKeySize);
        out.writeInt(joinInfo.size());
        for (Long bitmap : joinInfo.keySet()) {
            out.writeLong(bitmap);
            List<int[]> keys = joinInfo.get(bitmap);
            out.writeInt(keys.size());
            for (int[] keyIds : keys) {
                for (Integer keyId : keyIds) {
                    out.writeInt(keyId);
                }
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        primaryKeySize = in.readInt();
        int joinInfoSize = in.readInt();
        joinInfo = new HashMap<>(joinInfoSize);
        for (int i = 0; i < joinInfoSize; i++) {
            Long bitmap = in.readLong();
            int keyListSize = in.readInt();
            for (int j = 0; j < keyListSize; j++) {
                int[] keyId = new int[primaryKeySize];
                for (int k = 0; k < primaryKeySize; k++) {
                    keyId[k] = in.readInt();
                }
                joinInfo.compute(bitmap, (b, keys) -> {
                    if (keys == null) {
                        keys = new ArrayList<>(maxListSize);
                    }
                    keys.add(keyId);
                    return keys;
                });
            }
        }
    }
}
