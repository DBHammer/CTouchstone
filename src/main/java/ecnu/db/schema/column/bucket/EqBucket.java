package ecnu.db.schema.column.bucket;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.filter.Parameter;

import java.math.BigDecimal;

/**
 * @author alan
 */
public class EqBucket implements Comparable<EqBucket> {
    // eq bucket对应的non-eq bucket
    public NonEqBucket parent;
    // eq bucket的容量
    public BigDecimal capacity;
    // eq bucket的左边界
    public BigDecimal leftBorder;
    // eq bucket的右边界
    public BigDecimal rightBorder;
    // eq bucket中包含的相等条件，概率->参数
    public Multimap<BigDecimal, Parameter> eqConditions = ArrayListMultimap.create();

    public EqBucket() {
    }

    public EqBucket(NonEqBucket parent, BigDecimal capacity, BigDecimal leftBorder, BigDecimal rightBorder) {
        this.parent = parent;
        this.capacity = capacity;
        this.leftBorder = leftBorder;
        this.rightBorder = rightBorder;
    }

    @Override
    public int compareTo(EqBucket bucket) {
        return bucket.capacity.compareTo(capacity);
    }

    public boolean hasSpaceFor(BigDecimal probability) {
        return leftBorder.add(capacity).compareTo(probability) >= 0;
    }
}
