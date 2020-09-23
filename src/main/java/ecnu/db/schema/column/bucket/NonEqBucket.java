package ecnu.db.schema.column.bucket;

import java.math.BigDecimal;

/**
 * @author alan
 */
public class NonEqBucket {
    public BigDecimal probability;
    public String value;
    public NonEqBucket leftBucket;
    public NonEqBucket rightBucket;
    public EqBucket child;

    /**
     * 找到大于{@code probability}的区间
     *
     * @param probability 需要查询的概率
     * @return 区间
     */
    public NonEqBucket search(BigDecimal probability) {
        if (this.probability == null) {
            return this;
        }
        if (probability.compareTo(this.probability) < 0) {
            return leftBucket.search(probability);
        } else if (probability.compareTo(this.probability) > 0) {
            return rightBucket.search(probability);
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
