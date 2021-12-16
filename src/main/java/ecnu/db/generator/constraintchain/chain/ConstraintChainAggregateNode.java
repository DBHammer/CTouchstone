package ecnu.db.generator.constraintchain.chain;

import ecnu.db.schema.TableManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class ConstraintChainAggregateNode extends ConstraintChainNode {
    private List<String> groupKey;
    private BigDecimal aggProbability;
    ConstraintChainFilterNode aggFilter;
    private final Logger logger = LoggerFactory.getLogger(ConstraintChainAggregateNode.class);

    public BigDecimal getAggProbability() {
        return aggProbability;
    }

    public void setAggProbability(BigDecimal aggProbability) {
        this.aggProbability = aggProbability.stripTrailingZeros();
    }

    public ConstraintChainAggregateNode(List<String> groupKeys, BigDecimal aggProbability) {
        super(ConstraintChainNodeType.AGGREGATE);
        this.groupKey = groupKeys;
        this.aggProbability = aggProbability.stripTrailingZeros();
    }

    public ConstraintChainAggregateNode() {
        super(ConstraintChainNodeType.AGGREGATE);
    }

    public boolean removeAgg() {
        // 如果filter含有虚参，则不能被约减。其需要参与计算。
        if (aggFilter != null && aggFilter.getParameters().stream().anyMatch(parameter -> !parameter.isActual())) {
            return false;
        }
        // filter不再需要被计算，只需要考虑group key的情况
        // 如果没有group key 则不需要进行分布控制 无需考虑
        if (groupKey == null) {
            return true;
        }
        // 清理group key， 如果含有参照表的外键，则clean被参照表的group key
        cleanGroupKeys();
        // 如果group key中全部是外键 则需要控制外键分布 不能删减
        if (groupKey.stream().allMatch(key -> TableManager.getInstance().isForeignKey(key))) {
            return false;
        }
        // 如果group key中包含主键 且无法支持 提示报错
        if (groupKey.stream().anyMatch(key -> TableManager.getInstance().isPrimaryKey(key))) {
            logger.error("不能在查询中支持聚集算子 {}", this);
        }
        return true;
    }

    // todo filter the attributes of the same table
    private void cleanGroupKeys() {
        TreeMap<String, List<String>> table2keys = mapGroupKeys();
        if (table2keys.size() > 1) {
            List<String> topologicalOrder = TableManager.getInstance().createTopologicalOrder();
            Collections.reverse(topologicalOrder);
            // 从参照表到被参照表进行访问
            for (String tableName : topologicalOrder) {
                List<String> keys = table2keys.get(tableName);
                // clean attributes of primary table
                if (keys != null) {
                    // if the first group attribute is fk, remove all its referenced table
                    if (keys.stream().anyMatch(key -> TableManager.getInstance().isForeignKey(key))) {
                        for (Map.Entry<String, List<String>> table2keyList : table2keys.entrySet()) {
                            if (TableManager.getInstance().isRefTable(tableName, table2keyList.getKey())) {
                                logger.debug("remove invalid group key {} from node {}", table2keyList.getValue(), this);
                                groupKey.removeAll(table2keyList.getValue());
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    /**
     * 按表聚集
     *
     * @return 表和对应的group key
     */
    private TreeMap<String, List<String>> mapGroupKeys() {
        TreeMap<String, List<String>> table2keys = new TreeMap<>();
        for (String key : groupKey) {
            String[] array = key.split("\\.");
            String tableName = array[0] + "." + array[1];
            if (!table2keys.containsKey(tableName)) {
                table2keys.put(tableName, new ArrayList<>());
            }
            table2keys.get(tableName).add(key);
        }
        return table2keys;
    }

    public ConstraintChainFilterNode getAggFilter() {
        return aggFilter;
    }

    public void setAggFilter(ConstraintChainFilterNode aggFilter) {
        this.aggFilter = aggFilter;
    }

    @Override
    public String toString() {
        return String.format("{GroupKey:%s, aggProbability:%s}", groupKey, aggProbability);
    }

    public List<String> getGroupKey() {
        return groupKey;
    }

    public void setGroupKey(List<String> groupKey) {
        this.groupKey = groupKey;
    }
}
