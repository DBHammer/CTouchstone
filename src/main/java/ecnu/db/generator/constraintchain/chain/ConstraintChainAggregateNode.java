package ecnu.db.generator.constraintchain.chain;

import ecnu.db.generator.constraintchain.filter.Parameter;
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
        this.aggProbability = aggProbability;
    }

    public ConstraintChainAggregateNode(List<String> groupKeys, BigDecimal aggProbability) {
        super(ConstraintChainNodeType.AGGREGATE);
        this.groupKey = groupKeys;
        this.aggProbability = aggProbability;
    }

    public ConstraintChainAggregateNode() {
        super(ConstraintChainNodeType.AGGREGATE);
    }

    public boolean removeAgg(Set<String> allTables) {
        if (groupKey == null) {
            if (aggFilter == null) {
                return true;
            } else {
                return aggFilter.getParameters().stream().allMatch(Parameter::isActual);
            }
        } else {
            TreeMap<String, List<String>> table2keys = cleanGroupKeys();
            //todo deal with fk and attributes
            if (groupKey.stream().noneMatch(key -> TableManager.getInstance().isPrimaryKeyOrForeignKey(key))) {
                return true;
            } else {
                if (table2keys.size() == 1 &&
                        allTables.stream().anyMatch(tableName -> TableManager.getInstance().isRefTable(tableName, table2keys.firstKey()))) {
                    logger.error("不能在查询中支持聚集算子 {}", this);
                    return true;
                } else {
                    return false;
                }
            }
        }
    }


    private TreeMap<String, List<String>> cleanGroupKeys() {
        TreeMap<String, List<String>> table2keys = mapGroupKeys();
        if (table2keys.size() > 1) {
            List<String> topologicalOrder = TableManager.getInstance().createTopologicalOrder();
            for (String tableName : topologicalOrder) {
                List<String> keys = table2keys.get(tableName);
                // clean attributes of primary table
                if (keys != null) {
                    // if the first group attribute is fk, remove all its referenced table
                    if (keys.size() == 1 && TableManager.getInstance().isPrimaryKeyOrForeignKey(keys.get(0))) {
                        Iterator<String> keysIterator = groupKey.iterator();
                        while (keysIterator.hasNext()) {
                            String[] remoteArray = keysIterator.next().split("\\.");
                            String remoteTable = remoteArray[0] + "." + remoteArray[1];
                            if (TableManager.getInstance().isRefTable(tableName, remoteTable)) {
                                keysIterator.remove();
                            }
                        }
                    }
                    break;
                }
            }
        }
        return mapGroupKeys();
    }

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
