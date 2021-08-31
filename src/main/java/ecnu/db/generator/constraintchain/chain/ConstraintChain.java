package ecnu.db.generator.constraintchain.chain;

import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.utils.exception.TouchstoneException;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

/**
 * @author wangqingshuai
 */
public class ConstraintChain {

    private final List<ConstraintChainNode> nodes = new ArrayList<>();

    private String tableName;

    public ConstraintChain() {
    }

    public ConstraintChain(String tableName) {
        this.tableName = tableName;
    }

    public void addNode(ConstraintChainNode node) {
        nodes.add(node);
    }

    public List<ConstraintChainNode> getNodes() {
        return nodes;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }


    public List<Parameter> getParameters() {
        return nodes.stream().filter(ConstraintChainFilterNode.class::isInstance)
                .map(ConstraintChainFilterNode.class::cast)
                .map(ConstraintChainFilterNode::getParameters)
                .flatMap(Collection::stream).toList();
    }

    @Override
    public String toString() {
        return "{tableName:" + tableName + ",nodes:" + nodes + "}";
    }

    /**
     * 计算pk和fk的bitmap
     *
     * @param pkBitMap  pkTag -> bitmaps
     * @param fkBitMaps ref_col+local_col -> bitmaps
     * @throws TouchstoneException 计算失败
     */
    public void evaluate(long[] pkBitMap, Map<String, long[]> fkBitMaps) throws TouchstoneException {
        boolean[] flag = new boolean[pkBitMap.length];
        Arrays.fill(flag, true);
        for (ConstraintChainNode node : nodes) {
            switch (node.getConstraintChainNodeType()) {
                case FILTER:
                    boolean[] evaluateStatus = ((ConstraintChainFilterNode) node).evaluate();
                    IntStream.range(0, pkBitMap.length).parallel().forEach(i -> flag[i] &= evaluateStatus[i]);
                    break;
                case FK_JOIN:
                    ConstraintChainFkJoinNode constraintChainFkJoinNode = (ConstraintChainFkJoinNode) node;
                    double probability = constraintChainFkJoinNode.getProbability().doubleValue();
                    long[] fkBitMap = fkBitMaps.computeIfAbsent(constraintChainFkJoinNode.getLocalCols() + ":" +
                            constraintChainFkJoinNode.getRefCols(), k -> new long[pkBitMap.length]);
                    long fkTag = constraintChainFkJoinNode.getPkTag();
                    IntStream.range(0, pkBitMap.length).parallel()
                            .filter(i -> flag[i])
                            .forEach(i -> {
                                //todo 引入规则
                                flag[i] &= ThreadLocalRandom.current().nextDouble() < probability;
                                fkBitMap[i] += fkTag * (1 + Boolean.compare(flag[i], false));
                            });
                    break;
                case PK_JOIN:
                    long pkTag = ((ConstraintChainPkJoinNode) node).getPkTag();
                    IntStream.range(0, flag.length).parallel()
                            .forEach(i -> pkBitMap[i] += pkTag * (1 + Boolean.compare(flag[i], false)));
                    break;
                default:
                    throw new UnsupportedOperationException("不支持的Node类型");
            }
        }
    }
}
