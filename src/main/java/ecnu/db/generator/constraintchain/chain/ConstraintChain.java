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

    public boolean canSkipChain() {
        return nodes.isEmpty() || nodes.stream().anyMatch(p -> p.getConstraintChainNodeType() != ConstraintChainNodeType.FILTER);
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
                case FILTER -> {
                    boolean[] evaluateStatus = ((ConstraintChainFilterNode) node).evaluate();
                    IntStream.range(0, pkBitMap.length).parallel().forEach(i -> flag[i] &= evaluateStatus[i]);
                }
                case FK_JOIN -> {
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
                }
                case PK_JOIN -> {
                    long pkTag = ((ConstraintChainPkJoinNode) node).getPkTag();
                    IntStream.range(0, flag.length).parallel()
                            .forEach(i -> pkBitMap[i] += pkTag * (1 + Boolean.compare(flag[i], false)));
                }
                default -> throw new UnsupportedOperationException("不支持的Node类型");
            }
        }
    }

    public StringBuilder presentConstraintChains(Map<String, SubGraph> subGraphHashMap, String color) {
        String lastNodeInfo = "";
        double lastProbability = 0;
        String conditionColor = String.format("[style=filled, color=\"%s\"];%n", color);
        String tableColor = String.format("[shape=box,style=filled, color=\"%s\"];%n", color);
        StringBuilder graph = new StringBuilder();
        for (ConstraintChainNode node : nodes) {
            String currentNodeInfo;
            double currentProbability = 0;
            switch (node.constraintChainNodeType) {
                case FILTER -> {
                    currentNodeInfo = String.format("\"%s\"", node);
                    currentProbability = ((ConstraintChainFilterNode) node).getProbability().doubleValue();
                    graph.append("\t").append(currentNodeInfo).append(conditionColor);
                }
                case FK_JOIN -> {
                    ConstraintChainFkJoinNode fkJoinNode = ((ConstraintChainFkJoinNode) node);
                    String pkCols = fkJoinNode.getRefCols().split("\\.")[2];
                    currentNodeInfo = String.format("\"Fk%s%d\"", pkCols, fkJoinNode.getPkTag());
                    String subGraphTag = String.format("cluster%s%d", pkCols, fkJoinNode.getPkTag());
                    currentProbability = fkJoinNode.getProbability().doubleValue();
                    subGraphHashMap.putIfAbsent(subGraphTag, new SubGraph(subGraphTag));
                    subGraphHashMap.get(subGraphTag).fkInfo = currentNodeInfo + conditionColor;
                }
                case PK_JOIN -> {
                    ConstraintChainPkJoinNode pkJoinNode = ((ConstraintChainPkJoinNode) node);
                    String locPks = pkJoinNode.getPkColumns()[0];
                    currentNodeInfo = String.format("\"Pk%s%d\"", locPks, pkJoinNode.getPkTag());
                    String localSubGraph = String.format("cluster%s%d", locPks, pkJoinNode.getPkTag());
                    subGraphHashMap.putIfAbsent(localSubGraph, new SubGraph(localSubGraph));
                    subGraphHashMap.get(localSubGraph).pkInfo = currentNodeInfo + conditionColor;
                }
                default -> throw new UnsupportedOperationException();
            }
            if (!lastNodeInfo.isBlank()) {
                graph.append(String.format("\t%s->%s[label=\"%3$,.4f\"];%n", lastNodeInfo, currentNodeInfo, lastProbability));
            } else {
                graph.append(String.format("\t\"%s\"%s", tableName, tableColor));
                graph.append(String.format("\t\"%s\"->%s[label=\"1.0\"]%n", tableName, currentNodeInfo));
            }
            lastNodeInfo = currentNodeInfo;
            lastProbability = currentProbability;
        }
        if (!lastNodeInfo.startsWith("\"Pk")) {
            graph.append("\t").append("RESULT").append(conditionColor);
            graph.append(String.format("\t%s->RESULT[label=\"%2$,.4f\"]%n", lastNodeInfo, lastProbability));
        }
        return graph;
    }

    static class SubGraph {
        private final String joinTag;
        String pkInfo;
        String fkInfo;

        public SubGraph(String joinTag) {
            this.joinTag = joinTag;
        }

        @Override
        public String toString() {
            return String.format("\tsubgraph \"%s\" { %n \t\t%s \t\t%s\t}%n", joinTag, pkInfo, fkInfo);
        }
    }
}
