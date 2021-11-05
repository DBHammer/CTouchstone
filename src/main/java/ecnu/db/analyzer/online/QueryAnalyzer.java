package ecnu.db.analyzer.online;

import ecnu.db.dbconnector.DbConnector;
import ecnu.db.generator.constraintchain.chain.*;
import ecnu.db.generator.constraintchain.filter.LogicNode;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.utils.exception.analyze.UnsupportedSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;
import static ecnu.db.utils.CommonUtils.CANONICAL_NAME_CONTACT_SYMBOL;

public class QueryAnalyzer {

    protected static final Logger logger = LoggerFactory.getLogger(QueryAnalyzer.class);
    private final AbstractAnalyzer abstractAnalyzer;
    private final DbConnector dbConnector;
    protected double skipNodeThreshold = 0.01;

    private static final int SKIP_CHAIN = -2;


    public QueryAnalyzer(AbstractAnalyzer abstractAnalyzer, DbConnector dbConnector) {
        this.abstractAnalyzer = abstractAnalyzer;
        this.dbConnector = dbConnector;
    }

    public void setSkipNodeThreshold(double skipNodeThreshold) {
        this.skipNodeThreshold = skipNodeThreshold;
    }

    public void setAliasDic(Map<String, String> aliasDic) {
        abstractAnalyzer.setAliasDic(aliasDic);
    }


    /**
     * 根据输入的列名统计非重复值的个数，进而给出该列是否为主键
     *
     * @param pkTable 需要测试的主表
     * @param pkCol   主键
     * @param fkTable 外表
     * @param fkCol   外键
     * @return 该列是否为主键
     * @throws TouchstoneException 由于逻辑错误无法判断是否为主键的异常
     * @throws SQLException        无法通过数据库SQL查询获得多列属性的ndv
     */
    private boolean isPrimaryKey(String pkTable, String pkCol, String fkTable, String fkCol) throws TouchstoneException, SQLException {
        if (TableManager.getInstance().isRefTable(fkTable, fkCol, pkTable + "." + pkCol)) {
            return true;
        }
        if (TableManager.getInstance().isRefTable(pkTable, pkCol, fkTable + "." + fkCol)) {
            return false;
        }
        if (!pkCol.contains(",")) {
            if (ColumnManager.getInstance().getNdv(pkTable + CANONICAL_NAME_CONTACT_SYMBOL + pkCol) ==
                    ColumnManager.getInstance().getNdv(fkTable + CANONICAL_NAME_CONTACT_SYMBOL + fkCol)) {
                return TableManager.getInstance().getTableSize(pkTable) < TableManager.getInstance().getTableSize(fkTable);
            } else {
                int pkTableNdv = ColumnManager.getInstance().getNdv(pkTable + CANONICAL_NAME_CONTACT_SYMBOL + pkCol);
                int fkTableNdv = ColumnManager.getInstance().getNdv(fkTable + CANONICAL_NAME_CONTACT_SYMBOL + fkCol);
                return pkTableNdv > fkTableNdv;
            }
        } else {
            int leftTableNdv = dbConnector.getMultiColNdv(pkTable, pkCol);
            int rightTableNdv = dbConnector.getMultiColNdv(fkTable, fkCol);
            if (leftTableNdv == rightTableNdv) {
                return TableManager.getInstance().getTableSize(pkTable) < TableManager.getInstance().getTableSize(fkTable);
            } else {
                return leftTableNdv > rightTableNdv;
            }
        }
    }

    /**
     * 分析一个节点，提取约束链信息
     *
     * @param node            需要分析的节点
     * @param constraintChain 约束链
     * @return 节点行数，小于零代表停止继续向上分析
     * @throws TouchstoneException 节点分析出错
     * @throws SQLException        无法收集多列主键的ndv
     */
    private int analyzeNode(ExecutionNode node, ConstraintChain constraintChain, int lastNodeLineCount) throws TouchstoneException, SQLException {
        return switch (node.getType()) {
            case join, outerJoin, antiJoin -> analyzeJoinNode(node, constraintChain, lastNodeLineCount);
            case filter -> analyzeSelectNode(node, constraintChain, lastNodeLineCount);
            case scan -> throw new TouchstoneException(String.format("中间节点'%s'不为scan", node.getId()));
            case aggregate -> analyzeAggregateNode(node, constraintChain, lastNodeLineCount);
        };
    }

    private int analyzeSelectNode(ExecutionNode node, ConstraintChain constraintChain, int lastNodeLineCount) throws TouchstoneException {
        LogicNode root = analyzeSelectInfo(node.getInfo());
        BigDecimal ratio = BigDecimal.valueOf(node.getOutputRows()).divide(BigDecimal.valueOf(lastNodeLineCount), BIG_DECIMAL_DEFAULT_PRECISION);
        ConstraintChainFilterNode filterNode = new ConstraintChainFilterNode(ratio, root);
        constraintChain.addNode(filterNode);
        return node.getOutputRows();
    }

    private int analyzeAggregateNode(ExecutionNode node, ConstraintChain constraintChain, int lastNodeLineCount) throws TouchstoneException {
        if(node.getInfo().length()!=0) {
            LogicNode root = analyzeSelectInfo(node.getInfo());
            BigDecimal ratio = BigDecimal.valueOf(node.getRowsAfterFilter()).divide(BigDecimal.valueOf(lastNodeLineCount), BIG_DECIMAL_DEFAULT_PRECISION);
            ConstraintChainFilterNode filterNode = new ConstraintChainFilterNode(ratio, root);
            constraintChain.addNode(filterNode);
            ConstraintChainAggregateNode aggregateNode = new ConstraintChainAggregateNode(node.getGroupKey(),node.getInfo(),node.getOutputRows(), node.getRowsAfterFilter());
            constraintChain.addNode(aggregateNode);
        }else{
            ConstraintChainAggregateNode aggregateNode = new ConstraintChainAggregateNode(node.getGroupKey(),node.getInfo(),node.getOutputRows(), node.getRowsAfterFilter());
            constraintChain.addNode(aggregateNode);
        }
        return node.getOutputRows();
    }

    private int analyzeJoinNode(ExecutionNode node, ConstraintChain constraintChain, int lastNodeLineCount) throws TouchstoneException, SQLException {
        String[] joinColumnInfos = abstractAnalyzer.analyzeJoinInfo(node.getInfo());
        String localTable = joinColumnInfos[0];
        String localCol = joinColumnInfos[1];
        String externalTable = joinColumnInfos[2];
        String externalCol = joinColumnInfos[3];
        // 如果当前的join节点，不属于之前遍历的节点，则停止继续向上访问
        if (!localTable.equals(constraintChain.getTableName())
                && !externalTable.equals(constraintChain.getTableName())) {
            return -1;
        }
        //将本表的信息放在前面，交换位置
        if (constraintChain.getTableName().equals(externalTable)) {
            localTable = joinColumnInfos[2];
            localCol = joinColumnInfos[3];
            externalTable = joinColumnInfos[0];
            externalCol = joinColumnInfos[1];
        }
        //根据主外键分别设置约束链输出信息
        if (isPrimaryKey(localTable, localCol, externalTable, externalCol)) {
            if (TableManager.getInstance().getTableSize(localTable) == lastNodeLineCount) {
                node.setJoinTag(-1);
                return SKIP_CHAIN;
            }
            node.setJoinTag(TableManager.getInstance().getJoinTag(localTable));
            ConstraintChainPkJoinNode pkJoinNode = new ConstraintChainPkJoinNode(node.getJoinTag(), localCol.split(","));
            constraintChain.addNode(pkJoinNode);
            //设置主键
            TableManager.getInstance().setPrimaryKeys(localTable, localCol);
            return -1; // 主键的情况下停止继续遍历
        } else {
            if (node.getJoinTag() < 0) {
                logger.info("由于join节点对应的主键输入为全集，跳过节点{}", node.getInfo());
                return node.getOutputRows();
            }
            BigDecimal probability = BigDecimal.valueOf((double) node.getOutputRows() / lastNodeLineCount);
            TableManager.getInstance().setForeignKeys(localTable, localCol, externalTable, externalCol);
            ConstraintChainFkJoinNode fkJoinNode = new ConstraintChainFkJoinNode(localTable + "." + localCol, externalTable + "." + externalCol, node.getJoinTag(), probability);
            if(node.getType() == ExecutionNode.ExecutionNodeType.antiJoin){
                fkJoinNode.setAntiJoin();
            }
            constraintChain.addNode(fkJoinNode);
            return node.getOutputRows();
        }
    }

    /**
     * 获取一条路径上的约束链
     *
     * @param path 需要处理的路径
     * @return 获取的约束链
     * @throws TouchstoneException 无法处理路径
     * @throws SQLException        无法采集多列主键的ndv
     */
    private ConstraintChain extractConstraintChain(List<ExecutionNode> path) throws TouchstoneException, SQLException {
        if (path == null || path.isEmpty()) {
            throw new TouchstoneException(String.format("非法的path输入 '%s'", path));
        }
        ExecutionNode node = path.get(0);
        ConstraintChain constraintChain;
        int lastNodeLineCount;
        //分析约束链的第一个node
        switch (node.getType()) {
            case scan -> {
                constraintChain = new ConstraintChain(node.getTableName());
                lastNodeLineCount = node.getOutputRows();
            }
            case filter -> {
                LogicNode result = analyzeSelectInfo(node.getInfo());
                constraintChain = new ConstraintChain(node.getTableName());
                BigDecimal ratio = BigDecimal.valueOf(node.getOutputRows()).divide(BigDecimal.valueOf(TableManager.getInstance().getTableSize(node.getTableName())), BIG_DECIMAL_DEFAULT_PRECISION);
                ConstraintChainFilterNode filterNode = new ConstraintChainFilterNode(ratio, result);
                constraintChain.addNode(filterNode);
                lastNodeLineCount = node.getOutputRows();
            }
            default -> throw new TouchstoneException(String.format("底层节点 %s 只能为select或者scan", node.getId()));
        }
        for (int i = 1; i < path.size(); i++) {
            node = path.get(i);
            try {
                lastNodeLineCount = analyzeNode(node, constraintChain, lastNodeLineCount);
                if (lastNodeLineCount == SKIP_CHAIN && constraintChain.getNodes().isEmpty()) {
                    return null;
                }
            } catch (TouchstoneException e) {
                // 小于设置的阈值以后略去后续的节点
                if (node.getOutputRows() * 1.0 / TableManager.getInstance().getTableSize(node.getTableName()) < skipNodeThreshold) {
                    logger.error("提取约束链失败", e);
                    logger.info(String.format("%s, 但节点行数与tableSize比值小于阈值，跳过节点%s", e.getMessage(), node));
                    return constraintChain;
                }
                throw e;
            }
            if (lastNodeLineCount < 0) {
                logger.error("约束链中的size不正常");
                return constraintChain;
            }
        }
        return constraintChain;
    }

    /**
     * 将树结构根据叶子节点分割为不同的path
     *
     * @param currentNode 需要处理的查询树节点
     * @param paths       需要返回的路径
     */
    private void getPathsIterate(ExecutionNode currentNode, List<List<ExecutionNode>> paths, List<ExecutionNode> currentPath) {
        currentPath.add(0, currentNode);
        if (currentNode.leftNode == null && currentNode.rightNode == null) {
            paths.add(new ArrayList<>(currentPath));
        }
        if (currentNode.leftNode != null) {
            getPathsIterate(currentNode.leftNode, paths, currentPath);
        }
        if (currentNode.rightNode != null) {
            getPathsIterate(currentNode.rightNode, paths, currentPath);
        }
        currentPath.remove(0);
    }

    /**
     * 获取查询树的约束链信息和表信息
     *
     * @param query 查询语句
     * @return 该查询树结构出的约束链信息和表信息
     */
    public List<List<ConstraintChain>> extractQuery(String query) throws SQLException, TouchstoneException {
        List<List<ConstraintChain>> constraintChains = new ArrayList<>();
        List<String[]> queryPlan = dbConnector.explainQuery(query);
        List<List<String[]>> queryPlans = abstractAnalyzer.splitQueryPlan(queryPlan);
        try {
            for (List<String[]> plan : queryPlans) {
                List<ConstraintChain> currentConstraintChains = new ArrayList<>();
                ExecutionNode executionTree = abstractAnalyzer.getExecutionTree(plan);
                //获取查询树的所有路径
                List<List<ExecutionNode>> paths = new ArrayList<>();
                getPathsIterate(executionTree, paths, new LinkedList<>());
                CommonUtils.setForkJoinParallelism(paths.size());
                // 并发处理约束链
                currentConstraintChains.addAll(paths.parallelStream().map(path -> {
                    try {
                        return extractConstraintChain(path);
                    } catch (TouchstoneException | SQLException e) {
                        logger.error(path.toString(), e);
                        return null;
                    }
                }).filter(Objects::nonNull).toList());
                constraintChains.add(currentConstraintChains);
            }
        } catch (TouchstoneException e) {
            if (queryPlan != null && !queryPlan.isEmpty()) {
                String queryPlanContent = queryPlan.stream().map(plan -> String.join("\t", plan))
                        .collect(Collectors.joining(System.lineSeparator()));
                logger.error(queryPlanContent, e);
            }
        }
        logger.info("Status:获取完成");
        return constraintChains;
    }

    /**
     * 分析select信息
     *
     * @param operatorInfo 需要分析的operator_info
     * @return 分析查询的逻辑树
     * @throws TouchstoneException 分析失败
     */
    private synchronized LogicNode analyzeSelectInfo(String operatorInfo) throws TouchstoneException {
        try {
            return abstractAnalyzer.analyzeSelectOperator(operatorInfo);
        } catch (Exception e) {
            throw new UnsupportedSelect(operatorInfo, e);
        }
    }

}
