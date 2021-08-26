package ecnu.db.analyzer.online;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import ecnu.db.generator.constraintchain.chain.ConstraintChain;
import ecnu.db.generator.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.generator.constraintchain.chain.ConstraintChainFkJoinNode;
import ecnu.db.generator.constraintchain.chain.ConstraintChainPkJoinNode;
import ecnu.db.generator.constraintchain.filter.SelectResult;
import ecnu.db.dbconnector.DbConnector;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.SchemaManager;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.utils.exception.analyze.IllegalQueryTableNameException;
import ecnu.db.utils.exception.analyze.UnsupportedSelect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.*;

/**
 * @author wangqingshuai
 */
public abstract class AbstractAnalyzer {

    private static final Pattern CANONICAL_TBL_NAME = Pattern.compile("[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+");
    protected static final Logger logger = LoggerFactory.getLogger(AbstractAnalyzer.class);
    protected NodeTypeTool nodeTypeRef;
    protected double skipNodeThreshold = 0.01;
    protected String defaultDatabase;
    private DbConnector dbConnector;
    private Map<String, String> aliasDic = new HashMap<>();

    public void setAliasDic(Map<String, String> aliasDic) {
        this.aliasDic = aliasDic;
    }

    public void setDefaultDatabase(String defaultDatabase) {
        this.defaultDatabase = defaultDatabase;
    }

    public void setSkipNodeThreshold(double skipNodeThreshold) {
        this.skipNodeThreshold = skipNodeThreshold;
    }

    /**
     * 从operator_info里提取tableName
     *
     * @param operatorInfo 需要处理的operator_info
     * @return 提取的表名
     */
    protected abstract String extractOriginTableName(String operatorInfo) throws IllegalQueryTableNameException;

    /**
     * 查询树的解析
     *
     * @param queryPlan query解析出的查询计划，带具体的行数
     * @return 查询树Node信息
     * @throws TouchstoneException 查询树无法解析
     */
    public abstract ExecutionNode getExecutionTree(List<String[]> queryPlan) throws TouchstoneException;

    /**
     * 分析join信息
     *
     * @param joinInfo join字符串
     * @return 长度为4的字符串数组，0，1为join info左侧的表名和列名，2，3为join右侧的表明和列名
     * @throws TouchstoneException 无法分析的join条件
     */
    protected abstract String[] analyzeJoinInfo(String joinInfo) throws TouchstoneException;

    /**
     * 分析select信息
     *
     * @param operatorInfo 需要分析的operator_info
     * @return SelectResult
     * @throws TouchstoneException 分析失败
     */
    public SelectResult analyzeSelectInfo(String operatorInfo) throws TouchstoneException{
        try {
            return analyzeSelectOperator(operatorInfo);
        } catch (Exception e) {
            String stackTrace = Throwables.getStackTraceAsString(e);
            throw new UnsupportedSelect(operatorInfo, stackTrace);
        }
    }

    protected abstract SelectResult analyzeSelectOperator(String operatorInfo) throws Exception;


    /**
     * 获取查询树的约束链信息和表信息
     *
     * @param query 查询语句
     * @return 该查询树结构出的约束链信息和表信息
     */
    public List<ConstraintChain> extractQuery(String query) throws SQLException {
        List<ConstraintChain> constraintChains = new ArrayList<>();
        List<String[]> queryPlan = dbConnector.explainQuery(query);
        try {
            ExecutionNode executionTree = getExecutionTree(queryPlan);
            for (List<ExecutionNode> path : getPaths(executionTree)) {
                constraintChains.add(extractConstraintChain(path));
            }
        } catch (TouchstoneException | SQLException e) {
            if (queryPlan != null && !queryPlan.isEmpty()) {
                String queryPlanContent = queryPlan.stream().map((strs) -> String.join(";", strs))
                        .collect(Collectors.joining(System.lineSeparator()));
                logger.error(queryPlanContent, e);
            }
        }
        logger.info("Status:获取完成");
        return constraintChains;
    }


    /**
     * 获取查询树的所有路径
     *
     * @param root 需要处理的查询树
     * @return 按照从底部节点到顶部节点形式的所有路径
     */
    private List<List<ExecutionNode>> getPaths(ExecutionNode root) {
        List<List<ExecutionNode>> paths = new ArrayList<>();
        getPathsIterate(root, paths);
        return paths;
    }

    /**
     * getPaths 的内部迭代方法
     *
     * @param root  需要处理的查询树
     * @param paths 需要返回的路径
     */
    private void getPathsIterate(ExecutionNode root, List<List<ExecutionNode>> paths) {
        if (root.leftNode == null && root.rightNode == null) {
            List<ExecutionNode> newPath = Lists.newArrayList(root);
            paths.add(newPath);
            return;
        }
        if (root.leftNode != null) {
            getPathsIterate(root.leftNode, paths);
        }
        if (root.rightNode != null) {
            getPathsIterate(root.rightNode, paths);
        }
        for (List<ExecutionNode> path : paths) {
            path.add(root);
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
        if (path == null || path.size() == 0) {
            throw new TouchstoneException(String.format("非法的path输入 '%s'", path));
        }
        ExecutionNode node = path.get(0);
        ConstraintChain constraintChain;
        String tableName;
        int lastNodeLineCount;
        if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
            SelectResult result = analyzeSelectInfo(node.getInfo());
            tableName = result.getTableName();
            constraintChain = new ConstraintChain(tableName);
            BigDecimal ratio = BigDecimal.valueOf(node.getOutputRows()).divide(BigDecimal.valueOf(SchemaManager.getInstance().getTableSize(tableName)), BIG_DECIMAL_DEFAULT_PRECISION);
            ConstraintChainFilterNode filterNode = new ConstraintChainFilterNode(ratio, result.getCondition(), result.getColumns());
            constraintChain.addNode(filterNode);
            constraintChain.addParameters(result.getParameters());
            lastNodeLineCount = node.getOutputRows();
        } else if (node.getType() == ExecutionNode.ExecutionNodeType.scan) {
            tableName = extractTableName(node.getInfo());
            constraintChain = new ConstraintChain(tableName);
            lastNodeLineCount = node.getOutputRows();
        } else {
            throw new TouchstoneException(String.format("底层节点'%s'不应该为join", node.getId()));
        }
        for (int i = 1; i < path.size(); i++) {
            node = path.get(i);
            try {
                lastNodeLineCount = analyzeNode(node, constraintChain, tableName, lastNodeLineCount);
            } catch (TouchstoneException e) {
                // 小于设置的阈值以后略去后续的节点
                if (node.getOutputRows() * 1.0 / SchemaManager.getInstance().getTableSize(tableName) < skipNodeThreshold) {
                    logger.error("提取约束链失败", e);
                    logger.info(String.format("%s, 但节点行数与tableSize比值小于阈值，跳过节点%s", e.getMessage(), node));
                    break;
                }
                throw e;
            }
            if (lastNodeLineCount < 0) {
                break;
            }
        }
        return constraintChain;
    }

    /**
     * 分析一个节点，提取约束链信息
     *
     * @param node            需要分析的节点
     * @param constraintChain 约束链
     * @param tableName       表名
     * @return 节点行数，小于0代表停止继续向上分析
     * @throws TouchstoneException 节点分析出错
     * @throws SQLException        无法收集多列主键的ndv
     */
    private int analyzeNode(ExecutionNode node, ConstraintChain constraintChain, String tableName, int lastNodeLineCount) throws TouchstoneException, SQLException {
        if (node.getType() == ExecutionNode.ExecutionNodeType.scan) {
            throw new TouchstoneException(String.format("中间节点'%s'不为scan", node.getId()));
        }
        if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
            SelectResult result = analyzeSelectInfo(node.getInfo());
            if (!tableName.equals(result.getTableName())) {
                return -1;
            }
            ConstraintChainFilterNode filterNode = new ConstraintChainFilterNode(BigDecimal.valueOf((double) node.getOutputRows() / lastNodeLineCount), result.getCondition(), result.getColumns());
            lastNodeLineCount = node.getOutputRows();
            constraintChain.addNode(filterNode);
            constraintChain.addParameters(result.getParameters());
        } else if (node.getType() == ExecutionNode.ExecutionNodeType.join) {
            String[] joinColumnInfos = analyzeJoinInfo(node.getInfo());
            String localTable = joinColumnInfos[0], localCol = joinColumnInfos[1],
                    externalTable = joinColumnInfos[2], externalCol = joinColumnInfos[3];
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
                if (node.getJoinTag() < 0) {
                    node.setJoinTag(SchemaManager.getInstance().getJoinTag(localTable));
                }
                ConstraintChainPkJoinNode pkJoinNode = new ConstraintChainPkJoinNode(node.getJoinTag(), localCol.split(","));
                constraintChain.addNode(pkJoinNode);
                //设置主键
                SchemaManager.getInstance().setPrimaryKeys(localTable, localCol);
                return -1; // 主键的情况下停止继续遍历
            } else {
                if (node.getJoinTag() < 0) {
                    node.setJoinTag(SchemaManager.getInstance().getJoinTag(localTable));
                }
                BigDecimal probability = BigDecimal.valueOf((double) node.getOutputRows() / lastNodeLineCount);
                SchemaManager.getInstance().setForeignKeys(localTable, localCol, externalTable, externalCol);
                ConstraintChainFkJoinNode fkJoinNode = new ConstraintChainFkJoinNode(localTable + "." + localCol, externalTable + "." + externalCol, node.getJoinTag(), probability);
                constraintChain.addNode(fkJoinNode);
                lastNodeLineCount = node.getOutputRows();
            }
        }
        return lastNodeLineCount;
    }

    public void setDbConnector(DbConnector dbConnector) {
        this.dbConnector = dbConnector;
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
    public boolean isPrimaryKey(String pkTable, String pkCol, String fkTable, String fkCol) throws TouchstoneException, SQLException {
        if (SchemaManager.getInstance().isRefTable(fkTable, fkCol, pkTable + "." + pkCol)) {
            return true;
        }
        if (SchemaManager.getInstance().isRefTable(pkTable, pkCol, fkTable + "." + fkCol)) {
            return false;
        }
        if (!pkCol.contains(",")) {
            if (ColumnManager.getInstance().getNdv(pkTable + CANONICAL_NAME_CONTACT_SYMBOL + pkCol) ==
                    ColumnManager.getInstance().getNdv(fkTable + CANONICAL_NAME_CONTACT_SYMBOL + fkCol)) {
                return SchemaManager.getInstance().getTableSize(pkTable) < SchemaManager.getInstance().getTableSize(fkTable);
            } else {
                int pkTableNdv = ColumnManager.getInstance().getNdv(pkTable + CANONICAL_NAME_CONTACT_SYMBOL + pkCol);
                int fkTableNdv = ColumnManager.getInstance().getNdv(fkTable + CANONICAL_NAME_CONTACT_SYMBOL + fkCol);
                return pkTableNdv > fkTableNdv;
            }
        } else {
            int leftTableNdv = dbConnector.getMultiColNdv(pkTable, pkCol);
            int rightTableNdv = dbConnector.getMultiColNdv(fkTable, fkCol);
            if (leftTableNdv == rightTableNdv) {
                return SchemaManager.getInstance().getTableSize(pkTable) < SchemaManager.getInstance().getTableSize(fkTable);
            } else {
                return leftTableNdv > rightTableNdv;
            }
        }
    }

    protected String[] convertToDbTableName(String[] result) {
        if (aliasDic.containsKey(result[0])) {
            result[0] = aliasDic.get(result[0]);
        }
        if (aliasDic.containsKey(result[2])) {
            result[2] = aliasDic.get(result[2]);
        }
        return result;
    }

    protected String extractTableName(String operatorInfo) throws IllegalQueryTableNameException {
        String canonicalTableName = extractOriginTableName(operatorInfo);
        if (aliasDic.containsKey(canonicalTableName)) {
            return aliasDic.get(canonicalTableName);
        }
        return addDatabaseNamePrefix(canonicalTableName);
    }

    /**
     * 单个数据库时把表转换为<database>.<table>的形式
     *
     * @param tableName 表名
     * @return 转换后的表名
     */
    public String addDatabaseNamePrefix(String tableName) throws IllegalQueryTableNameException {
        List<List<String>> matches = matchPattern(CANONICAL_TBL_NAME, tableName);
        if (matches.size() == 1 && matches.get(0).get(0).length() == tableName.length()) {
            return tableName;
        } else {
            if (defaultDatabase == null) {
                throw new IllegalQueryTableNameException();
            }
            return String.format("%s.%s", defaultDatabase, tableName);
        }
    }
}
