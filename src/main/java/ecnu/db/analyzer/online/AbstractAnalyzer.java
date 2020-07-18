package ecnu.db.analyzer.online;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import ecnu.db.analyzer.online.node.ExecutionNode;
import ecnu.db.analyzer.online.node.NodeTypeRefFactory;
import ecnu.db.analyzer.online.node.NodeTypeTool;
import ecnu.db.analyzer.statical.QueryAliasParser;
import ecnu.db.dbconnector.DatabaseConnectorInterface;
import ecnu.db.schema.Schema;
import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneToolChainException;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author wangqingshuai
 */
public abstract class AbstractAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(AbstractAnalyzer.class);

    protected DatabaseConnectorInterface dbConnector;
    protected Map<String, String> aliasDic = new HashMap<>();
    protected QueryAliasParser queryAliasParser = new QueryAliasParser();
    protected Map<String, Schema> schemas;
    protected int sqlArgIndex = 0;
    protected int lastArgIndex = 0;
    protected Map<String, List<String>> argsAndIndex = new HashMap<>();
    protected NodeTypeTool nodeTypeRef;
    protected SystemConfig config;
    protected Multimap<String, String> tblName2CanonicalTblName;

    AbstractAnalyzer(SystemConfig config, 
                     DatabaseConnectorInterface dbConnector, 
                     Map<String, Schema> schemas, 
                     Multimap<String, String> tblName2CanonicalTblName) {
        this.nodeTypeRef = NodeTypeRefFactory.getNodeTypeRef(config.getDatabaseVersion());
        this.dbConnector = dbConnector;
        this.schemas = schemas;
        this.config = config;
        this.tblName2CanonicalTblName = tblName2CanonicalTblName;
    }

    /**
     * sql的查询计划中，需要使用查询计划的列名
     *
     * @param databaseVersion 数据库类型
     * @return
     * @throws TouchstoneToolChainException
     */
    abstract String[] getSqlInfoColumns(String databaseVersion) throws TouchstoneToolChainException;

    /**
     * 获取数据库使用的静态解析器的数据类型
     *
     * @return 静态解析器使用的数据库类型
     */
    public abstract String getDbType();

    /**
     * 从operator_info里提取tableName
     *
     * @param operatorInfo 需要处理的operator_info
     * @return 提取的表名
     */
    abstract String extractTableName(String operatorInfo);

    /**
     * 查询树的解析
     *
     * @param queryPlan query解析出的查询计划，带具体的行数
     * @return 查询树Node信息
     * @throws TouchstoneToolChainException 查询树无法解析
     */
    public abstract ExecutionNode getExecutionTree(List<String[]> queryPlan) throws TouchstoneToolChainException;

    /**
     * 分析join信息
     *
     * @param joinInfo join字符串
     * @return 长度为4的字符串数组，0，1为join info左侧的表名和列名，2，3为join右侧的表明和列名
     * @throws TouchstoneToolChainException 无法分析的join条件
     */
    abstract String[] analyzeJoinInfo(String joinInfo) throws TouchstoneToolChainException;

    /**
     * 创建colName到对应的oprator的multimap，并返回关于where_condition的信息
     *
     * @param operatorInfo 需要处理的operator_info
     * @param conditions   用于创建的multimap
     * @return 关于whereExpr的信息
     * @throws TouchstoneToolChainException 无法分析的部分
     */
    abstract WhereConditionInfo buildConditionMap(String operatorInfo, Multimap<String, String> conditions) throws TouchstoneToolChainException;

    /**
     * 分析传入的select 过滤条件，传出表名和格式化后的condition
     *
     * @param operatorInfo 传入的select的operatorInfo
     * @return 表名和格式化后的condition
     */
    Pair<String, String> analyzeSelectCondition(String operatorInfo) throws TouchstoneToolChainException {
        Multimap<String, String> conditionMap = ArrayListMultimap.create();
        WhereConditionInfo ret = buildConditionMap(operatorInfo, conditionMap);
        boolean useAlias = ret.useAlias, isOr = ret.isOr;
        String tableName = ret.tableName;
        if (aliasDic.containsKey(tableName)) {
            tableName = aliasDic.get(tableName);
        }
        StringBuilder conditionStr = new StringBuilder();
        for (Map.Entry<String, String> entry : conditionMap.entries()) {
            String columnName = entry.getKey();
            String operator = entry.getValue();
            conditionStr.append(String.format("%s@%s#", columnName, operator));
            String selectArgName = columnName + " " + operator;
            if (useAlias) {
                selectArgName = tableName + "." + selectArgName;
            }
            StringBuilder selectArgs = new StringBuilder();

            if (operator.contains("in")) {
                int dateOrNot = schemas.get(tableName).isDate(columnName) ? 1 : 0;
                int inCount = Integer.parseInt(operator.split("[()]")[1]);
                selectArgs = new StringBuilder(String.format("(%s)",
                        IntStream.range(0, inCount)
                                .mapToObj((i) -> String.format("'#%d,%d,%d#'", sqlArgIndex, i, dateOrNot))
                                .collect(Collectors.joining(", "))));
                sqlArgIndex++;
            } else {
                if ("bet".equals(operator)) {
                    selectArgs.append(String.format("ween '#%d,0,%d#' and '#%d,1,%d#'",
                            sqlArgIndex,
                            schemas.get(tableName).isDate(columnName) ? 1 : 0,
                            sqlArgIndex++,
                            schemas.get(tableName).isDate(columnName) ? 1 : 0));
                } else {
                    selectArgs.append(String.format("#%d,0,%d#", sqlArgIndex++, schemas.get(tableName).isDate(columnName) ? 1 : 0));
                }
            }
            if (argsAndIndex.containsKey(selectArgName)) {
                argsAndIndex.get(selectArgName).add(selectArgs.toString());
            } else {
                List<String> value = new ArrayList<>();
                value.add(selectArgs.toString());
                argsAndIndex.put(selectArgName, value);
            }
        }
        if (!isOr && conditionMap.size() > 1) {
            conditionStr.append("and");
        } else if (isOr && conditionMap.size() > 1) {
            conditionStr.append("or");
        }


        return new MutablePair<>(tableName, conditionStr.toString());
    }

    public List<String[]> getQueryPlan(String queryCanonicalName, String sql) throws SQLException, TouchstoneToolChainException {
        aliasDic = queryAliasParser.getTableAlias(config.isCrossMultiDatabase(), config.getDatabaseName(), sql, getDbType());
        return dbConnector.explainQuery(queryCanonicalName, sql, getSqlInfoColumns(config.getDatabaseVersion()));
    }

    /**
     * 获取查询树的约束链信息和表信息
     *
     * @param queryCanonicalName query的标准名称
     * @param root 查询树
     * @return 该查询树结构出的约束链信息和表信息
     */
    public List<String> extractQueryInfos(String queryCanonicalName, ExecutionNode root) throws SQLException {

        List<String> queryInfos = new ArrayList<>();
        List<List<ExecutionNode>> paths = getPaths(root);
        for (List<ExecutionNode> path : paths) {
            QueryInfo queryInfo = null;
            try {
                queryInfo = extractConstraintChain(path);
            } catch (TouchstoneToolChainException e) {
                logger.error(String.format("提取'%s'的一个约束链失败", queryCanonicalName), e);
            }
            if (queryInfo == null) {
                break;
            }
            if (!queryInfo.getData().isBlank()) {
                queryInfos.add(String.format("[%s];%s", queryInfo.getTableName(), queryInfo.getData()));
            }
        }
        return queryInfos;
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
        if (root.leftNode != null) {
            getPathsIterate(root.leftNode, paths);
            for (List<ExecutionNode> path : paths) {
                path.add(root);
            }
        }
        if (root.rightNode != null) {
            getPathsIterate(root.rightNode, paths);
            for (List<ExecutionNode> path : paths) {
                path.add(root);
            }
        }
        if (root.leftNode == null && root.rightNode == null) {
            List<ExecutionNode> newPath = new ArrayList<>(Collections.singletonList(root));
            paths.add(newPath);
        }
    }

    /**
     * 获取一条路径上的约束链
     *
     * @param path 需要处理的路径
     * @return 获取的约束链
     * @throws TouchstoneToolChainException 无法处理路径
     * @throws SQLException                 无法处理路径
     */
    private QueryInfo extractConstraintChain(List<ExecutionNode> path) throws TouchstoneToolChainException, SQLException {
        if (path == null || path.size() == 0) {
            throw new TouchstoneToolChainException(String.format("非法的path输入 '%s'", path));
        }
        ExecutionNode node = path.get(0);
        QueryInfo constraintChain;
        String tableName;
        if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
            Pair<String, String> tableNameAndSelectCondition = analyzeSelectCondition(node.getInfo());
            tableName = tableNameAndSelectCondition.getLeft();
            String selectInfo = "[0," + tableNameAndSelectCondition.getRight() + "," +
                    (double) node.getOutputRows() / schemas.get(tableName).getTableSize() + "];";
            constraintChain = new QueryInfo(selectInfo, tableName, node.getOutputRows());
        } else if (node.getType() == ExecutionNode.ExecutionNodeType.scan) {
            tableName = extractTableName(node.getInfo());
            Schema schema = schemas.get(tableName);
            constraintChain = new QueryInfo("", tableName, schema.getTableSize());
        } else {
            throw new TouchstoneToolChainException(String.format("底层节点'%s'不应该为join", node.getId()));
        }
        for (int i = 1; i < path.size(); i++) {
            node = path.get(i);
            boolean isStop;
            try {
                isStop = analyzeNode(node, constraintChain, tableName);
            } catch (TouchstoneToolChainException | SQLException e) {
                // 小于设置的阈值以后略去后续的节点
                if (node.getOutputRows() * 1.0 / schemas.get(tableName).getTableSize() < config.getSkipNodeThreshold()) {
                    logger.error("提取约束链失败", e);
                    logger.info(String.format("%s, 但节点行数与tableSize比值小于阈值，跳过节点%s", e.getMessage(), node));
                    break;
                }
                throw e;
            }
            if (isStop) {
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
     * @return 是否停止继续向上分析
     * @throws TouchstoneToolChainException 节点分析出错
     * @throws SQLException                 节点分析出错
     */
    private boolean analyzeNode(ExecutionNode node, QueryInfo constraintChain, String tableName) throws TouchstoneToolChainException, SQLException {
        if (node.getType() == ExecutionNode.ExecutionNodeType.scan) {
            throw new TouchstoneToolChainException(String.format("中间节点'%s'不为scan", node.getId()));
        }
        if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
            Pair<String, String> tableNameAndSelectCondition = analyzeSelectCondition(node.getInfo());
            if (tableName.equals(tableNameAndSelectCondition.getKey())) {
                constraintChain.addConstraint("[0," + tableNameAndSelectCondition.getRight() + "," +
                        (double) node.getOutputRows() / constraintChain.getLastNodeLineCount() + "];");
                constraintChain.setLastNodeLineCount(node.getOutputRows());
            }
        } else if (node.getType() == ExecutionNode.ExecutionNodeType.join) {
            String[] joinColumnInfos = analyzeJoinInfo(node.getInfo());
            String pkTable = joinColumnInfos[0], pkCol = joinColumnInfos[1],
                    fkTable = joinColumnInfos[2], fkCol = joinColumnInfos[3];
            // 如果当前的join节点，不属于之前遍历的节点，则停止继续向上访问
            if (!pkTable.equals(constraintChain.getTableName())
                    && !fkTable.equals(constraintChain.getTableName())) {
                return true;
            }
            //将本表的信息放在前面，交换位置
            if (constraintChain.getTableName().equals(fkTable)) {
                pkTable = joinColumnInfos[2];
                pkCol = joinColumnInfos[3];
                fkTable = joinColumnInfos[0];
                fkCol = joinColumnInfos[1];
            }
            //根据主外键分别设置约束链输出信息
            if (isPrimaryKey(pkTable, pkCol, fkTable, fkCol)) {
                if (node.getJoinTag() < 0) {
                    node.setJoinTag(schemas.get(pkTable).getJoinTag());
                }
                constraintChain.addConstraint("[1," + pkCol.replace(',', '#') + "," +
                        node.getJoinTag() + "," + 2 * node.getJoinTag() + "];");
                //设置主键
                schemas.get(pkTable).setPrimaryKeys(pkCol);
                constraintChain.setLastNodeLineCount(node.getOutputRows());
                return true;
            } else {
                if (node.getJoinTag() < 0) {
                    node.setJoinTag(schemas.get(pkTable).getJoinTag());
                }
                String primaryKey = fkTable + "." + fkCol;
                constraintChain.addConstraint("[2," + fkCol.replace(',', '#') + "," +
                        (double) node.getOutputRows() / constraintChain.getLastNodeLineCount() + "," +
                        primaryKey.replace(',', '#') + "," +
                        node.getJoinTag() + "," + 2 * node.getJoinTag() + "];");
                //设置外键
                logger.info("table:" + pkTable + ".column:" + pkCol + " -ref- table:" +
                        fkCol + ".column:" + fkTable);
                schemas.get(pkTable).addForeignKey(pkCol, fkTable, fkCol);
                constraintChain.setLastNodeLineCount(node.getOutputRows());
            }
        }
        return false;
    }

    /**
     * 根据输入的列名统计非重复值的个数，进而给出该列是否为主键
     *
     * @param pkTable
     * @param pkCol
     * @param fkTable
     * @param fkCol
     * @return 该列是否为主键
     * @throws TouchstoneToolChainException
     * @throws SQLException
     */
    private boolean isPrimaryKey(String pkTable, String pkCol, String fkTable, String fkCol) throws TouchstoneToolChainException, SQLException {
        if (String.format("%s.%s", pkTable, pkCol).equals(schemas.get(fkTable).getMetaDataFks().get(fkCol))) {
            return true;
        }
        if (String.format("%s.%s", fkTable, fkCol).equals(schemas.get(pkTable).getMetaDataFks().get(pkCol))) {
            return false;
        }
        if (!pkCol.contains(",")) {
            if (schemas.get(pkTable).getNdv(pkCol) == schemas.get(fkTable).getNdv(fkCol)) {
                return schemas.get(pkTable).getTableSize() < schemas.get(fkTable).getTableSize();
            } else {
                return schemas.get(pkTable).getNdv(pkCol) > schemas.get(fkTable).getNdv(fkCol);
            }
        } else {
            int leftTableNdv = dbConnector.getMultiColNdv(pkTable, pkCol);
            int rightTableNdv = dbConnector.getMultiColNdv(fkTable, fkCol);
            if (leftTableNdv == rightTableNdv) {
                return schemas.get(pkTable).getTableSize() < schemas.get(fkTable).getTableSize();
            } else {
                return leftTableNdv > rightTableNdv;
            }
        }
    }

    public Map<String, List<String>> getArgsAndIndex() {
        return argsAndIndex;
    }

    public void outputSuccess(boolean success) {
        if (success) {
            lastArgIndex = sqlArgIndex;
        } else {
            sqlArgIndex = lastArgIndex;
        }
        for (Schema schema : schemas.values()) {
            schema.keepJoinTag(success);
        }
    }

}
