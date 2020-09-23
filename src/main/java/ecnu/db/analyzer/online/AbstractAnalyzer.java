package ecnu.db.analyzer.online;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import ecnu.db.analyzer.statical.QueryAliasParser;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainFilterNode;
import ecnu.db.constraintchain.chain.ConstraintChainFkJoinNode;
import ecnu.db.constraintchain.chain.ConstraintChainPkJoinNode;
import ecnu.db.constraintchain.filter.SelectResult;
import ecnu.db.dbconnector.DatabaseConnectorInterface;
import ecnu.db.exception.CannotFindSchemaException;
import ecnu.db.exception.TouchstoneToolChainException;
import ecnu.db.exception.UnsupportedDBTypeException;
import ecnu.db.schema.Schema;
import ecnu.db.utils.AbstractDatabaseInfo;
import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneSupportedDatabaseVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ecnu.db.utils.CommonUtils.BIG_DECIMAL_DEFAULT_PRECISION;

/**
 * @author wangqingshuai
 */
public abstract class AbstractAnalyzer {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractAnalyzer.class);
    protected final QueryAliasParser queryAliasParser = new QueryAliasParser();
    protected DatabaseConnectorInterface dbConnector;
    protected Map<String, String> aliasDic = new HashMap<>();
    protected Map<String, Schema> schemas;
    protected int parameterId = 0;
    protected NodeTypeTool nodeTypeRef;
    protected SystemConfig config;
    protected Multimap<String, String> tblName2CanonicalTblName;
    protected TouchstoneSupportedDatabaseVersion analyzerSupportedDatabaseVersion;
    protected AbstractDatabaseInfo databaseInfo;

    protected AbstractAnalyzer(SystemConfig config, DatabaseConnectorInterface dbConnector,
                               AbstractDatabaseInfo databaseInfo, Map<String, Schema> schemas,
                               Multimap<String, String> tblName2CanonicalTblName) {
        analyzerSupportedDatabaseVersion = config.getDatabaseVersion();
        this.dbConnector = dbConnector;
        this.schemas = schemas;
        this.config = config;
        this.tblName2CanonicalTblName = tblName2CanonicalTblName;
        this.databaseInfo = databaseInfo;
    }

    public AbstractAnalyzer check() throws TouchstoneToolChainException {
        if (databaseInfo == null || databaseInfo.getSupportedDatabaseVersions() == null || databaseInfo.getSupportedDatabaseVersions().size() == 0) {
            throw new TouchstoneToolChainException("未制定分析器配置信息");
        } else if (!databaseInfo.getSupportedDatabaseVersions().contains(analyzerSupportedDatabaseVersion)) {
            throw new UnsupportedDBTypeException(config.getDatabaseVersion());
        } else if (nodeTypeRef == null) {
            throw new TouchstoneToolChainException("未初始化node映射");
        } else {
            return this;
        }
    }


    /**
     * 从operator_info里提取tableName
     *
     * @param operatorInfo 需要处理的operator_info
     * @return 提取的表名
     */
    protected abstract String extractTableName(String operatorInfo);

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
    protected abstract String[] analyzeJoinInfo(String joinInfo) throws TouchstoneToolChainException;

    /**
     * 分析select信息
     *
     * @param operatorInfo 需要分析的operator_info
     * @return SelectResult
     * @throws TouchstoneToolChainException 分析失败
     */
    protected abstract SelectResult analyzeSelectInfo(String operatorInfo) throws TouchstoneToolChainException;

    public List<String[]> getQueryPlan(String queryCanonicalName, String sql, AbstractDatabaseInfo databaseInfo) throws SQLException, TouchstoneToolChainException {
        aliasDic = queryAliasParser.getTableAlias(config.isCrossMultiDatabase(), config.getDatabaseName(), sql, databaseInfo.getStaticalDbVersion());
        return dbConnector.explainQuery(queryCanonicalName, sql, databaseInfo.getSqlInfoColumns());
    }

    /**
     * 获取查询树的约束链信息和表信息
     *
     * @param queryCanonicalName query的标准名称
     * @param root               查询树
     * @return 该查询树结构出的约束链信息和表信息
     */
    public List<ConstraintChain> extractQueryInfos(String queryCanonicalName, ExecutionNode root) throws SQLException {
        List<ConstraintChain> constraintChains = new ArrayList<>();
        List<List<ExecutionNode>> paths = getPaths(root);
        for (List<ExecutionNode> path : paths) {
            ConstraintChain constraintChain = null;
            try {
                constraintChain = extractConstraintChain(path);
            } catch (TouchstoneToolChainException e) {
                logger.error(String.format("提取'%s'的一个约束链失败", queryCanonicalName), e);
            }
            if (constraintChain == null) {
                break;
            }
            constraintChains.add(constraintChain);
        }
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
            List<ExecutionNode> newPath = Lists.newArrayList(root);
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
    private ConstraintChain extractConstraintChain(List<ExecutionNode> path) throws TouchstoneToolChainException, SQLException {
        if (path == null || path.size() == 0) {
            throw new TouchstoneToolChainException(String.format("非法的path输入 '%s'", path));
        }
        ExecutionNode node = path.get(0);
        ConstraintChain constraintChain;
        String tableName;
        int lastNodeLineCount;
        if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
            SelectResult result = analyzeSelectInfo(node.getInfo());
            tableName = result.getTableName();
            constraintChain = new ConstraintChain(tableName);
            BigDecimal ratio = BigDecimal.valueOf(node.getOutputRows()).divide(BigDecimal.valueOf(getSchema(tableName).getTableSize()), BIG_DECIMAL_DEFAULT_PRECISION);
            ConstraintChainFilterNode filterNode = new ConstraintChainFilterNode(tableName, ratio, result.getCondition(), result.getColumns());
            constraintChain.addNode(filterNode);
            constraintChain.addParameters(result.getParameters());
            lastNodeLineCount = node.getOutputRows();
        } else if (node.getType() == ExecutionNode.ExecutionNodeType.scan) {
            tableName = extractTableName(node.getInfo());
            constraintChain = new ConstraintChain(tableName);
            lastNodeLineCount = node.getOutputRows();
        } else {
            throw new TouchstoneToolChainException(String.format("底层节点'%s'不应该为join", node.getId()));
        }
        for (int i = 1; i < path.size(); i++) {
            node = path.get(i);
            try {
                lastNodeLineCount = analyzeNode(node, constraintChain, tableName, lastNodeLineCount);
            } catch (TouchstoneToolChainException e) {
                // 小于设置的阈值以后略去后续的节点
                if (node.getOutputRows() * 1.0 / getSchema(tableName).getTableSize() < config.getSkipNodeThreshold()) {
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
     * @throws TouchstoneToolChainException 节点分析出错
     * @throws SQLException                 节点分析出错
     */
    private int analyzeNode(ExecutionNode node, ConstraintChain constraintChain, String tableName, int lastNodeLineCount) throws TouchstoneToolChainException, SQLException {
        if (node.getType() == ExecutionNode.ExecutionNodeType.scan) {
            throw new TouchstoneToolChainException(String.format("中间节点'%s'不为scan", node.getId()));
        }
        if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
            SelectResult result = analyzeSelectInfo(node.getInfo());
            if (!tableName.equals(result.getTableName())) {
                throw new TouchstoneToolChainException("select表名不匹配");
            }
            ConstraintChainFilterNode filterNode = new ConstraintChainFilterNode(tableName, BigDecimal.valueOf((double) node.getOutputRows() / lastNodeLineCount), result.getCondition(), result.getColumns());
            lastNodeLineCount = node.getOutputRows();
            constraintChain.addNode(filterNode);
            constraintChain.addParameters(result.getParameters());
        } else if (node.getType() == ExecutionNode.ExecutionNodeType.join) {
            String[] joinColumnInfos = analyzeJoinInfo(node.getInfo());
            String pkTable = joinColumnInfos[0], pkCol = joinColumnInfos[1],
                    fkTable = joinColumnInfos[2], fkCol = joinColumnInfos[3];
            // 如果当前的join节点，不属于之前遍历的节点，则停止继续向上访问
            if (!pkTable.equals(constraintChain.getTableName())
                    && !fkTable.equals(constraintChain.getTableName())) {
                return -1;
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
                    node.setJoinTag(getSchema(pkTable).getJoinTag());
                }
                ConstraintChainPkJoinNode pkJoinNode = new ConstraintChainPkJoinNode(pkTable, node.getJoinTag(), pkCol.split(","));
                constraintChain.addNode(pkJoinNode);
                //设置主键
                getSchema(pkTable).setPrimaryKeys(pkCol);
                return node.getOutputRows();
            } else {
                if (node.getJoinTag() < 0) {
                    node.setJoinTag(getSchema(pkTable).getJoinTag());
                }
                BigDecimal probability = BigDecimal.valueOf((double) node.getOutputRows() / lastNodeLineCount);
                //设置外键
                logger.info("table:" + pkTable + ".column:" + pkCol + " -ref- table:" +
                        fkCol + ".column:" + fkTable);
                getSchema(pkTable).addForeignKey(pkCol, fkTable, fkCol);
                ConstraintChainFkJoinNode fkJoinNode = new ConstraintChainFkJoinNode(pkTable, fkTable, node.getJoinTag(), getSchema(pkTable).getForeignKeys(), probability);
                constraintChain.addNode(fkJoinNode);
            }
        }
        return lastNodeLineCount;
    }

    /**
     * 根据输入的列名统计非重复值的个数，进而给出该列是否为主键
     *
     * @param pkTable 需要测试的主表
     * @param pkCol   主键
     * @param fkTable 外表
     * @param fkCol   外键
     * @return 该列是否为主键
     * @throws TouchstoneToolChainException 由于逻辑错误无法判断是否为主键的异常
     * @throws SQLException                 无法通过数据库SQL查询获得多列属性的ndv
     */
    private boolean isPrimaryKey(String pkTable, String pkCol, String fkTable, String fkCol) throws TouchstoneToolChainException, SQLException {
        if (String.format("%s.%s", pkTable, pkCol).equals(getSchema(fkTable).getMetaDataFks().get(fkCol))) {
            return true;
        }
        if (String.format("%s.%s", fkTable, fkCol).equals(getSchema(pkTable).getMetaDataFks().get(pkCol))) {
            return false;
        }
        if (!pkCol.contains(",")) {
            if (getSchema(pkTable).getNdv(pkCol) == getSchema(fkTable).getNdv(fkCol)) {
                return getSchema(pkTable).getTableSize() < getSchema(fkTable).getTableSize();
            } else {
                return getSchema(pkTable).getNdv(pkCol) > getSchema(fkTable).getNdv(fkCol);
            }
        } else {
            int leftTableNdv = dbConnector.getMultiColNdv(pkTable, pkCol);
            int rightTableNdv = dbConnector.getMultiColNdv(fkTable, fkCol);
            if (leftTableNdv == rightTableNdv) {
                return getSchema(pkTable).getTableSize() < getSchema(fkTable).getTableSize();
            } else {
                return leftTableNdv > rightTableNdv;
            }
        }
    }

    public int getParameterId() {
        return parameterId++;
    }

    public Schema getSchema(String tableName) throws CannotFindSchemaException {
        Schema schema = schemas.get(tableName);
        if (schema == null) {
            throw new CannotFindSchemaException(tableName);
        }
        return schema;
    }

}
