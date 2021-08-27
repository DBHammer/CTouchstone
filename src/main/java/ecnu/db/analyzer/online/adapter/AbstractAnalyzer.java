package ecnu.db.analyzer.online.adapter;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import ecnu.db.analyzer.online.ExecutionNode;
import ecnu.db.analyzer.online.NodeTypeTool;
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

    protected NodeTypeTool nodeTypeRef;

    /**
     * 从operator_info里提取tableName
     *
     * @param operatorInfo 需要处理的operator_info
     * @return 提取的表名
     */
    public abstract String extractOriginTableName(String operatorInfo) throws IllegalQueryTableNameException;

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
    public abstract String[] analyzeJoinInfo(String joinInfo) throws TouchstoneException;

    /**
     * 分析join信息
     *
     * @param operatorInfo 查询的条件
     * @return SelectResult 算数抽象语法树
     * @throws Exception 无法分析的Selection条件
     */
    public abstract SelectResult analyzeSelectOperator(String operatorInfo) throws Exception;
}
