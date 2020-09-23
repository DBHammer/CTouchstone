package ecnu.db.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.exception.InstantiateParameterException;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.StringColumn;
import org.apache.commons.collections.CollectionUtils;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ecnu.db.constraintchain.filter.operation.CompareOperator.*;
import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.GREATER;
import static ecnu.db.constraintchain.filter.operation.CompareOperator.TYPE.LESS;

/**
 * @author wangqingshuai
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UniVarFilterOperation extends AbstractFilterOperation {
    protected String columnName;
    private Boolean hasNot = false;

    public UniVarFilterOperation() {
        super(null);
    }

    public UniVarFilterOperation(String columnName, CompareOperator operator) {
        super(operator);
        this.columnName = columnName;
    }

    /**
     * merge operation
     */
    public static void merge(List<BoolExprNode> toMergeNodes, Multimap<String, UniVarFilterOperation> col2uniFilters, boolean isAnd) {
        for (String colName : col2uniFilters.keySet()) {
            Collection<UniVarFilterOperation> filters = col2uniFilters.get(colName);
            Multimap<CompareOperator, UniVarFilterOperation> typ2Filter = Multimaps.index(filters, AbstractFilterOperation::getOperator);
            Set<CompareOperator.TYPE> types = typ2Filter.asMap().keySet().stream().map(CompareOperator::getType).collect(Collectors.toSet());
            if (typ2Filter.size() == 1) {
                toMergeNodes.add((BoolExprNode) CollectionUtils.get(typ2Filter.values(), 0));
                continue;
            }
            if (types.contains(GREATER) && types.contains(LESS)) {
                RangeFilterOperation newFilter = new RangeFilterOperation(colName);
                newFilter.addLessParameters(Stream.concat(typ2Filter.get(CompareOperator.LE).stream(), typ2Filter.get(CompareOperator.LT).stream())
                        .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
                newFilter.addGreaterParameters(Stream.concat(typ2Filter.get(CompareOperator.GE).stream(), typ2Filter.get(CompareOperator.GT).stream())
                        .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
                setLessOperator(isAnd, typ2Filter, newFilter);
                setGreaterOperator(isAnd, typ2Filter, newFilter);
                toMergeNodes.add(newFilter);
            } else if (types.contains(LESS) && !types.contains(GREATER)) {
                RangeFilterOperation newFilter = new RangeFilterOperation(colName);
                newFilter.addLessParameters(Stream.concat(typ2Filter.get(CompareOperator.LE).stream(), typ2Filter.get(CompareOperator.LT).stream())
                        .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
                setGreaterOperator(isAnd, typ2Filter, newFilter);
                toMergeNodes.add(newFilter);
            } else if (types.contains(GREATER) && !types.contains(LESS)) {
                RangeFilterOperation newFilter = new RangeFilterOperation(colName);
                newFilter.addGreaterParameters(Stream.concat(typ2Filter.get(CompareOperator.GE).stream(), typ2Filter.get(CompareOperator.GT).stream())
                        .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
                setLessOperator(isAnd, typ2Filter, newFilter);
                toMergeNodes.add(newFilter);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    private static void setLessOperator(boolean isAnd, Multimap<CompareOperator, UniVarFilterOperation> typ2Filter, RangeFilterOperation newFilter) {
        if (isAnd) {
            newFilter.setGreaterOperator(typ2Filter.containsKey(CompareOperator.GT) ? CompareOperator.GT : CompareOperator.GE);
        } else {
            newFilter.setGreaterOperator(typ2Filter.containsKey(CompareOperator.GE) ? CompareOperator.GE : CompareOperator.GT);
        }
    }

    private static void setGreaterOperator(boolean isAnd, Multimap<CompareOperator, UniVarFilterOperation> typ2Filter, RangeFilterOperation newFilter) {
        if (isAnd) {
            newFilter.setLessOperator(typ2Filter.containsKey(CompareOperator.LT) ? CompareOperator.LT : CompareOperator.LE);
        } else {
            newFilter.setLessOperator(typ2Filter.containsKey(CompareOperator.LE) ? CompareOperator.LE : CompareOperator.LT);
        }
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
    }

    public Boolean getHasNot() {
        return hasNot;
    }

    public void setHasNot(Boolean hasNot) {
        this.hasNot = hasNot;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String toString() {
        List<String> content = parameters.stream().map(Parameter::toString).collect(Collectors.toList());
        content.add(0, String.format("%s", columnName));
        if (hasNot) {
            return String.format("not(%s(%s))", operator.toString().toLowerCase(), String.join(", ", content));
        }
        return String.format("%s(%s)", operator.toString().toLowerCase(), String.join(", ", content));
    }

    /**
     * 初始化lt,gt,le,ge等比较的filter
     *
     * @param absColumn 需要用到的column
     */
    public void instantiateUniParamCompParameter(AbstractColumn absColumn) {
        instantiateUniParamCompParameter(absColumn, operator, parameters);
    }

    /**
     * {@link #instantiateUniParamCompParameter(AbstractColumn) instantiateUniParamCompParameter(AbstractColumn)}的内部方法，与
     * {@linkplain ecnu.db.constraintchain.filter.operation.RangeFilterOperation#instantiateBetweenParameter(AbstractColumn)
     * ecnu.db.constraintchain.filter.operation.RangeFilterOperation#instantiateBetweenParameter(AbstractColumn)}共享逻辑
     *
     * @param column     需要的column
     * @param operator   涉及的操作符
     * @param parameters 涉及的参数
     */
    protected void instantiateUniParamCompParameter(AbstractColumn column, CompareOperator operator, List<Parameter> parameters) {
        if (operator.getType() != LESS && operator.getType() != GREATER) {
            throw new UnsupportedOperationException();
        }
        probability = operator.getType() == LESS ? probability : BigDecimal.ONE.subtract(probability);
        // todo currently we are regarding (lt, le) as the lt, same goes for (gt, ge), see <a href="https://youtrack.biui.me/issue/TOUCHSTONE-18">TOUCHSTONE-18</a>
        operator = LT;
        String data = column.genData(probability);
        parameters.forEach((param) -> param.setData(data));
        if (column.hasNotMetCondition(operator + data)) { // for uni compare we use operator and generated value as identifier
            column.insertNonEqProbability(probability, operator, parameters.get(0));
        }
    }

    /**
     * 初始化等值filter的参数
     *
     * @param column 涉及的column
     */
    public void instantiateEqualParameter(AbstractColumn column) throws InstantiateParameterException {
        if (hasNot) {
            probability = BigDecimal.valueOf(1 - column.getNullPercentage()).subtract(probability);
        }
        if (operator == EQ) {
            column.insertEqualProbability(probability, parameters.get(0));
        } else if (operator == NE) {
            column.insertEqualProbability(BigDecimal.valueOf(1 - column.getNullPercentage()).subtract(probability), parameters.get(0));
        } else if (operator == IN) {
            column.insertInProbability(probability, parameters);
        } else if (operator == LIKE) {
            String value = ((StringColumn) column).generateLikeData(parameters.get(0).getData());
            parameters.get(0).setData(value);
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
