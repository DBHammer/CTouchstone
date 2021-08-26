package ecnu.db.generator.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import ecnu.db.generator.constraintchain.filter.BoolExprNode;
import ecnu.db.generator.constraintchain.filter.BoolExprType;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.schema.ColumnManager;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author wangqingshuai
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UniVarFilterOperation extends AbstractFilterOperation {
    protected String canonicalColumnName;
    private Boolean hasNot = false;

    public UniVarFilterOperation() {
        super(null);
    }

    public UniVarFilterOperation(String canonicalColumnName, CompareOperator operator) {
        super(operator);
        this.canonicalColumnName = canonicalColumnName;
    }

    /**
     * merge operation
     */
    public static void merge(List<BoolExprNode> toMergeNodes, Multimap<String, UniVarFilterOperation> col2uniFilters, boolean isAnd) {
        for (String colName : col2uniFilters.keySet()) {
            Collection<UniVarFilterOperation> filters = col2uniFilters.get(colName);
            Multimap<CompareOperator, UniVarFilterOperation> typ2Filter = Multimaps.index(filters, AbstractFilterOperation::getOperator);
            if (typ2Filter.size() == 1) {
                toMergeNodes.addAll(typ2Filter.values());
                continue;
            }
            RangeFilterOperation newFilter = new RangeFilterOperation(colName);
            newFilter.addLessParameters(Stream.concat(typ2Filter.get(CompareOperator.LE).stream(), typ2Filter.get(CompareOperator.LT).stream())
                    .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
            newFilter.addGreaterParameters(Stream.concat(typ2Filter.get(CompareOperator.GE).stream(), typ2Filter.get(CompareOperator.GT).stream())
                    .flatMap((filter) -> filter.getParameters().stream()).collect(Collectors.toList()));
            newFilter.setLessOperator(isAnd, typ2Filter);
            newFilter.setGreaterOperator(isAnd, typ2Filter);
            toMergeNodes.add(newFilter);
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

    public String getCanonicalColumnName() {
        return canonicalColumnName;
    }

    public void setCanonicalColumnName(String canonicalColumnName) {
        this.canonicalColumnName = canonicalColumnName;
    }

    @Override
    public String toString() {
        List<String> content = parameters.stream().map(Parameter::toString).collect(Collectors.toList());
        content.add(0, String.format("%s", canonicalColumnName));
        if (hasNot) {
            return String.format("not(%s(%s))", operator.toString().toLowerCase(), String.join(", ", content));
        }
        return String.format("%s(%s)", operator.toString().toLowerCase(), String.join(", ", content));
    }

    /**
     * 初始化等值filter的参数
     */
    public void instantiateParameter() {
        if (hasNot) {
            probability = BigDecimal.ONE.subtract(probability);
        }
        ColumnManager.getInstance().insertUniVarProbability(canonicalColumnName, probability, operator, parameters);
    }

    @Override
    public boolean[] evaluate() {
        return ColumnManager.getInstance().evaluate(canonicalColumnName, operator, parameters, hasNot);
    }
}
