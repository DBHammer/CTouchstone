package ecnu.db.generator.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.generator.constraintchain.filter.BoolExprNode;
import ecnu.db.generator.constraintchain.filter.BoolExprType;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.schema.ColumnManager;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @author wangqingshuai
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UniVarFilterOperation extends AbstractFilterOperation {
    protected String canonicalColumnName;
    private boolean hasNot = false;

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
    public static void merge(List<BoolExprNode> toMergeNodes, Map<String, Collection<UniVarFilterOperation>> col2uniFilters, boolean isAnd) {
        for (var col2uniFilter : col2uniFilters.entrySet()) {
            Collection<UniVarFilterOperation> filters = col2uniFilter.getValue();
            Map<CompareOperator, List<UniVarFilterOperation>> typ2Filter = filters.stream()
                    .map(filter -> new AbstractMap.SimpleEntry<>(filter.getOperator(), filter))
                    .collect(Collectors.groupingBy(AbstractMap.SimpleEntry::getKey,
                            Collectors.mapping(AbstractMap.SimpleEntry::getValue, Collectors.toList())));
            if (typ2Filter.size() == 1) {
                toMergeNodes.addAll(typ2Filter.values().stream().flatMap(Collection::stream).toList());
                continue;
            }
            RangeFilterOperation newFilter = new RangeFilterOperation(col2uniFilter.getKey());
            newFilter.addLessParameters(Stream.concat(typ2Filter.getOrDefault(CompareOperator.LE, new ArrayList<>()).stream(),
                            typ2Filter.getOrDefault(CompareOperator.LT, new ArrayList<>()).stream())
                    .flatMap((filter) -> filter.getParameters().stream()).toList());
            newFilter.addGreaterParameters(Stream.concat(typ2Filter.getOrDefault(CompareOperator.GE, new ArrayList<>()).stream(),
                            typ2Filter.getOrDefault(CompareOperator.GT, new ArrayList<>()).stream())
                    .flatMap((filter) -> filter.getParameters().stream()).toList());
            newFilter.setLessOperator(isAnd, typ2Filter);
            newFilter.setGreaterOperator(isAnd, typ2Filter);
            toMergeNodes.add(newFilter);
        }
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
    }

    public boolean getHasNot() {
        return hasNot;
    }

    public void setHasNot(boolean hasNot) {
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
