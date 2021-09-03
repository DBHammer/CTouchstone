package ecnu.db.generator.constraintchain.filter.operation;

import ecnu.db.generator.constraintchain.filter.BoolExprType;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.schema.ColumnManager;
import ecnu.db.utils.exception.analyze.IllegalQueryColumnNameException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author alan
 */
public class RangeFilterOperation extends UniVarFilterOperation {
    private final List<Parameter> lessParameters = new ArrayList<>();
    private final List<Parameter> greaterParameters = new ArrayList<>();
    private CompareOperator lessOperator;
    private CompareOperator greaterOperator;

    public RangeFilterOperation(String columnName) throws IllegalQueryColumnNameException {
        super(columnName, CompareOperator.RANGE, null);
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
    }

    @Override
    public List<Parameter> getParameters() {
        List<Parameter> parameters = new ArrayList<>(lessParameters);
        parameters.addAll(greaterParameters);
        return parameters;
    }

    @Override
    public void setParameters(List<Parameter> parameters) {
        throw new UnsupportedOperationException();
    }

    public void setLessOperator(boolean isAnd, Map<CompareOperator, List<UniVarFilterOperation>> typ2Filter) {
        this.lessOperator = (isAnd && (typ2Filter.containsKey(CompareOperator.LT)) ? CompareOperator.LT : CompareOperator.LE);
    }

    public void setGreaterOperator(boolean isAnd, Map<CompareOperator, List<UniVarFilterOperation>> typ2Filter) {
        this.greaterOperator = (isAnd && typ2Filter.containsKey(CompareOperator.GT)) ? CompareOperator.GT : CompareOperator.GE;
    }

    public void addLessParameters(Collection<Parameter> parameter) {
        lessParameters.addAll(parameter);
    }

    public void addGreaterParameters(Collection<Parameter> parameter) {
        greaterParameters.addAll(parameter);
    }

    @Override
    public void instantiateParameter() {
        if (!lessParameters.isEmpty() && !greaterParameters.isEmpty()) {
            ColumnManager.getInstance().insertBetweenProbability(canonicalColumnName, probability,
                    lessOperator, lessParameters, greaterOperator, greaterParameters);
        } else {
            super.instantiateParameter();
        }
    }

    @Override
    public boolean[] evaluate() {
        throw new UnsupportedOperationException();
    }
}
