package ecnu.db.constraintchain.filter.operation;

import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.schema.ColumnManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author alan
 */
public class RangeFilterOperation extends UniVarFilterOperation {
    private final List<Parameter> lessParameters = new ArrayList<>();
    private final List<Parameter> greaterParameters = new ArrayList<>();
    private CompareOperator lessOperator;
    private CompareOperator greaterOperator;

    public RangeFilterOperation(String columnName) {
        super(columnName, CompareOperator.RANGE);
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
    }

    @Override
    public void addParameter(Parameter parameter) {
        throw new UnsupportedOperationException();
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

    public void setLessOperator(boolean isAnd, Multimap<CompareOperator, UniVarFilterOperation> typ2Filter) {
        this.lessOperator = (isAnd && (typ2Filter.containsKey(CompareOperator.LT)) ? CompareOperator.LT : CompareOperator.LE);
    }

    public void setGreaterOperator(boolean isAnd, Multimap<CompareOperator, UniVarFilterOperation> typ2Filter) {
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
        if (lessParameters.size() > 0 && greaterParameters.size() > 0) {
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
