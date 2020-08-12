package ecnu.db.constraintchain.filter.operation;

import ecnu.db.constraintchain.filter.BoolExprType;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.schema.column.AbstractColumn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author alan
 */
public class RangeFilterOperation extends UniVarFilterOperation {
    private final List<Parameter> lessParameters = new ArrayList<>();
    private CompareOperator lessOperator;
    private final List<Parameter> greaterParameters = new ArrayList<>();
    private CompareOperator greaterOperator;
    public RangeFilterOperation(String columnName) {
        super(columnName, CompareOperator.RANGE);
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
    }

    @Override
    public void setParameters(List<Parameter> parameters) {
        throw new UnsupportedOperationException();
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

    public List<Parameter> getLessParameters() {
        return lessParameters;
    }

    public void addLessParameters(Collection<Parameter> parameter) {
        lessParameters.addAll(parameter);
    }

    public List<Parameter> getGreaterParameters() {
        return greaterParameters;
    }

    public void addGreaterParameters(Collection<Parameter> parameter) {
        greaterParameters.addAll(parameter);
    }

    public CompareOperator getLessOperator() {
        return lessOperator;
    }

    public void setLessOperator(CompareOperator lessOperator) {
        this.lessOperator = lessOperator;
    }

    public CompareOperator getGreaterOperator() {
        return greaterOperator;
    }

    public void setGreaterOperator(CompareOperator greaterOperator) {
        this.greaterOperator = greaterOperator;
    }

    @Override
    public void instantiateUniParamCompParameter(AbstractColumn absColumn) {
        if (lessParameters.size() == 0 && greaterParameters.size() > 0) {
            instantiateUniParamCompParameter(absColumn, lessOperator, lessParameters);
        }
        else if (greaterParameters.size() == 0 && lessParameters.size() > 0) {
            instantiateUniParamCompParameter(absColumn, lessOperator, lessParameters);
        }
    }

    public void instantiateBetweenParameter(AbstractColumn absColumn) {
        String lessParamStr = lessParameters.stream().map(Parameter::getData).collect(Collectors.joining()),
                greaterParamStr = greaterParameters.stream().map(Parameter::getData).collect(Collectors.joining());
        if (absColumn.hasNotMetCondition(lessOperator + lessParamStr + greaterOperator + greaterParamStr)) {
            absColumn.addCondition(lessOperator + lessParamStr + greaterOperator + greaterParamStr);
            absColumn.insertBetweenProbability(probability, lessParameters, greaterParameters);
        }
    }
}
