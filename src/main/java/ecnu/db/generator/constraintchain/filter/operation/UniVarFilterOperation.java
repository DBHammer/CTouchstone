package ecnu.db.generator.constraintchain.filter.operation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import ecnu.db.generator.constraintchain.filter.BoolExprNode;
import ecnu.db.generator.constraintchain.filter.BoolExprType;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.TableManager;
import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.utils.exception.analyze.IllegalQueryColumnNameException;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ecnu.db.generator.constraintchain.filter.operation.CompareOperator.GE;
import static ecnu.db.generator.constraintchain.filter.operation.CompareOperator.LT;


/**
 * @author wangqingshuai
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UniVarFilterOperation extends AbstractFilterOperation {
    protected String canonicalColumnName;

    public UniVarFilterOperation() {
        super(null);
    }

    public UniVarFilterOperation(String canonicalColumnName, CompareOperator operator, List<Parameter> parameters)
            throws IllegalQueryColumnNameException {
        super(operator);
        this.canonicalColumnName = canonicalColumnName;
        if (CommonUtils.isNotCanonicalColumnName(canonicalColumnName)) {
            throw new IllegalQueryColumnNameException();
        }
        this.parameters = parameters;
    }

    public void amendParameters() {
        if (operator == GE || operator == LT) {
            for (Parameter parameter : parameters) {
                parameter.setData(parameter.getData() + 1);
                parameter.setDataValue(ColumnManager.getInstance().getColumn(canonicalColumnName).transferDataToValue(parameter.getData()));
            }
        }
    }

    @Override
    public boolean hasKeyColumn() {
        return TableManager.getInstance().isPrimaryKey(canonicalColumnName) || TableManager.getInstance().isForeignKey(canonicalColumnName);
    }

    @Override
    public BoolExprType getType() {
        return BoolExprType.UNI_FILTER_OPERATION;
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
        return String.format("%s(%s)", operator.toString().toLowerCase(), String.join(", ", content));
    }

    /**
     * 初始化等值filter的参数
     */
    public void applyConstraint() {
        ColumnManager.getInstance().applyUniVarConstraint(canonicalColumnName, probability, operator, parameters);
    }

    @Override
    public boolean[] evaluate() {
        return ColumnManager.getInstance().evaluate(canonicalColumnName, operator, parameters);
    }

    @JsonIgnore
    @Override
    public boolean isDifferentTable(String tableName) {
        return !canonicalColumnName.contains(tableName);
    }

    @Override
    public String toSQL() {
        String parametersSQL;
        if (parameters.size() == 1) {
            parametersSQL = "'" + parameters.get(0).getDataValue() + "'";
        } else {
            parametersSQL = "('" + parameters.stream().map(Parameter::getDataValue).collect(Collectors.joining("','")) + "')";
        }
        return canonicalColumnName + CompareOperator.toSQL(operator) + parametersSQL;
    }


}
