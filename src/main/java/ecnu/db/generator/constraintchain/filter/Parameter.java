package ecnu.db.generator.constraintchain.filter;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;

/**
 * @author alan
 * 代表需要实例化的参数
 */
@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, resolver = ParameterResolver.class, property = "id", scope = Parameter.class)
public class Parameter {

    /**
     * parameter的id，用于后续实例化
     */
    private int id;
    /**
     * parameter的内部data，用于快速计算
     */
    private long data;
    /**
     * 操作数
     */
    @JsonIgnore
    private String operand;
    /**
     * String化的值
     */
    @JsonIgnore
    private String dataValue;

    public Parameter() {
    }

    public Parameter(Integer id, String operand, String dataValue) {
        this.id = id;
        this.operand = operand;
        this.dataValue = dataValue;
    }

    public Integer getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getDataValue() {
        return dataValue;
    }

    public void setDataValue(String dataValue) {
        this.dataValue = dataValue;
    }

    public long getData() {
        return data;
    }


    public void setData(long data) {
        this.data = data;
    }

    @JsonIgnore
    public String getOperand() {
        return operand;
    }

    public void setOperand(String operand) {
        this.operand = operand;
    }

    @Override
    public String toString() {
        return "{id:" + id + ", data:" + dataValue + "}";
    }
}
