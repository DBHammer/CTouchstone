package ecnu.db.generator.constraintchain.filter;

import com.fasterxml.jackson.annotation.JsonIgnore;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.ColumnType;
import ecnu.db.utils.CommonUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author alan
 * 代表需要实例化的参数
 */
public class Parameter {

    private static final Pattern CanonicalColumnName = Pattern.compile("[a-zA-Z][a-zA-Z0-9$_]*\\.[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+");

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

    public boolean isActual() {
        return isActual;
    }

    public void setActual(boolean actual) {
        isActual = actual;
    }

    /**
     * 此参数是否为确定的值
     */
    @JsonIgnore
    private boolean isActual = true;

    public Parameter() {
    }

    public Parameter(Integer id, String operand, String dataValue) {
        this.id = id;
        this.operand = operand;
        Matcher matcher = CanonicalColumnName.matcher(operand);
        List<String> cols = new ArrayList<>();
        if(matcher.find()){
            cols.add(matcher.group());
        }
        if(cols.size()==1 &&  ColumnManager.getInstance().getColumnType(cols.get(0)) == ColumnType.DATE){
            dataValue = dataValue.split(" ")[0];
        }
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
