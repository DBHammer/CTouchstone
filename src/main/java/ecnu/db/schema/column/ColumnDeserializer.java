package ecnu.db.schema.column;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.DefaultJSONParser;
import com.alibaba.fastjson.parser.deserializer.ObjectDeserializer;

import java.lang.reflect.Type;

/**
 * @author xuechao.lian
 */
public class ColumnDeserializer implements ObjectDeserializer {

    @Override
    public <T> T deserialze(DefaultJSONParser parser, Type type, Object filedName) {
        Integer value = null;
        JSONObject col = parser.parseObject();
        if (col == null) {
            return null;
        }
        switch (col.getObject("columnType", ColumnType.class)) {
            case INTEGER:
                IntColumn intColumn = new IntColumn(col.getString("columnName"));
                intColumn.setNullPercentage(col.getFloatValue("nullPercentage"));
                intColumn.setNdv(col.getIntValue("ndv"));
                intColumn.setMin(col.getIntValue("min"));
                intColumn.setMax(col.getIntValue("max"));
                return (T) intColumn;
            case BOOL:
                BoolColumn boolColumn = new BoolColumn(col.getString("columnName"));
                boolColumn.setNullPercentage(col.getFloatValue("nullPercentage"));
                return (T) boolColumn;
            case DECIMAL:
                DecimalColumn decimalColumn = new DecimalColumn(col.getString("columnName"));
                decimalColumn.setNullPercentage(col.getFloatValue("nullPercentage"));
                decimalColumn.setMin(col.getDoubleValue("min"));
                decimalColumn.setMax(col.getDoubleValue("max"));
                return (T) decimalColumn;
            case VARCHAR:
                StringColumn stringColumn = new StringColumn(col.getString("columnName"));
                stringColumn.setNullPercentage(col.getFloatValue("nullPercentage"));
                stringColumn.setNdv(col.getIntValue("ndv"));
                stringColumn.setMaxLength(col.getIntValue("maxLength"));
                stringColumn.setAvgLength(col.getBigDecimal("avgLength"));
                return (T) stringColumn;
            case DATETIME:
                DateColumn dateColumn = new DateColumn(col.getString("columnName"));
                dateColumn.setNullPercentage(col.getFloatValue("nullPercentage"));
                dateColumn.setBegin(col.getString("begin"));
                dateColumn.setEnd(col.getString("end"));
                return (T) dateColumn;
            default:
        }

        return (T) col;
    }

    @Override
    public int getFastMatchToken() {
        return 0;
    }
}
