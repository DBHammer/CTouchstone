package ecnu.db.generation;

import ecnu.db.exception.TouchstoneException;
import ecnu.db.schema.Schema;

import java.util.*;

/**
 * @author irisyou
 */
public class Preprocessor {

    /**
     * Map<String, Schema>中，String是表名，Schema是表结构
     *
     * @param schemas 输入的乱序的表结构
     * @return 按照拓扑序排序的表结构
     */
    public static List<Schema> getTableOrder(Map<String, Schema> schemas) throws TouchstoneException {

        List<Schema> tableOrder = new ArrayList<>();

        // map: tables -> referenced tables
        HashMap<Schema, ArrayList<String>> tableDependencyInfo = new HashMap<>();

        for (Schema schema : schemas.values()) {
            if (schema.getForeignKeys().size() != 0) {
                Map<String, String> foreignKeys = schema.getForeignKeys();
                ArrayList<String> referencedTables = new ArrayList<>();
                for (int j = 0; j < foreignKeys.size(); j++) {
                    referencedTables.add(foreignKeys.get(j).split("\\.")[0]);
                }
                tableDependencyInfo.put(schema, referencedTables);
            } else {
                tableOrder.add(schema);
            }
        }

        int lastTableOrderSize = tableOrder.size();//记录经过上一次循环后的拓扑序中table的数量

        Iterator<HashMap.Entry<Schema, ArrayList<String>>> iterator = tableDependencyInfo.entrySet().iterator();
        while (true) {
            while (iterator.hasNext()) {
                HashMap.Entry<Schema, ArrayList<String>> entry = iterator.next();
                if (tableOrder.containsAll(entry.getValue())) {
                    tableOrder.add(entry.getKey());
                    iterator.remove();//将已经加入排序的schema从tableDependencyInfo里移除
                }
            }

            if (tableOrder.size() == schemas.size()) {
                break;
            }
            if (tableOrder.size() == lastTableOrderSize) { //如果本轮循环中没有加入新的表
                throw new TouchstoneException("该数据库表中外键约束不完整");
            }
            iterator = tableDependencyInfo.entrySet().iterator();
            lastTableOrderSize = tableOrder.size();
        }

        return tableOrder;
    }

}
