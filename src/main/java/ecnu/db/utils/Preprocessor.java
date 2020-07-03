package ecnu.db.utils;

import ecnu.db.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author irisyou
 */
public class Preprocessor {

    private static final Logger logger = LoggerFactory.getLogger(Preprocessor.class);

    /**
     * Map<String, Schema>中，String是表名，Schema是表结构
     * @param schemas 输入的乱序的表结构
     * @return 按照拓扑序排序的表结构
     */
    public static List<Schema> getTableOrder(Map<String, Schema> schemas) throws TouchstoneToolChainException {

        List<Schema> tableOrder = new ArrayList<Schema>();

        // map: tables -> referenced tables
        HashMap<Schema, ArrayList<String>> tableDependencyInfo = new HashMap<Schema, ArrayList<String>>();

        for(Schema schema : schemas.values()) {
            if (schema.getForeignKeys().size() != 0) {
                HashMap<String, String> foreignKeys = schema.getForeignKeys();
                ArrayList<String> referencedTables = new ArrayList<String>();
                for (int j = 0; j < foreignKeys.size(); j++) {
                    referencedTables.add(foreignKeys.get(j).split("\\.")[0]);
                }
                tableDependencyInfo.put(schema, referencedTables);
            }
            else {
                tableOrder.add(schema);
            }
        }

        List<Schema> lastTableOrder = new ArrayList<Schema>();//记录经过上一次循环后的拓扑序

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
            if(tableOrder.size() == lastTableOrder.size()){ //如果本轮循环中没有加入新的表
                throw new TouchstoneToolChainException("该数据库表中外键约束不完整");
            }
            iterator = tableDependencyInfo.entrySet().iterator();
            lastTableOrder = tableOrder;
        }

        logger.info("\nThe order of tables: \n\t" + tableOrder);

        return tableOrder;
    }

}
