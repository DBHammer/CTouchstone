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

    public static List<String> getTableOrder(List<Schema> schemas) {

        Set<String> allSchemas = new HashSet<String>();

        //含有外键的schemas
        Set<String> schemasWithFK = new HashSet<String>();

        // map: tables -> referenced tables
        Map<String, ArrayList<String>> tableDependencyInfo = new HashMap<String, ArrayList<String>>();

        for (int i = 0; i < schemas.size(); i++) {
            Schema schema = schemas.get(i);
            allSchemas.add(schema.getTableName());
            if (schema.getForeignKeys().size() != 0) {
                schemasWithFK.add(schema.getTableName());
                HashMap<String, String> foreignKeys = schema.getForeignKeys();
                ArrayList<String> referencedTables = new ArrayList<String>();
                for (int j = 0; j < foreignKeys.size(); j++) {
                    referencedTables.add(foreignKeys.get(j).split("\\.")[0]);
                }
                tableDependencyInfo.put(schema.getTableName(), referencedTables);
            }
        }

        //先剩下只被依赖的schema
        allSchemas.removeAll(schemasWithFK);
        Set<String> tableOrder = new LinkedHashSet<String>();
        tableOrder.addAll(allSchemas);
        Iterator<Map.Entry<String, ArrayList<String>>> iterator = tableDependencyInfo.entrySet().iterator();
        while (true) {
            while (iterator.hasNext()) {
                Map.Entry<String, ArrayList<String>> entry = iterator.next();
                if (tableOrder.containsAll(entry.getValue())) {
                    tableOrder.add(entry.getKey());
                }
            }
            if (tableOrder.size() == schemas.size()) {
                break;
            }
            iterator = tableDependencyInfo.entrySet().iterator();
        }

        logger.info("\nThe order of tables: \n\t" + tableOrder);
        return tableOrder.stream().collect(Collectors.toList());
    }

}
