package ecnu.db.schema;

import ecnu.db.dbconnector.DbConnector;
import ecnu.db.dbconnector.adapter.PgConnector;
import ecnu.db.utils.DatabaseConnectorConfig;
import ecnu.db.utils.exception.TouchstoneException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CreateSchema {
    public static void main(String[] args) throws IOException, TouchstoneException, SQLException {
        TableManager.getInstance().setResultDir("result");
        TableManager.getInstance().loadSchemaInfo();
        ColumnManager.getInstance().setResultDir("result");
        ColumnManager.getInstance().loadColumnMetaData();
        //创建连接
        DatabaseConnectorConfig databaseConnectorConfig = new DatabaseConnectorConfig();
        databaseConnectorConfig.setDatabaseIp("biui.me");
        databaseConnectorConfig.setDatabaseName(null);
        databaseConnectorConfig.setDatabasePort("5432");
        databaseConnectorConfig.setDatabaseUser("postgres");
        databaseConnectorConfig.setDatabasePwd("Biui1227..");
        DbConnector dbConnector = new PgConnector(databaseConnectorConfig);
        String closeConnector = "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname='lihao' AND pid <> pg_backend_pid();";
        dbConnector.executeSql(closeConnector);
        String createTable = "DROP DATABASE IF EXISTS LIHAO;CREATE DATABASE LIHAO;";
        dbConnector.executeSql(createTable);
        databaseConnectorConfig.setDatabaseName("lihao");
        DbConnector dbConnector1 = new PgConnector(databaseConnectorConfig);
        for (Map.Entry<String, Table> tableName2Schema : TableManager.getInstance().getSchemas().entrySet()) {
            tableName2Schema.getValue().toSQL(dbConnector1, tableName2Schema.getKey());
        }
        List<String> addFks = new ArrayList<>();
        for (Map.Entry<String, Table> tableName2Schema : TableManager.getInstance().getSchemas().entrySet()) {
            String tableName = tableName2Schema.getKey();
            Table table = tableName2Schema.getValue();
            Map<String, String> foreignKeys = table.getForeignKeys();
            List<String> pks = new ArrayList<>(table.getPrimaryKeysList());
            String addPk = null;
            String addFk = null;
            if (!foreignKeys.isEmpty()) {
                for (Map.Entry<String, String> foreignKey : foreignKeys.entrySet()) {
                    pks.removeIf(pk -> pk.equals(foreignKey.getKey()));
                    String key = foreignKey.getKey().split("\\.")[2].toUpperCase();
                    String tableRef = foreignKey.getValue().split("\\.")[1].toUpperCase();
                    String indexName = tableName.split("\\.")[1] + "_" + key.split("_")[1].toLowerCase();
                    String simpleTableName = tableName.split("\\.")[1].toUpperCase();
                    addFk = String.format("ALTER TABLE %s ADD FOREIGN KEY (%s) references %s; CREATE INDEX %s on %s(%s);", simpleTableName, key, tableRef, indexName, simpleTableName, key);
                    addFks.add(addFk);
                    System.out.println(addFk);
                }
            }
            if (!pks.isEmpty()) {
                for (String pk : pks) {
                    pk = pk.split(",")[0].split("\\.")[2].toUpperCase();
                    String simpleTableName = tableName.split("\\.")[1].toUpperCase();
                    addPk = String.format("ALTER TABLE %s ADD PRIMARY KEY (%s);", simpleTableName, pk);
                    System.out.println(addPk);
                }
            }
            if (addPk != null) {
                dbConnector1.executeSql(addPk);
            }
        }
        for (String addFk : addFks) {
            dbConnector1.executeSql(addFk);
        }


    }
}
