package ecnu.db.schema;

import ecnu.db.utils.CommonUtils;
import ecnu.db.utils.exception.TouchstoneException;
import picocli.CommandLine;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "create", description = "generate the ddl sql for the new database")
public class DDLGenerator implements Callable<Integer> {
    @CommandLine.Option(names = {"-c", "--config_path"}, required = true, description = "the config path for creating ddl")
    private String configPath;
    @CommandLine.Option(names = {"-d", "--database"}, defaultValue = "TouchstoneDatabase", description = "the database name")
    private String dataBase;
    @CommandLine.Option(names = {"-o", "--output"}, defaultValue = "./ddl", description = "the output path for dll")
    private String outputPath;

    private static final String OUTPUT_PATH = "./output";
    private static final String CREATE_SCHEMA_PATH = OUTPUT_PATH + "/CreateSchema.sql";
    private static final String CREATE_INDEX_PATH = OUTPUT_PATH + "/CreateIndex.sql";
    private static final String IMPORT_DATA = OUTPUT_PATH + "/importData.sql";

    @Override
    public Integer call() throws IOException, SQLException, TouchstoneException {
        TableManager.getInstance().setResultDir(configPath);
        TableManager.getInstance().loadSchemaInfo();
        ColumnManager.getInstance().setResultDir(configPath);
        ColumnManager.getInstance().loadColumnMetaData();
        //构建createschema语句
        createSchema();
        //构建数据导入语句
        importData();
        //构建createindex语句
        createIndex();
        return null;
    }

    public void createSchema() throws SQLException, TouchstoneException, IOException {
        String createTable = "DROP DATABASE IF EXISTS "+ dataBase + ";\nCREATE DATABASE "+ dataBase + ";\n";
        StringBuilder createSchema = new StringBuilder(createTable);
        createSchema.append("\\c ").append(dataBase).append("\n");
        for (Map.Entry<String, Table> tableName2Schema : TableManager.getInstance().getSchemas().entrySet()) {
            createSchema.append(tableName2Schema.getValue().getSql(tableName2Schema.getKey())).append("\n");
        }
        CommonUtils.writeFile(CREATE_SCHEMA_PATH, createSchema.toString());
    }

    public void importData() throws IOException {
        StringBuilder importData = new StringBuilder("\\c " + dataBase +";\n");
        for (Map.Entry<String, Table> tableName2Schema : TableManager.getInstance().getSchemas().entrySet()) {
            String tableName = tableName2Schema.getKey();
            String inData = "\\Copy " + tableName.split("\\.")[1] + " FROM " + "'" + "./data/" + tableName + "0" + "' DELIMITER ',';\n";
            importData.append(inData);
        }
        CommonUtils.writeFile(IMPORT_DATA, importData.toString());
    }
    public void createIndex() throws IOException {
        List<String> addFks = new ArrayList<>();
        StringBuilder createIndex =  new StringBuilder("\\c " + dataBase + "\n");
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
                    addFk = String.format("ALTER TABLE %s ADD FOREIGN KEY (%s) references %s;%nCREATE INDEX %s on %s(%s);%n", simpleTableName, key, tableRef, indexName, simpleTableName, key);
                    addFks.add(addFk);
                }
            }
            if (!pks.isEmpty()) {
                for (String pk : pks) {
                    pk = pk.split(",")[0].split("\\.")[2].toUpperCase();
                    String simpleTableName = tableName.split("\\.")[1].toUpperCase();
                    addPk = String.format("ALTER TABLE %s ADD PRIMARY KEY (%s);%n", simpleTableName, pk);
                }
            }
            if (addPk != null) {
                createIndex.append(addPk + "\n");
            }
        }
        for (String addFk : addFks) {
            createIndex.append(addFk + "\n");
        }
        CommonUtils.writeFile(CREATE_INDEX_PATH, createIndex.toString());
    }
}
