package ecnu.db;

import ecnu.db.dbconnector.DbConnector;
import ecnu.db.dbconnector.adapter.PgConnector;
import ecnu.db.utils.DatabaseConnectorConfig;
import ecnu.db.utils.exception.TouchstoneException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class getOutPut {
    public static void main(String[] args) throws IOException, SQLException, TouchstoneException {
        File oldSqlFile = new File("D:\\eclipse-workspace\\Mirage\\conf\\queriesTPCDS");
        File newSqlFile = new File("D:\\eclipse-workspace\\Mirage\\conf\\queries");
        List<String> arrayList1 = new ArrayList<>();
        List<String> arrayList2 = new ArrayList<>();
        List<String> requireFileOld = getRequireFile(oldSqlFile, ".sql", arrayList1);
        List<String> requireFileNew = getRequireFile(newSqlFile, ".sql", arrayList2);
        DatabaseConnectorConfig config1 = new DatabaseConnectorConfig("biui.me", "5432", "postgres", "Biui1227..", "tpcds");
        DbConnector dbConnector1 = new PgConnector(config1);
        DatabaseConnectorConfig config2 = new DatabaseConnectorConfig("biui.me", "5432", "postgres", "Biui1227..", "tpcdsdemo");
        DbConnector dbConnector2 = new PgConnector(config2);
        for (int i = 0; i < requireFileOld.size(); i++) {
            String sql1 = requireFileOld.get(i);
            String sql2 = requireFileNew.get(i);
            //sql1 = replaceCount(sql1);
            //sql2 = replaceCount(sql2);
            sql1 = sql1.replaceFirst("\\*", "count(*)");
            sql2 = sql2.replaceFirst("\\*", "count(*)");
            /*for (int i1 = 0; i1 < 3; i1++) {
                dbConnector1.getSqlResult(sql1);
            }*/
            long start = System.currentTimeMillis();
            int r1 = dbConnector1.getSqlResult(sql1);
            long round1 = System.currentTimeMillis() - start;
            //System.out.print(round1 + " ");
            /*for (int i1 = 0; i1 < 3; i1++) {
                dbConnector2.getSqlResult(sql2);
            }*/
            start = System.currentTimeMillis();
            int r2 = dbConnector2.getSqlResult(sql2);
            long round2 = System.currentTimeMillis() - start;
            //System.out.print(round2 + " ");
            //System.out.println(((double) (Math.abs(round2 - round1))) / (double) round1 + " ");
            if (r1 != r2 || r1 == 0) {
                System.out.println(i + " " + r1 + " " + r2);
            } else {
                System.out.println(i);
            }
        }


    }

    public static List<String> getRequireFile(File file, String suffix, List<String> arrayList) throws IOException {
        File[] listFiles = file.listFiles();
        assert listFiles != null;
        for (File file2 : listFiles) {
            if (file2.getName().endsWith(suffix)) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file2));
                StringBuilder content = new StringBuilder();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    if (!line.trim().startsWith("--")) {
                        content.append(line).append(" ");
                    }
                }
                arrayList.add(content.toString());
            } else if (file2.isDirectory()) {
                getRequireFile(file2, suffix, arrayList);
            }
        }
        return arrayList;
    }

    public static String replaceCount(String query){
        String[] queryRow= query.split("\n");
        String queryFirstRow = queryRow[0];
        StringBuilder result = new StringBuilder("select *");
        for (int i = 1; i < queryRow.length; i++) {
            result.append(queryRow[i]);
            result.append("\n");
        }
        return result.toString();
    }
}
