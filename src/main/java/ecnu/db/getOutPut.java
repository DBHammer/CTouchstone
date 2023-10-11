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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class getOutPut {
    private static final Pattern fromPattern = Pattern.compile("FROM [a-zA-Z0-9_\",]+ ");

    public static void main(String[] args) throws IOException, SQLException, TouchstoneException {
        File oldSqlFile = new File("D:\\eclipse-workspace\\public_bi_benchmark\\benchmark-classified\\oneWhere");
        //File newSqlFile = new File("D:\\eclipse-workspace\\Mirage\\resultBIBENCH\\queries");
        File newSqlFile = new File("D:\\eclipse-workspace\\Mirage\\result5\\queries");
        List<String> arrayList1 = new ArrayList<>();
        List<String> arrayList2 = new ArrayList<>();
        List<String> requireFileOld = getRequireFile(oldSqlFile, ".sql", arrayList1);
        List<String> requireFileNew = getRequireFile(newSqlFile, ".sql", arrayList2);
        DatabaseConnectorConfig config1 = new DatabaseConnectorConfig("49.52.27.35", "5632", "postgres", "Biui1227..", "bibench");
        DbConnector dbConnector1 = new PgConnector(config1);
        DatabaseConnectorConfig config2 = new DatabaseConnectorConfig("49.52.27.35", "5632", "postgres", "Biui1227..", "bibenchdemo");
        DbConnector dbConnector2 = new PgConnector(config2);
        for (int i = 0; i < requireFileOld.size(); i++) {
            String sql1 = requireFileOld.get(i);
            String sql2 = requireFileNew.get(i);
            sql1 = sql1.replace("SELECT * ", "SELECT COUNT(*) ");
            sql2 = sql2.replace("SELECT * ", "SELECT COUNT(*) ");
            sql1 = handleSql4BibenchOld(sql1);
            sql2 = handleSql4BibenchNew(sql2);
            //System.out.println(sql1);
//            sql1 = replaceCount(sql1);
//            sql2 = replaceCount(sql2);
//            sql1 = sql1.replaceFirst("\\*", "count(*)");
//            sql2 = sql2.replaceFirst("\\*", "count(*)");
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
            String tableName = "";
            Matcher matcher = fromPattern.matcher(sql1);
            if (matcher.find()) {
                tableName = matcher.group(0);
            }
            if(Math.abs(r1-r2) > 10){
                System.out.println((Math.abs(r1-r2) + sql1.substring(sql1.toLowerCase().indexOf("from"), sql1.toLowerCase().indexOf("from")+20)));
            }
            if (r1 != r2 || r1 == 0) {
                System.out.println(i + " " + r1 + " " + r2);
            } else {
                System.out.println(i + " " + r1 + " " + r2);
            }
        }


    }

    public static List<String> getRequireFile(File file, String suffix, List<String> arrayList) throws IOException {
        File[] listFiles = file.listFiles();
        assert listFiles != null;
        List<File> fileList = Arrays.asList(listFiles);
        fileList.sort((o1, o2) -> {
            if (o1.isDirectory() && o2.isFile())
                return -1;
            if (o1.isFile() && o2.isDirectory())
                return 1;
            return o1.getName().replace("_1.sql", ".sql").compareTo(o2.getName().replace("_1.sql", ".sql"));
        });
        for (File file1 : fileList) {
            System.out.print(file1.getName() + "\t");
        }
        System.out.println();
        for (File file2 : listFiles) {
            if (file2.getName().endsWith(suffix)) {
                BufferedReader bufferedReader = new BufferedReader(new FileReader(file2));
                StringBuilder content = new StringBuilder();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    if (!line.trim().startsWith("--")) {
                        content.append(line).append("\n");
                    }
                }
                arrayList.add(content.toString());
            }
        }
        return arrayList;
    }

    public static String replaceCount(String query) {
        String[] queryRow = query.split("\n");
        String queryFirstRow = queryRow[0];
        StringBuilder result = new StringBuilder("select *");
        for (int i = 1; i < queryRow.length; i++) {
            result.append(queryRow[i]);
            result.append("\n");
        }
        return result.toString();
    }

    public static String handleSql4BibenchOld(String originQuery) {
        String query = "SELECT count(*) FROM ";
        //将select的内容统一替换为count(*)
        String a = originQuery.split("FROM")[1];
        String b = a.split("GROUP BY")[0];
        query += originQuery.split("FROM")[originQuery.split("FROM").length - 1].split("GROUP BY")[0];
        return query;
    }

    public static String handleSql4BibenchNew(String originQuery) {
        String query = "select count(*) from ";
        //将select的内容统一替换为count(*)
        query += originQuery.split("from")[originQuery.split("from").length - 1].split("group by")[0];
        query += ";";
        Pattern cast = Pattern.compile("cast\\([0-9a-zA-Z:'_\\-. ]+\\)");
        Matcher matcher = cast.matcher(query);
        while (matcher.find()) {
            String castStr = matcher.group(0);
            String castInner = castStr.substring(5, castStr.length() - 1);
            query = query.replace(castStr, castInner);
        }
        query = query.replace(" as DATE", "");
        query = query.replace(" as BIGINT", "");
        query = query.replace(" as double precision", "");
        return query;
    }
}
