package ecnu.db;

import com.google.protobuf.Internal;
import ecnu.db.dbconnector.DbConnector;
import ecnu.db.dbconnector.adapter.PgConnector;
import ecnu.db.utils.DatabaseConnectorConfig;
import ecnu.db.utils.exception.TouchstoneException;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class getMirageError {
    public static final Pattern ACTUAL_ROWS = Pattern.compile("(Hash Join \\(actual rows=[0-9]+|Seq Scan on [a-zA-Z_]+ \\(actual rows=[0-9]+|Seq Scan on [a-zA-Z_]+ [a-zA-Z0-9_]+ \\(actual rows=[0-9]+)");

    public static void main(String[] args) throws IOException, SQLException, TouchstoneException {
        File oldSqlFile = new File("D:\\eclipse-workspace\\Mirage\\TpcdsInMirageWithSameJoinOrder\\tpcdsOrigin");
        File newSqlFile = new File("D:\\eclipse-workspace\\Mirage\\TpcdsInMirageWithSameJoinOrder\\tpcdsNew");
        List<String> arrayList1 = new ArrayList<>();
        List<String> arrayList2 = new ArrayList<>();
        List<String> requireFileOld = getRequireFile(oldSqlFile, ".sql", arrayList1);
        List<String> requireFileNew = getRequireFile(newSqlFile, ".sql", arrayList2);
        DatabaseConnectorConfig config1 = new DatabaseConnectorConfig("biui.me", "5432", "postgres", "Biui1227..", "tpcds");
        DbConnector dbConnector1 = new PgConnector(config1);
        DatabaseConnectorConfig config2 = new DatabaseConnectorConfig("biui.me", "5432", "postgres", "Biui1227..", "tpcdsdemo");
        DbConnector dbConnector2 = new PgConnector(config2);
        //记录结果
        int[][] result = new int[11][200];
        for (int i = 0; i < 100; i++) {
            String sql1 = requireFileOld.get(i);
            String sql2 = requireFileNew.get(i);
            sql1 = sql1.replaceFirst("\\*", "count(*)");
            sql2 = sql2.replaceFirst("\\*", "count(*)");
            List<String[]> r1 = dbConnector1.getQueryPlan(sql1);
            List<String[]> r2 = dbConnector2.getQueryPlan(sql2);
            String originPlan = join(r1);
            String newPlan = join(r2);
            List<Integer> originRow = new ArrayList<>();
            List<Integer> newRow = new ArrayList<>();
            Matcher originMatch = ACTUAL_ROWS.matcher(originPlan);
            Matcher newMatch = ACTUAL_ROWS.matcher(newPlan);
            while (originMatch.find() && newMatch.find()) {
                int currentOriginRow = Integer.parseInt(originMatch.group(0).split("=")[1]);
                int currentNewRow = Integer.parseInt(newMatch.group(0).split("=")[1]);
                originRow.add(currentOriginRow);
                newRow.add(currentNewRow);
            }
            if (originRow.size() != newRow.size()) {
                throw new UnsupportedOperationException();
            }
            System.out.println(i + 1 + ":");
            for (int i1 = 0; i1 < originRow.size(); i1++) {
                System.out.println(originRow.get(i1) + " " + newRow.get(i1));
                result[i1][2 * i] = originRow.get(i1);
                result[i1][2 * i + 1] = newRow.get(i1);
            }
//            System.out.println(originPlan);
//            System.out.println(newPlan);
        }
        FileWriter fw = new FileWriter(new File("D:\\eclipse-workspace\\Mirage\\TpcdsInMirageWithSameJoinOrder\\result"));
        BufferedWriter bw = new BufferedWriter(fw);
        for (int i = 0; i < 11; i++) {
            for (int j = 0; j < 200; j++) {
                if (result[i][j] != 0) {
                    bw.write(result[i][j] + " ");
                } else {
                    bw.write("0 ");
                }
            }
            bw.write("\n");
        }
        bw.close();
        fw.close();
    }

    public static String join(List<String[]> list) {
        StringBuilder sb = new StringBuilder();
        for (String[] strings : list) {
            for (String string : strings) {
                sb.append(string).append("\n");
            }
        }
        return sb.toString();
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
}
