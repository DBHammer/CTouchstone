package ecnu.db;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class replaceCount {
    public static void main(String[] args) throws IOException {
        File sqlFile = new File("D:\\eclipse-workspace\\Mirage\\resultSSB\\queries");
        Map<String, String> queryName2Sql = new HashMap<>();
        read(queryName2Sql, sqlFile);
        for (Map.Entry<String, String> q2Sql : queryName2Sql.entrySet()) {
            String queryName = q2Sql.getKey();
            String sql = q2Sql.getValue();
            String newSql = sql.replaceFirst("\\*", "count(*)");
            FileWriter fileWriter = new FileWriter("D:\\eclipse-workspace\\Mirage\\washTPCDS\\tpcdsMirageNew" + "\\" + queryName);
            fileWriter.write(newSql);
            fileWriter.close();
        }

    }

    private static void read(Map<String, String> map, File srcFolder) throws IOException {
        File[] fileArray = srcFolder.listFiles();
        assert fileArray != null;
        for (File file : fileArray) {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            StringBuilder content = new StringBuilder();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                if (!line.trim().startsWith("--")) {
                    content.append(line).append(" ");
                }
            }
            map.put(file.getName(), content.toString());
        }
    }
}
