package ecnu.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class getAllMultiFKInTPCDS {
    /* ws_bill_addr_sk
       ws_ship_addr_sk
       ws_ship_date_sk
       ws_sold_date_sk
       cs_bill_addr_sk
       cs_ship_addr_sk
       cs_ship_date_sk
       cs_sold_date_sk
    */
    public static void main(String[] args) throws IOException {
        File sqlFile = new File("D:\\eclipse-workspace\\Mirage\\conf\\queriesTPCDS");
        Map<String, String> name2Sql = new HashMap<>();
        Map<String, String> requireFile = getRequireFile(sqlFile, ".sql", name2Sql);
        ArrayList<ArrayList<String>> sqlFiles = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            sqlFiles.add(new ArrayList<>());
        }
        for (Map.Entry<String, String> query : requireFile.entrySet()) {
            String name = query.getKey();
            String sql = query.getValue();
            //if(sql.contains("ws_bill_addr_sk")||sql.contains("ws_ship_addr_sk")||sql.contains("ws_ship_date_sk")||sql.contains("ws_sold_date_sk")||sql.contains("cs_bill_addr_sk")||sql.contains("cs_ship_addr_sk")||sql.contains("cs_ship_date_sk")||sql.contains("cs_sold_date_sk")){
            if (sql.contains("ws_bill_addr_sk")) {
                sqlFiles.get(0).add(name);
            }
            if (sql.contains("ws_ship_addr_sk")) {
                sqlFiles.get(1).add(name);
            }

            if (sql.contains("ws_ship_date_sk")) {
                sqlFiles.get(2).add(name);
            }
            if (sql.contains("ws_sold_date_sk")) {
                sqlFiles.get(3).add(name);
            }

            if (sql.contains("cs_bill_addr_sk")) {
                sqlFiles.get(4).add(name);
            }
            if (sql.contains("cs_ship_addr_sk")) {
                sqlFiles.get(5).add(name);
            }
            if (sql.contains("cs_ship_date_sk")) {
                sqlFiles.get(6).add(name);
                //System.out.println(sql);
            }
            if (sql.contains("cs_sold_date_sk")) {
                sqlFiles.get(7).add(name);
            }
        }
        for (ArrayList<String> s : sqlFiles) {
            System.out.println(s);
        }
        Set<String> a = new TreeSet<>();
        a.addAll(sqlFiles.get(0));
        a.addAll(sqlFiles.get(3));
        a.addAll(sqlFiles.get(4));
        a.addAll(sqlFiles.get(7));
        sqlFiles.get(1).forEach(a::remove);
        sqlFiles.get(2).forEach(a::remove);
        sqlFiles.get(5).forEach(a::remove);
        sqlFiles.get(6).forEach(a::remove);
        System.out.println(a);
        System.out.println(a.size());
    }

    public static Map<String, String> getRequireFile(File file, String suffix, Map<String, String> arrayList) throws IOException {
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
                arrayList.put(file2.getName(), content.toString());
            } else if (file2.isDirectory()) {
                getRequireFile(file2, suffix, arrayList);
            }
        }
        return arrayList;
    }
}
