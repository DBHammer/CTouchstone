package ecnu.db;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class getAllTPCDS {
    private static final Pattern PARA = Pattern.compile("'[0-9]+'");

    public static void main(String[] args) throws IOException {
        File file = new File("D:\\eclipse-workspace\\Mirage\\conf\\queriesTPCDS");
        List<String> arrayList = new ArrayList<>();
        List<String> requireFile = getRequireFile(file, ".sql", arrayList);
        String filePath = "D:\\eclipse-workspace\\Mirage\\conf\\queriesTPCDS2";
        for (int i = 1; i <= requireFile.size(); i++) {
            String query = requireFile.get(i-1);
            String fileName;
            if (i < 10) {
                fileName = "q00" + i + ".sql";
            } else if (i < 100) {
                fileName = "q0" + i + ".sql";
            } else {
                fileName = "q" + i + ".sql";
            }
            try {
                FileWriter fileWriter = new FileWriter(filePath + "\\" + fileName);
                fileWriter.write(query);
                fileWriter.close();
            }catch (Exception e){
                throw new RuntimeException();
            }
        }
        //List<String> newList = ridRepeat(requireFile);
        //System.out.println(requireFile);
        //System.out.println(requireFile.size());
    }

    public static List<String> getRequireFile(File file, String suffix, List<String> arrayList) throws IOException {
        File[] listFiles = file.listFiles();
        assert listFiles != null;
        for (File file2 : listFiles) {
            if (file2.getName().endsWith(suffix)) {
                String content = new String(Files.readAllBytes(Paths.get(file2.getPath())));
                //arrayList.add(file2.getName());
                //String contentWithoutPara = removePara(content);
                //String name = file2.getName();
                arrayList.add(content);
            } else if (file2.isDirectory()) {
                getRequireFile(file2, suffix, arrayList);
            }
        }
        return arrayList;
    }

    public static String removePara(String query) {
        if (query.contains("cannotFindArgs")) {
            query = removeFirstLine(query);
        }
        Matcher matcher = PARA.matcher(query);
        return matcher.replaceAll("").trim();
    }

    public static List<String> ridRepeat(List<String> list) {
        //System.out.println("list = [" + list + "]");
        List<String> listNew = new ArrayList<String>();
        Set set = new HashSet();
        for (String str : list) {
            if (set.add(str)) {
                listNew.add(str);
            }
        }
        return listNew;
    }

    public static String removeFirstLine(String tmp) {
        String result = tmp;
        for (int i = 0; i < tmp.length(); i++) {
            if (tmp.charAt(i) == '\n') {
                result = tmp.substring(i + 1);
                break;
            }
        }
        return result;
    }
}
