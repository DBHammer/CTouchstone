package ecnu.db;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class getErrorTouchstone {

    public static void main(String[]args) throws FileNotFoundException {
        {
            int matchTime = 0;
            List<String> strs = new ArrayList<>();
            try
            {
                String encoding = "UTF-8";
                File file = new File("D:\\eclipse-workspace\\Mirage\\conf\\tpchInfo.txt");
                if (file.isFile() && file.exists()){
                    InputStreamReader read = new InputStreamReader(
                            new FileInputStream(file), encoding);
                    BufferedReader bufferedReader = new BufferedReader(read);
                    String lineTxt = null;
                    while ((lineTxt = bufferedReader.readLine()) != null)
                    {
                        matchTime = getMatchTime(matchTime, strs, lineTxt);
                    }
                    read.close();
                }
                else
                {
                    System.out.println("找不到指定的文件");
                }
            }
            catch (Exception e)
            {
                System.out.println("读取文件内容出错");
                e.printStackTrace();
            }
            for (String str : strs) {
                System.out.println(str);
            }
        }
    }

    private static int getMatchTime(int matchTime, List<String> strs, String lineTxt) {
        Pattern p = Pattern.compile("error:0\\.\\d*[1-9]");
        Matcher m = p.matcher(lineTxt);
        boolean result = m.find();
        String find_result = null;
        if (result)
        {
            matchTime++;
            find_result = m.group(0);
            find_result = find_result.split(":")[1];
            strs.add(find_result);
        }
        return matchTime;
    }

}
