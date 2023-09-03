package ecnu.db;

import java.io.File;

public class ListFilesInDirectory {
    public static void main(String[] args) {
        String directoryPath = "C:\\Users\\82084\\Desktop\\学习资料\\HYDRA\\新建文件夹\\2\\test\\sqlqueries"; // 替换成你的目录路径
        listFiles(directoryPath);
    }

    public static void listFiles(String directoryPath) {
        File directory = new File(directoryPath);

        // 首先确保指定的路径是一个目录
        if (!directory.isDirectory()) {
            System.out.println("指定的路径不是一个有效的目录。");
            return;
        }

        // 获取目录下所有文件
        File[] files = directory.listFiles();

        if (files != null) {
            System.out.println("目录 " + directory.getAbsolutePath() + " 中的文件列表:");

            for (File file : files) {
                if (file.isFile()) {
                    System.out.println(file.getName());
                }
            }
        } else {
            System.out.println("无法列出文件，可能是由于权限问题或目录不存在。");
        }
    }
}
