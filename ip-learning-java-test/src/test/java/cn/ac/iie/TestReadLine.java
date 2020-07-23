package cn.ac.iie;

import java.io.*;

public class TestReadLine {
    public static void main(String[] args) throws Exception {
        String encoding = "UTF-8";
        File file = new File("C:\\Users\\94976\\Desktop\\test.txt");
        InputStreamReader read = new InputStreamReader(
                new FileInputStream(file), encoding);
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt = null;
        long sum = 0L;
        while ((lineTxt = bufferedReader.readLine()) != null){
            long num = Long.parseLong(lineTxt);
            sum = sum + num;
        }
        System.out.println(sum);
    }
}
