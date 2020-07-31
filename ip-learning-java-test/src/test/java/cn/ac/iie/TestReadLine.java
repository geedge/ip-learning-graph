package cn.ac.iie;

import java.io.*;
import java.net.URL;
import java.util.TreeMap;
import java.util.regex.Pattern;

public class TestReadLine {
    public static void main(String[] args) throws Exception {
        Pattern pattern = Pattern.compile("^[^.]*$");
        String encoding = "UTF-8";
//        File file = new File("C:\\Users\\94976\\Desktop\\test.txt");
        URL url = TestReadLine.class.getClassLoader().getResource("topDomain.txt");
        assert url != null;
        File file = new File(url.getFile());
        InputStreamReader read = new InputStreamReader(
                new FileInputStream(file), encoding);
        BufferedReader bufferedReader = new BufferedReader(read);
        String lineTxt;
        int cnt = 0;
        while ((lineTxt = bufferedReader.readLine()) != null){
            if (pattern.matcher(lineTxt).matches()){
                cnt += 1;
                System.out.println(lineTxt);
            }
        }
        System.out.println(cnt);
        System.out.println(url.getFile());
    }
}
