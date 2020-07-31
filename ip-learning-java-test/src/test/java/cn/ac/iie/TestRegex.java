package cn.ac.iie;

import java.util.regex.Pattern;

public class TestRegex {
    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("^[^.]*$");
        System.out.println(pattern.matcher("com.dz").matches());
    }
}
