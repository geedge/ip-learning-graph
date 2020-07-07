package cn.ac.iie.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

public class TopDomainUtils {
    private static Logger logger = LoggerFactory.getLogger(TopDomainUtils.class);


    public static String getSecDomain(String urlDomain, HashMap<String, HashMap<String, String>> maps) {
        String[] split = urlDomain.split("\\.");
        String secDomain = null;
        for (int i = split.length - 1; i >= 0; i--) {
            int maps_index = split.length - (i + 1);
            HashMap<String, String> innerMap = maps.get("map_id_" + maps_index);
            HashMap<String, String> fullTop = maps.get("full");
            if (!(innerMap.containsKey(split[i]))) {
                String strSec = "";
                for (int j = i; j < split.length; j++) {
                    strSec += (split[j] + ".");
                }
                secDomain = strSec.substring(0, strSec.length() - 1);
                if (fullTop.containsKey(getTopDomainFromSecDomain(secDomain))) {
                    break;
                } else {
                    while (!fullTop.containsKey(getTopDomainFromSecDomain(secDomain)) && getTopDomainFromSecDomain(secDomain).contains(".")) {
                        secDomain = getTopDomainFromSecDomain(secDomain);
                    }
                    break;
                }
            }
        }
        return secDomain;
    }

    private static String getTopDomainFromSecDomain(String secDomain) {
        String quFirstDian = secDomain;
        if (secDomain.contains(".")) {
            quFirstDian = secDomain.substring(secDomain.indexOf(".")).substring(1);
        }
        return quFirstDian;
    }

    public static HashMap<String, HashMap<String, String>> readTopDomainFile(String filePath) {
        HashMap<String, HashMap<String, String>> maps = makeHashMap(filePath);
        try {
            String encoding = "UTF-8";
            File file = new File(filePath);
            if (file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    HashMap<String, String> fullTop = maps.get("full");
                    fullTop.put(lineTxt, lineTxt);
                    maps.put("full", fullTop);
                    String[] split = lineTxt.split("\\.");
                    for (int i = split.length - 1; i >= 0; i--) {
                        int maps_index = split.length - (i + 1);
                        HashMap<String, String> innerMap = maps.get("map_id_" + maps_index);
                        innerMap.put(split[i], split[i]);
                        maps.put("map_id_" + maps_index, innerMap);
                    }
                }
                read.close();
            } else {
                logger.error("TopDomainUtils>=>readTopDomainFile filePath is wrong--->{" + filePath + "}<---");
            }
        } catch (Exception e) {
            logger.error("TopDomainUtils>=>readTopDomainFile get filePathData error--->{" + e + "}<---");
            e.printStackTrace();
        }
        return maps;
    }

    private static int getMaxLength(String filePath) {
        int lengthDomain = 0;
        try {
            String encoding = "UTF-8";
            File file = new File(filePath);
            if (file.isFile() && file.exists()) { //判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);//考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    String[] split = lineTxt.split("\\.");
                    if (split.length > lengthDomain) {
                        lengthDomain = split.length;
                    }
                }
                read.close();
            } else {
                logger.error("TopDomainUtils>>getMaxLength filePath is wrong--->{" + filePath + "}<---");
            }
        } catch (Exception e) {
            logger.error("TopDomainUtils>=>getMaxLength get filePathData error--->{" + e + "}<---");
            e.printStackTrace();
        }
        return lengthDomain;
    }

    private static HashMap<String, HashMap<String, String>> makeHashMap(String filePath) {
        int maxLength = getMaxLength(filePath);
        HashMap<String, HashMap<String, String>> maps = new HashMap<String, HashMap<String, String>>();
        for (int i = 0; i < maxLength; i++) {
            maps.put("map_id_" + i, new HashMap<String, String>());
        }
        maps.put("full", new HashMap<String, String>());
        return maps;
    }

    //通用方法,传入url,返回domain,这里的domain不包含端口号,含有:一定是v6
    public static String getDomainFromUrl(String oriUrl) {
        //先按照?切分,排除后续干扰
        //后续操作不再涉及?号,排除http://在?后的情况
        String url = oriUrl.split("[?]")[0];
        //获取file_path与domain
        if (url.contains("http://") || url.contains("https://")) {
            //包含http://或者https://时
            //获取domain
            if (url.split("//")[1].split("/")[0].split(":").length <= 2) {
                //按照:切分后最终长度为1或2,说明是v4
                String v4Domain = url.split("//")[1]//按照//切分,索引1包含domain
                        .split("/")[0]//按照/切分,索引0包含domain
                        .split(":")[0];//v4按照:切分去除domain上的端口号后,索引0为最终域名
                return v4Domain;
            } else {
                //按照:切分后长度>2,说明是v6地址,v6地址不包含端口号(暂定),只需要先切分//再切分/
                String v6Domain = url.split("//")[1]//按照//切分,索引1包含domain
                        .split("/")[0];//v6按照/切分后索引0就是domain
                return v6Domain;
            }
        } else {
            //无http://或者https://
            //获取domain
            if (url.split("/")[0].split(":").length <= 2) {
                //按照:切分后长度为1或2,说明为v4
                //无http://时直接按照/切分,索引0包含域名domain,再按照":"切分,0索引就是domain
                String v4Domain = url.split("/")[0].split(":")[0];
                return v4Domain;
            } else {
                //按照:切分后长度>2,说明为v6,v6地址不包含端口号(暂定),只需要切分/取索引0
                String v6Domain = url.split("/")[0];
                return v6Domain;
            }
        }
    }
}
