package cn.ac.iie.utils;

import cn.ac.iie.config.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.HashMap;

public class TopDomainUtils {
    private static Logger logger = LoggerFactory.getLogger(TopDomainUtils.class);


    public static String getSecDomain(String urlDomain, HashMap<String, HashMap<String, String>> maps) {
        String[] split = urlDomain.split("\\.");
        String secDomain = null;
        for (int i = split.length - 1; i >= 0; i--) {
            int mapsIndex = split.length - (i + 1);
            HashMap<String, String> innerMap = maps.get("map_id_" + mapsIndex);
            HashMap<String, String> fullTop = maps.get("full");
            if (!(innerMap.containsKey(split[i]))) {
                StringBuilder strSec = new StringBuilder();
                for (int j = i; j < split.length; j++) {
                    strSec.append(split[j]).append(".");
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

    private static File getTopDomainFile(){
        URL url = TopDomainUtils.class.getClassLoader().getResource(ApplicationConfig.TOP_DOMAIN_FILE_NAME);
        File file = null;
        if (url!=null){
            file = new File(url.getFile());
        }
        if (file != null && file.isFile() && file.exists()){
            return file;
        }
        return null;
    }

    public static HashMap<String, HashMap<String, String>> readTopDomainFile() {
        URL url = TopDomainUtils.class.getClassLoader().getResource(ApplicationConfig.TOP_DOMAIN_FILE_NAME);
        assert url != null;
        HashMap<String, HashMap<String, String>> maps = makeHashMap(url.getFile());
        try {
            String encoding = "UTF-8";
            File file = new File(url.getFile());
            if (file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    HashMap<String, String> fullTop = maps.get("full");
                    fullTop.put(lineTxt, lineTxt);
                    maps.put("full", fullTop);
                    String[] split = lineTxt.split("\\.");
                    for (int i = split.length - 1; i >= 0; i--) {
                        int mapsIndex = split.length - (i + 1);
                        HashMap<String, String> innerMap = maps.get("map_id_" + mapsIndex);
                        innerMap.put(split[i], split[i]);
                        maps.put("map_id_" + mapsIndex, innerMap);
                    }
                }
                read.close();
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
            if (file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt;
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
        HashMap<String, HashMap<String, String>> maps = new HashMap<>();
        for (int i = 0; i < maxLength; i++) {
            maps.put("map_id_" + i, new HashMap<String, String>());
        }
        maps.put("full", new HashMap<String, String>());
        return maps;
    }

    /**
     * 通用方法,传入url,返回domain,这里的domain不包含端口号,含有:一定是v6
     * @param oriUrl
     * @return
     */
    public static String getDomainFromUrl(String oriUrl) {
        //先按照?切分,排除后续干扰
        String url = oriUrl.split("[?]")[0];
        //排除http://或https://干扰
        url = url.replaceAll("https://", "").replaceAll("http://", "");
        String domain;

        //获取domain
        if (url.split("/")[0].split(":").length <= 2) {
            //按照:切分后最终长度为1或2,说明是v4
            domain = url
                    //按照/切分,索引0包含domain
                    .split("/")[0]
                    //v4按照:切分去除domain上的端口号后,索引0为最终域名
                    .split(":")[0];
        } else {
            //按照:切分后长度>2,说明是v6地址,v6地址不包含端口号(暂定),只需要先切分//再切分/
            domain = url.split("/")[0];
        }
        return domain;

    }
}
