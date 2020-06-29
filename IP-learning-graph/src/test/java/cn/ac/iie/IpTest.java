package cn.ac.iie;

import cn.ac.iie.config.ApplicationConfig;
import cn.ac.iie.dao.BaseClickhouseData;
import com.arangodb.entity.BaseDocument;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Pattern;

public class IpTest {
    public static void main(String[] args) throws UnknownHostException {
        /*
        String ipStr = "192.168.40.152";
        Pattern pattern = Pattern.compile("^[\\d]*$");
        String[] split = ipStr.split("\\.");
        for (String s:split){
            System.out.println(s);
            System.out.println(pattern.matcher(s).matches());
        }

//        String ip = "17.57.145.7";
        String ip = "pixel.rubiconproject.com";
//        String ip = "113.200.17.239";
        System.out.println(ip.hashCode());
        int hash = Math.abs(ip.hashCode());
        int i = hash % ApplicationConfig.THREAD_POOL_NUMBER;
        System.out.println(i);
        */
        HashMap<Integer, ArrayList<BaseDocument>> vFqdnMap = new HashMap<>();
        for (int i = 0; i < ApplicationConfig.THREAD_POOL_NUMBER; i++) {
            vFqdnMap.put(i, new ArrayList<>());
        }
        try {
//        ArrayList<BaseDocument> orDefault = vFqdnMap.getOrDefault(-7, new ArrayList<>());
            ArrayList<BaseDocument> orDefault = vFqdnMap.get(-7);
            System.out.println(orDefault.toString());
            Set<Integer> integers = vFqdnMap.keySet();
            for (Integer in:integers){
                System.out.println(in);
            }
        }catch (Exception e){
            e.printStackTrace();
            System.out.println(e.toString());
        }
//        String[] ipArr = ipStr.split("\\.");
//        long ipLong = (Long.valueOf(ipArr[0]) << 24) + (Long.valueOf(ipArr[1]) << 16) + (Long.valueOf(ipArr[2]) << 8) + (Long.valueOf(ipArr[3]));
//        System.out.println(ipLong);
//
//
//        StringBuffer ipBf = new StringBuffer();
//        ipBf.append(ipLong >>> 24).append(".");
//        ipBf.append((ipLong >>> 16) & 0xFF).append(".");
//        ipBf.append((ipLong >>> 8) & 0xFF).append(".");
//        ipBf.append(ipLong & 0xFF);
//        String ip = ipBf.toString();
//        System.out.println(ip);
//
//        System.out.println("---------------");
//        InetAddress byName = InetAddress.getByName("2001:470:19:790::38");
//        byte[] address = byName.getAddress();
//        for (byte b : address) {
//            System.out.println(b & 0xFF);
//        }


    }
}
