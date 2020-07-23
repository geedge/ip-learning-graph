package cn.ac.iie;

import cn.ac.iie.service.update.vertex.Fqdn;
import cn.ac.iie.utils.ArangoDBConnect;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class TestReflect {
    public static void main(String[] args) throws Exception {
        Class<Fqdn> fqdnClass = Fqdn.class;
//        for(Constructor constructor : fqdnClass.getConstructors()){
//            System.out.println(constructor);
//        }
        Constructor<Fqdn> constructor = fqdnClass.getConstructor(HashMap.class, ArangoDBConnect.class, String.class, ConcurrentHashMap.class, CountDownLatch.class);
        System.out.println(constructor);
    }
}
