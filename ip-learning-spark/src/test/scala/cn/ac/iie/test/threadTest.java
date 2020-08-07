package cn.ac.iie.test;

public class threadTest implements Runnable {
    private int count = 10;
    @Override
    public /*synchronized*/ void run() {
        count--;
        System.out.println(Thread.currentThread().getName() + " count = " + count);
    }

    public static void main(String[] args) {
        threadTest t = new threadTest();
        for(int i=0; i<5; i++) {
            new Thread(t, "THREAD" + i).start();
        }
    }

}
