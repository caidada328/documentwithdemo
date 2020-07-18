package com.caicai.gmall.realtime.Text;

 class Singleton23 {
    private Singleton23(){}
    private static Singleton23 single = new Singleton23();

    public static Singleton23 getInstance(){
        return single;
    }

}
