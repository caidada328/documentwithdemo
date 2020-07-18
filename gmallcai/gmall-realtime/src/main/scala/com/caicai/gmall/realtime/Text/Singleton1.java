package com.caicai.gmall.realtime.Text;

public class Singleton1 {
private  Singleton1(){}
private static Singleton1 single;

public static Singleton1 getInstance(){
    if(single == null){
        single = new Singleton1();
    }
    return single;

}

}
