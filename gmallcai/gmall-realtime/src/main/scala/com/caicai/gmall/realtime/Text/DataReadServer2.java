package com.caicai.gmall.realtime.Text;


import com.alibaba.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class DataReadServer2 {
    /**
     * @param
     * @Author: CaiDeQing
     * @Description:
     * @Date:Created in 2020/6/5 15:08
     * @Modified By:
     */
    public ConcurrentHashMap<String, String> getUnitMinutrPV(long uid, long startStamp, long endStamp) {
        long min = startStamp;
        int count = (int) ((endStamp - startStamp) / (60 * 1000));
        List<String> lst = new ArrayList<String>();
        for (int i = 0; i <= count; i++) {
            min = startStamp + i * 60 * 1000;
            lst.add(uid + "_" + min);
        }
        return parallelBatchMinutesPV(lst);
    }

    private ConcurrentHashMap<String, String> parallelBatchMinutesPV(List<String> lstKeys) {
        ConcurrentHashMap<String, String> hashRet = new ConcurrentHashMap<String, String>();
        int parallel = 3;
        List<List<String>> lstBatchKeys = null;
        if (lstKeys.size() < parallel) {
            lstBatchKeys = new ArrayList<List<String>>(1);
            lstBatchKeys.add(lstKeys);
        } else {
            lstBatchKeys = new ArrayList<List<String>>(parallel);
            for (int i = 0; i < parallel; i++) {
                List<String> lst = new ArrayList<String>();
                lstBatchKeys.add(lst);
            }

            for (int i = 0; i < lstKeys.size(); i++) {
                lstBatchKeys.get(i % parallel).add(lstKeys.get(i));
            }
        }
        List<Future<ConcurrentHashMap<String, String>>> futures = new ArrayList<Future<ConcurrentHashMap<String, String>>>(5);

        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("ParallelBatchQuery").build();
        ThreadPoolExecutor excutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(lstBatchKeys.size(), factory);

        for (List<String> keys : lstBatchKeys) {
            Callable<ConcurrentHashMap<String, String>> callable = new BatchMinutePVCallable(keys);
            FutureTask<ConcurrentHashMap<String, String>> future = (FutureTask<ConcurrentHashMap<String, String>>) excutor.submit(callable);
            futures.add(future);
        }
        excutor.shutdown();


        try{
        boolean stillRunning = !excutor.awaitTermination(5000000, TimeUnit.MILLISECONDS);
        if (stillRunning) {
            try {
                excutor.shutdown();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    } catch (InterruptedException e){
        try{
            Thread.currentThread().interrupt();
        }catch (Exception e1){
            e1.printStackTrace();
        }
    }
    for(Future f : futures){
        try{
            if(f.get() != null){
                hashRet.putAll((ConcurrentHashMap<String,String>)f.get());
            }
        }catch(Exception e) {
            try {
                Thread.currentThread().interrupt();
            }catch(Exception e2){
                e.printStackTrace();
            }
        }
        }
        return hashRet;
    }



class BatchMinutePVCallable implements Callable<ConcurrentHashMap<String,String>>{
    private List<String> keys;
    public BatchMinutePVCallable(List<String> lstKeys)  {
        this.keys = lstKeys;
    }
    @Override
    public ConcurrentHashMap<String, String> call() throws Exception {
        return null;
    }
}





}
