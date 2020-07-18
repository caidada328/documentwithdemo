package com.caicai.gmall.realtime.Text;


import com.alibaba.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;

import javax.xml.transform.Result;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class DataReaderServer {
    //获取店铺一天内各分钟PV值的入口函数
    public ConcurrentHashMap<String, String> getUnitMinutePV(long uid, long startStamp, long endStamp) {
        long min = startStamp;
        int count = (int) ((endStamp - startStamp) / (60 * 1000));
        List<String> lst = new ArrayList<String>();
        for (int i = 0; i <= count; i++) {
            min = startStamp + i * 60 * 1000;
            lst.add(uid + "_" + min);
        }
        return parallelBatchMinutePV(lst);
    }
    //多线程并发查询，获取分钟PV值

    private ConcurrentHashMap<String, String> parallelBatchMinutePV(List<String> lstKeys) {
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

        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("ParallelBatchQuery");
        ThreadFactory factory = builder.build();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(lstBatchKeys.size(), factory);

        for (List<String> keys : lstBatchKeys) {
            Callable<ConcurrentHashMap<String, String>> callable = new BatchMinutePVCallable(keys);
            FutureTask<ConcurrentHashMap<String, String>> future = (FutureTask<ConcurrentHashMap<String, String>>) executor.submit(callable);
            futures.add(future);
        }
        executor.shutdown();

        // Wait for all the tasks to finish
        try {
            boolean stillRunning = !executor.awaitTermination(
                    5000000, TimeUnit.MILLISECONDS);
            if (stillRunning) {
                try {
                    executor.shutdownNow();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        } catch (InterruptedException e) {
            try {
                Thread.currentThread().interrupt();
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }

        // Look for any exception
        for (Future f : futures) {
            try {
                if (f.get() != null) {
                    hashRet.putAll((ConcurrentHashMap<String, String>) f.get());
                }
            } catch (InterruptedException e) {
                try {
                    Thread.currentThread().interrupt();
                } catch (Exception e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        return hashRet;
    }

    //一个线程批量查询，获取分钟PV值
//    protected static ConcurrentHashMap<String, String> getBatchMinutePV(List<String> lstKeys) {
////      // int version =   HColumnDescriptor.DEFAULT_MIN_VERSIONS + 1;
////        ConcurrentHashMap<String, String> hashRet = null;
////        List<Get> lstGet = new ArrayList<Get>();
////        String[] splitValue = null;
////        for (String s : lstKeys) {
////            splitValue = s.split("_");
////            long uid = Long.parseLong(splitValue[0]);
////            long min = Long.parseLong(splitValue[1]);
////            byte[] key = new byte[16];
////            Bytes.putLong(key, 0, uid);
////            Bytes.putLong(key, 8, min);
////            Get g = new Get(key);
////            g.addFamily(fp);
////            lstGet.add(g);
////        }
////        Result[] res = null;
////        try {
////            res = tableMinutePV[rand.nextInt(tableN)].get(lstGet);
////        } catch (IOException e1) {
////            logger.error("tableMinutePV exception, e=" + e1.getStackTrace());
////        }
////
////        if (res != null && res.length > 0) {
////            hashRet = new ConcurrentHashMap<String, String>(res.length);
////            for (Result re : res) {
////                if (re != null && !re.isEmpty()) {
////                    try {
////                        byte[] key = re.getRow();
////                        byte[] value = re.getValue(fp, cp);
////                        if (key != null && value != null) {
////                            hashRet.put(String.valueOf(Bytes.toLong(key,
////                                    Bytes.SIZEOF_LONG)), String.valueOf(Bytes
////                                    .toLong(value)));
////                        }
////                    } catch (Exception e2) {
////                        logger.error(e2.getStackTrace());
////                    }
////                }
////            }
////        }
////
////        return hashRet;
////    }

    class BatchMinutePVCallable implements Callable<ConcurrentHashMap<String, String>> {
        private List<String> keys;

        public BatchMinutePVCallable(List<String> lstKeys) {
            this.keys = lstKeys;
        }

        public ConcurrentHashMap<String, String> call() throws Exception {
            //return DataReadServer.getBatchMinutePV(keys);
            return null;
        }
    }
}