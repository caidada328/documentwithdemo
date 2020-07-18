package com.caicai.gmall.realtime.Text;


import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.math.BigInteger;

public class HbaseUtil {
    public static boolean createTable(HBaseAdmin admin, HTableDescriptor table, byte[][] splits)
            throws IOException {
        try {
            admin.createTable(table, splits);
            return true;
        } catch (TableExistsException e) {
            System.out.println("table " + table.getNameAsString() + " already exists");
            // the table already exists...
            return false;
        }
    }

    public static byte[][] getHexSplits(String startKey, String endKey, int numRegions) {
        //start:001,endkey:100,10region [001,010][011,020]
        byte[][] splits = new byte[numRegions-1][];
        BigInteger lowestKey = new BigInteger(startKey, 16);
        BigInteger highestKey = new BigInteger(endKey, 16);
        BigInteger range = highestKey.subtract(lowestKey);
        BigInteger regionIncrement = range.divide(BigInteger.valueOf(numRegions));
        lowestKey = lowestKey.add(regionIncrement);
        for(int i=0; i < numRegions-1;i++) {
            BigInteger key = lowestKey.add(regionIncrement.multiply(BigInteger.valueOf(i)));
            byte[] b = String.format("%016x", key).getBytes();
            splits[i] = b;
        }
        return splits;
    }

   public static void main(String args[]) {
            HbaseUtil hu = new HbaseUtil();
       System.out.println(hu.getHexSplits("001","100",10));


    /* Configuration conf = (Configuration) HBaseConfiguration.create();
        String table_log_name = "user_name";
       int tableN = 10;
       HTable[] wTableLog = new HTable[tableN];
       for (int i = 0; i < tableN; i++) {

           try {
               wTableLog[i] = new HTable((org.apache.hadoop.conf.Configuration) conf, table_log_name);
               wTableLog[i].setWriteBufferSize(5 * 1024 * 1024); //5MB
           } catch (IOException e) {
               e.printStackTrace();
           }
           wTableLog[i].setAutoFlush(false);}
       int ThreadN = 1;
       for(int i=0;i<ThreadN;i++){
           Thread n = new Thread(){
               @Override
               public void run() {
                   while(true) {
                       try {
                           sleep(1000);
                       }catch(Exception e){
                           e.printStackTrace();
                       }
             synchronized (wTableLog[i]){
                           try{
                               wTableLog[i].flushCommits();
                           }
                           catch ( IOException e){
                               e.printStackTrace();
                           }
               }
                   }
               }
           };
       }


   */

    }

}
