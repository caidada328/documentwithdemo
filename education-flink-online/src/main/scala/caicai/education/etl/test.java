package caicai.education.etl;

import com.caicai.education.util.DataSourceUtil;
import com.jcraft.jsch.jce.MD5;
import org.apache.flink.shaded.curator.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator.org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.flink.shaded.curator.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.io.MD5Hash;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class test {
    public static void main(String[] args) throws Exception {
        System.out.println(System.currentTimeMillis());
    }

    public static String generateHash(String input) {
        try {
            //参数校验
            if (null == input) {
                return null;
            }
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());
            byte[] digest = md.digest();
            BigInteger bi = new BigInteger(1, digest);
            String hashText = bi.toString(16);
            while (hashText.length() < 32) {
                hashText = "0" + hashText;
            }
            return hashText;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
