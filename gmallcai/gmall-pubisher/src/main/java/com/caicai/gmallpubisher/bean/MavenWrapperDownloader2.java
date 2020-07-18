package com.caicai.gmallpubisher.bean;

import java.io.*;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Properties;

public class MavenWrapperDownloader2 {
    public static final String WRAPPER_VERSION = "0.5.6";

    private static final String DEFAULT_DOWNLOAD_URL = "http://repo.maven.apache.org/maven2/maven-wrapper/" +
            WRAPPER_VERSION + "/maven-wrapper-" + WRAPPER_VERSION + ".jar";

    private static final String MAVEN_WRAPPER_PROPERTIES_PROPERTIES_PATH = ".mvn/wrapper/maven-wrapper.properties";

    private static final String PROPERTY_NAME_WRAPPER_URL = "wrapperUrl";
    private static final String MAVEN_WRAPPER_JAR_PATH =  ".mvn/wrapper/maven-wrapper.jar";


    public static void main(String args[]) {
        System.out.print("- Downloader started");
        File baseDirectory = new File(args[0]);
        System.out.println("- USing base directory :" + baseDirectory.getAbsolutePath());

        File mavenWrapperPropertyFile = new File(baseDirectory, MAVEN_WRAPPER_PROPERTIES_PROPERTIES_PATH);
        String url = DEFAULT_DOWNLOAD_URL;
        if (mavenWrapperPropertyFile.exists()) {
            FileInputStream mavenWrapperPropertyFileInputStream = null;
            try {
                mavenWrapperPropertyFileInputStream = new FileInputStream(mavenWrapperPropertyFile);
                Properties mavenWrapperProperties = new Properties();
                mavenWrapperProperties.load(mavenWrapperPropertyFileInputStream);
                url = mavenWrapperProperties.getProperty(PROPERTY_NAME_WRAPPER_URL,url);

            } catch (Exception e) {
                System.out.println("- ERROR loading '" + MAVEN_WRAPPER_PROPERTIES_PROPERTIES_PATH + "'");
            }finally{
                if(mavenWrapperPropertyFileInputStream != null){
                    try {
                        mavenWrapperPropertyFileInputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("- DOwnloading from :" + url);

            File outputFile = new File(baseDirectory.getAbsolutePath(),MAVEN_WRAPPER_JAR_PATH);
            if(!outputFile.getParentFile().exists()){
                if(!outputFile.getParentFile().mkdirs()){
                    System.out.println(" - ERROR creating output direstory '" + outputFile.getParentFile().getAbsolutePath() + "'");

                }
            }
            System.out.println("- DOwnloading to : " +outputFile.getAbsolutePath());
            try {
                downloadFileFromURL(url, outputFile);
                System.out.println("Done");
                System.exit(0);
            }catch(Throwable e) {
                System.out.println(" - Error downloading");
                System.out.println(1);
            }
        }


    }

    private static void downloadFileFromURL(String urlString, File destination) throws Exception {
        if(System.getenv("MVN_USERNAME") != null && System.getenv("MVN_PASSWORD" ) != null){
            String username = System.getenv("MVN_USERNAME");
            String password = System.getenv("MVN_PASSWORD");
            Authenticator.setDefault(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
               //     return new PasswordAuthentication(username, password);
                    return super.getPasswordAuthentication();
                }
            });
        }

        URL website = new URL(urlString);
        ReadableByteChannel rbc;
        rbc = Channels.newChannel(website.openStream());
        FileOutputStream fos = new FileOutputStream(destination);
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        fos.close();
        rbc.close();


    }
}