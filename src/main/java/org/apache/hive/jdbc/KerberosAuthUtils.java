package org.apache.hive.jdbc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by fastbird on 2018/5/31.
 */
public class KerberosAuthUtils {
    public final static void kerberosLogin(){
        String kbsPropertiesPath = System.getenv("FASTBIRD_KEYTAB_PATH");
        System.out.println("Kerberos properties path:"+kbsPropertiesPath);

        String hadoopConfigPath = System.getenv("HADOOP_CONF");
        System.out.println("hadoop config path:"+hadoopConfigPath);

        Configuration conf = loadConfig(hadoopConfigPath);
        try {
            Properties properties = new Properties();
            properties.load(new FileInputStream(new File(kbsPropertiesPath)));
            String realm = properties.getProperty("java.security.krb5.realm");
            String kdc = properties.getProperty("java.security.krb5.kdc");
            String principal = properties.getProperty("kerberos.principal");
            String keytabPath = properties.getProperty("kerberos.keytab.path");
            System.setProperty("java.security.krb5.realm", realm);
            System.setProperty("java.security.krb5.kdc", kdc);

            conf.setBoolean("hadoop.security.authorization", true);
            //setting auth type kerberos
            conf.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(conf);

            UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
        } catch (IOException ex) {
            ex.printStackTrace();
            System.out.println("-----------Auth Failed.");
        }
        System.out.println("----------Auth Success!");
    }
    private static final Configuration loadConfig(String hadoopConfPath){
        Configuration config=new Configuration();
        File confDir=new File(hadoopConfPath);
        if(confDir==null||!confDir.isDirectory()){
            return config;
        }
        File[] confs=confDir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile()&&pathname.getName().endsWith("xml");
            }
        });
        for(File f:confs) {
            config.addResource(new Path(f.getAbsolutePath()));
        }
        return config;
    }
    public static void main(String[] args)throws Exception{
        kerberosLogin();
    }
}
