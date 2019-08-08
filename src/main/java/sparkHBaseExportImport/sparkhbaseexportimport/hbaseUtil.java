/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sparkHBaseExportImport.sparkhbaseexportimport;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 *
 * @author msumbul
 * @web https://whatsbigdata.be
 * @git https://github.com/michelsumbul
 */
public class hbaseUtil {

    public static Configuration getHBaseConfig() {
        Configuration config = HBaseConfiguration.create();

        String path = "/usr/hdp/current/hbase-client/conf/hbase-site.xml";
        config.addResource(new Path(path));
        return config;

    }

    public static Admin getHbaseAdmin(Configuration config) {
        try {
            //HBaseAdmin.checkHBaseAvailable(config);
            //For Hbase 1.1.2s
            HBaseAdmin.available(config);
            Connection connection = ConnectionFactory.createConnection(config);
            Admin admin = connection.getAdmin();
            return admin;
        } catch (ZooKeeperConnectionException ex) {
            Logger.getLogger(hbaseUtil.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(hbaseUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

}
