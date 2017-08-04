package com.demo.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.google.protobuf.ServiceException;

// Connecting to Kerberos secured HBase cluster
// Hortonworks - HBase 1.1.2.2.4.2.11-1 and Hadoop 2.7.3
public class HBaseClient {
	public static void main(String[] args) throws IOException, ServiceException {
		Logger.getRootLogger().setLevel(Level.DEBUG);
		Configuration configuration = HBaseConfiguration.create();
		
		configuration.set("hbase.zookeeper.quorum", "node01.hortonworks.com,node02.hortonworks.com,node03.hortonworks.com");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hadoop.security.authentication", "kerberos");
		configuration.set("hbase.security.authentication", "kerberos");
		configuration.set("hbase.cluster.distributed", "true");
		
		// check this setting on HBase side
		configuration.set("hbase.rpc.protection", "authentication"); 

		//what principal the master/region. servers use.
		configuration.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@FIELD.HORTONWORKS.COM"); 
		configuration.set("hbase.regionserver.keytab.file", "src/hbase.service.keytab"); 
		
		// this is needed even if you connect over rpc/zookeeper
		configuration.set("hbase.master.kerberos.principal", "hbase/_HOST@FIELD.HORTONWORKS.COM"); 
		configuration.set("hbase.master.keytab.file", "src/hbase.service.keytab");
		
		System.setProperty("java.security.krb5.conf","src/krb5.conf");
		// Enable/disable krb5 debugging 
		System.setProperty("sun.security.krb5.debug", "false");

		String principal = System.getProperty("kerberosPrincipal","hbase/hdp1.field.hortonworks.com@FIELD.HORTONWORKS.COM");
		String keytabLocation = System.getProperty("kerberosKeytab","src/hbase.service.keytab");

		// kinit with principal and keytab
		UserGroupInformation.setConfiguration(configuration);
		UserGroupInformation.loginUserFromKeytab(principal, keytabLocation);
		
		Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(configuration));
		System.out.println(connection.getAdmin().isTableAvailable(TableName.valueOf("atlas_titan")));
	}
}
