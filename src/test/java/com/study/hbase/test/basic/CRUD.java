package com.study.hbase.test.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * habse 基本CRUD（创建，读取，更新，删除测试）
 * @author 410s
 */
public class CRUD {
   static Connection connection;
   
   static Admin hBaseAdmin;
   static Configuration conf;   

    static {
        conf = HBaseConfiguration.create();
        conf.set("zookeeper.znode.parent","/hbase");
    
        conf.setInt("hbase.rpc.timeout",20000);
        conf.setInt("hbase.client.operation.timeout",30000);
        conf.setInt("hbase.client.scanner.timeout.period",20000);
        try {
			connection=ConnectionFactory.createConnection(conf);
			hBaseAdmin=connection.getAdmin();		
		} catch (IOException e1) {
			e1.printStackTrace();
		}    
    }
	    /**
	     * 创建table 
	     * 首先检查相应的table是否已存在
	     * 如果不存在则创建、如果已存在则直接返回 
	     * @param tableName  表名
	     * @param cFamily   列族名（family）
	     * @throws Exception
	     */
	    public static void createTable(String tableName, String[] cFamilies) throws Exception {	        
	    	if(hBaseAdmin.tableExists(TableName.valueOf(tableName))){
	    		System.out.println("create table failed.the table already exists");
	    		return;
	    	}
	        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
	        for (String cFamily : cFamilies) {
	            HColumnDescriptor column = new HColumnDescriptor(cFamily);
	            hTableDescriptor.addFamily(column);
	        }
	        hBaseAdmin.createTable(hTableDescriptor);
	        System.out.println("create table successed");
	    }

	    /**
	     * 删除table
	     * 首先检查table是否存在
	     * 如果存在则禁用后删除，不果不存在不做任何事情
	     * @param tableName
	     * @throws Exception
	     */
	    public static void dropTable(String tableName) throws Exception {
	        if (hBaseAdmin.tableExists(TableName.valueOf(tableName))) {
	            hBaseAdmin.disableTable(TableName.valueOf(tableName));
	            hBaseAdmin.deleteTable(TableName.valueOf(tableName));
	        }
	        System.out.println("drop table successed");
	    }
	    /*
	     * 得到Table对象
	     */
	    public static Table getHTable(String tableName) throws Exception {
	        return connection.getTable(TableName.valueOf(tableName));
	     }
	    /**
	     * 插入数据
	     * @param tableName
	     * @param map    Map<rowKey,Map<family,Map<qualifier,value>>>  需要插入的数据组织形式
	     * rowKey:行键
	     * family:列族
	     * qualifier:列标识
	     * value:值 
	     * @throws Exception
	     * 
	     */
	    public static void insert(String tableName,Map<String,Map<String,Map<String, String>>> rows) throws Exception  {
	    	
	    	Table hTable=getHTable(tableName);
	    	 
	    	for(Entry<String, Map<String, Map<String, String>>> row:rows.entrySet()){
	    		byte[] rowName=Bytes.toBytes(row.getKey());
	    		Map<String,Map<String,String>> families=row.getValue();
	    		
	    		for(Entry<String,Map<String,String>> family:families.entrySet()){
	    			byte[] familyName=Bytes.toBytes(family.getKey());
	    			Map<String,String> qualifiers=family.getValue();
	    			for(Entry<String,String> qualifier:qualifiers.entrySet()){
	    				byte[] qualifierName=Bytes.toBytes(qualifier.getKey());
	    				byte[] qualifierValue=Bytes.toBytes(qualifier.getValue());
	    				
	    				Put p=new Put(rowName);
	    				p.addColumn(familyName,qualifierName,qualifierValue);
	    				
	    				hTable.put(p);	    				
	    			}    			
	    		}    		
	    	}
	    	hTable.close();  
	    	System.out.println("insert complete");
	    }
	    /**
	     * 查找单行
	     * @param tableName
	     * @param rowKey
	     * @throws Exception
	     */
	    public static void selectOne(String tableName, String rowKey) throws Exception {
	        Table hTable = getHTable(tableName);
	        Get g1 = new Get(Bytes.toBytes(rowKey));
	        Result result = hTable.get(g1);
	        foreach(result);
	        System.out.println("selectOne end");
	        hTable.close();
	    }
	    /**
	     * 遍历查询结果
	     * @param result
	     * @throws Exception
	     */
	    public static void foreach(Result result) throws Exception {
	    	
	    	for(Cell cell:result.listCells()){
	    		StringBuilder sb=new StringBuilder();
	    		/*sb.append(Bytes.toString(cell.getRowArray())).append("\t");
	            sb.append(Bytes.toString(cell.getFamilyArray())).append("\t");
	            sb.append(Bytes.toString(cell.getQualifierArray())).append("\t");
	            sb.append(cell.getTimestamp()).append("\t");
	            sb.append(Bytes.toString(cell.getValueArray())).append("\t");*/
	    		sb.append(Bytes.toString(cell.getRow())).append("\t");
	            sb.append(Bytes.toString(cell.getFamily())).append("\t");
	            sb.append(Bytes.toString(cell.getQualifier())).append("\t");
	            sb.append(cell.getTimestamp()).append("\t");
	            sb.append(Bytes.toString(cell.getValue())).append("\t");	    		
	            System.out.println(sb.toString());
	    	}	     
	    }
	     public static void delete(String tableName, String rowKey) throws Exception {
	        Table hTable = getHTable(tableName);
	        List<Delete> list = new ArrayList<Delete>();
	        Delete d1 = new Delete(Bytes.toBytes(rowKey));
	        list.add(d1);
	        hTable.delete(list);
	        Get g1 = new Get(Bytes.toBytes(rowKey));
	        Result result = hTable.get(g1);
	        System.out.println("Get: " + result);
	        System.out.println("delete successed");
	        hTable.close();
	    } 

	    public static void selectAll(String tableName) throws Exception {
	        Table hTable = getHTable(tableName);
	        Scan scan = new Scan();           
	                
	      
	        ResultScanner resultScanner = null;
	        try {
	            resultScanner = hTable.getScanner(scan);
	            for (Result result : resultScanner) {
	                foreach(result);
	            }
	        }
	        catch (Exception e) {
	            e.printStackTrace();
	        }
	        finally {
	            if (resultScanner != null) {
	                resultScanner.close();
	            }
	        }
	        System.out.println("selectAll end");
	        hTable.close();
	    }
	
	    public static void main(String[] args) throws Exception{
	    	
	    	String tableName="testtable";
	    	String[] cfamily={"C1","C2"};
	    	dropTable(tableName);
	    	createTable(tableName,cfamily);
	    	
	    	//Map<rowKey,Map<family,Map<column,value>>>
	    	
	    	Map<String,String> qualifiers=new HashMap<String,String>();
	    	qualifiers.put("column1", "abc");
	    	qualifiers.put("column2", "def");
	    	Map<String,Map<String,String>> cFamilies=new HashMap<String,Map<String,String>>();
	    	cFamilies.put("C1", qualifiers);
	    	Map<String,Map<String,Map<String,String>>> rows=new HashMap<String,Map<String,Map<String,String>>>();
	    	rows.put("row1", cFamilies);
	    	insert(tableName,rows);	    	
	    	selectOne(tableName,"row1");    	
	    	selectAll(tableName);    	
	    	delete(tableName,"row1");    	
	    	selectAll(tableName);    
	    		
	    }
	
}
