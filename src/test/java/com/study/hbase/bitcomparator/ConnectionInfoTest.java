package com.study.hbase.bitcomparator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.study.hbase.bitcomparator.core.RowKeyEqualComparator;
import com.study.hbase.bitcomparator.util.HbaseUtil_back;

import net.sf.json.JSONObject;

public class ConnectionInfoTest {

	private static String tableName = "connectionInfo";
	private static String[] columns = new String[] { "property", "json" };
	private static String fileName = "D:\\Workspace\\eclipse\\hbase.bitcomparator\\resources\\connection.json";
	
	private static HTable table=null;
	static{
		try {
			table=(HTable) HbaseUtil_back.getHTable(tableName);
		} catch (Exception e) {
			System.out.println("create table error");
			System.exit(1);
		}
	}
	
	/**
	 * 第一个版本的数据读取，RowKey为md5url+stime
	 * @return
	 */

	public static List<Map.Entry<String, Map<String, String>>> loadConnections() {
		List<Map.Entry<String, Map<String, String>>> res = new ArrayList<Map.Entry<String, Map<String, String>>>();
		try {
			List<String> lines = FileUtils.readLines(new File(fileName));

			for (String str : lines) {
				Map<String, String> connMap = new HashMap<String, String>();
				JSONObject jobj = JSONObject.fromObject(str);
				for (Object key : jobj.keySet()) {
					connMap.put(key.toString(), jobj.get(key).toString());
				}				
				connMap.put("json", str);
				Map<String, Map<String, String>> con = new HashMap<String, Map<String, String>>();
				con.put(connMap.get("md5url")+connMap.get("stime"), connMap);
				res.addAll(con.entrySet());

			}
			return res;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * 第二个版本的数据读取，RowKey为md5UrlHash+siteCodeHash+sTimeHash 中间会有转为String不是很好。
	 * @return
	 */

	public static List<Map.Entry<String, Map<String, String>>> loadConnections2() {
		List<Map.Entry<String, Map<String, String>>> res = new ArrayList<Map.Entry<String, Map<String, String>>>();
		try {
			List<String> lines = FileUtils.readLines(new File(fileName));

			for (String str : lines) {
				Map<String, String> connMap = new HashMap<String, String>();
				JSONObject jobj = JSONObject.fromObject(str);
				for (Object key : jobj.keySet()) {
					connMap.put(key.toString(), jobj.get(key).toString());
				}				
				connMap.put("json", str);
				Map<String, Map<String, String>> con = new HashMap<String, Map<String, String>>();
				
				int md5UrlHash=connMap.get("md5url").hashCode();
				int siteCodeHash=connMap.get("sitecode").hashCode();
				int sTimeHash=connMap.get("stime").hashCode();
				con.put(Bytes.toString(Bytes.add(Bytes.toBytes(md5UrlHash),Bytes.toBytes(siteCodeHash))), connMap);
				res.addAll(con.entrySet());

			}
			return res;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * 第三个版本  RowKey 直接为哈希值转byte(较第二个版本少了转字符串的过程）
	 * @return
	 */
	public static List<Map.Entry<byte[], Map<String, String>>> loadConnections3() {
		List<Map.Entry<byte[], Map<String, String>>> res = new ArrayList<Map.Entry<byte[], Map<String, String>>>();
		try {
			List<String> lines = FileUtils.readLines(new File(fileName));

			for (String str : lines) {
				Map<String, String> connMap = new HashMap<String, String>();
				JSONObject jobj = JSONObject.fromObject(str);
				for (Object key : jobj.keySet()) {
					connMap.put(key.toString(), jobj.get(key).toString());
				}				
				connMap.put("json", str);
				Map<byte[], Map<String, String>> con = new HashMap<byte[], Map<String, String>>();
				
				int md5UrlHash=connMap.get("md5url").hashCode();
				int siteCodeHash=connMap.get("sitecode").hashCode();
				int sTimeHash=connMap.get("stime").hashCode();
				con.put(Bytes.add(Bytes.toBytes(md5UrlHash),Bytes.toBytes(siteCodeHash),Bytes.toBytes(sTimeHash)), connMap);
				res.addAll(con.entrySet());

			}
			return res;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	

	/**
	 * 第四个版本  测试Rowkey为0
	 * @return
	 */
	public static List<Map.Entry<byte[], Map<String, String>>> loadConnections4() {
		List<Map.Entry<byte[], Map<String, String>>> res = new ArrayList<Map.Entry<byte[], Map<String, String>>>();
		try {
			List<String> lines = FileUtils.readLines(new File(fileName));

			for (String str : lines) {
				Map<String, String> connMap = new HashMap<String, String>();
				JSONObject jobj = JSONObject.fromObject(str);
				for (Object key : jobj.keySet()) {
					connMap.put(key.toString(), jobj.get(key).toString());
				}				
				connMap.put("json", str);
				Map<byte[], Map<String, String>> con = new HashMap<byte[], Map<String, String>>();
				
				int md5UrlHash=0;
				int siteCodeHash=0;
				int sTimeHash=0;
				con.put(Bytes.add(Bytes.toBytes(md5UrlHash),Bytes.toBytes(siteCodeHash),Bytes.toBytes(sTimeHash)), connMap);
				res.addAll(con.entrySet());

			}
			return res;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	/**
	 * 插入数据
	 * 
	 * @param tableName
	 * @param rows
	 * @throws Exception
	 */
	public static void insert(String tableName, List<Map.Entry<String, Map<String, String>>> rows) throws Exception {
		HTable htable = table;
		int count = 0;
		for (Map.Entry<String, Map<String, String>> row : rows) {
			byte[] rowKey = Bytes.toBytes(row.getKey());

			Put p1 = new Put(rowKey);

			Map<String, String> values = row.getValue();

			for (String key : values.keySet()) {
				byte[] value = Bytes.toBytes(values.get(key));
				byte[] family = Bytes.toBytes("property");
				if (key.equals("json")) {
					family = Bytes.toBytes("json");
				}
				byte[] qualifier = Bytes.toBytes(key);
				p1.add(family, qualifier, value);
			}
			htable.put(p1);
			count++;
		}
		System.out.println("insert " + count + " rows");
	}
	
	/**
	 * 插入数据
	 * 
	 * @param tableName
	 * @param rows
	 * @throws Exception
	 */
	public static void insert2(String tableName, List<Map.Entry<byte[], Map<String, String>>> rows) throws Exception {
		HTable htable = table;
		int count = 0;
		for (Map.Entry<byte[], Map<String, String>> row : rows) {
			byte[] rowKey =row.getKey();

			Put p1 = new Put(rowKey);

			Map<String, String> values = row.getValue();

			for (String key : values.keySet()) {
				byte[] value = Bytes.toBytes(values.get(key));
				byte[] family = Bytes.toBytes("property");
				if (key.equals("json")) {
					family = Bytes.toBytes("json");
				}
				byte[] qualifier = Bytes.toBytes(key);
				p1.add(family, qualifier, value);
			}
			htable.put(p1);
			count++;
		}
		System.out.println("insert " + count + " rows");
	}
	//使用RowFilter过滤器
	public static List<String> findRowKeysByProperty(String property,String value) throws Exception{
		List<String>res=new ArrayList<String>();
		HTable hTable = table;
		Scan scan = new Scan();		
		

		List<Filter> filterList = new ArrayList<Filter>();

		Filter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("property")));

		Filter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(property)));
		
		Filter valueFilter=new ValueFilter(CompareFilter.CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(value+"")));
		
		filterList.add(familyFilter);
		filterList.add(qualifierFilter);
		filterList.add(valueFilter);
		
		FilterList fls=new FilterList(filterList);
		
		scan.setFilter(fls);

		ResultScanner resultScanner = null;
		try {
			resultScanner = hTable.getScanner(scan);
			for (Result result : resultScanner) {
				res.add(Bytes.toString(result.getRow()));		
				
				   
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (resultScanner != null) {
				resultScanner.close();
			}			
		}
		return res;
		
	}
	
	public static List<String> getJsonByRowKeys(List<String> rowKeys) throws IOException{
		List<String> res=new ArrayList<String>();
		for(String rowKey:rowKeys){
			Get get=new Get(Bytes.toBytes(rowKey));
			get.addColumn(Bytes.toBytes("json"),Bytes.toBytes("json"));
			Result detail = table.get(get);
			res.add(Bytes.toString(detail.getValue(Bytes.toBytes("json"),Bytes.toBytes("json"))));		
		}
		return res;	  
	}
	
	public static void printAll() throws Exception{
		HbaseUtil_back.selectAll(tableName);
	}
	/**
	 * RowKey为md5UrlHash+siteCodeHash+sTimeHas 转字符串
	 * @throws IOException 
	 */
	public static void find1() throws IOException{
		int md5UrlHash="ea67a96f233d6fcfd7cabc9a6a389283".hashCode();				
		int siteCodeHash="1509250008".hashCode();
		int sTimeHash="1481272834722".hashCode();
		
		Get get=new Get(Bytes.toBytes(Bytes.toString(Bytes.add(Bytes.toBytes(md5UrlHash),Bytes.toBytes(siteCodeHash)))));
		get.addColumn(Bytes.toBytes("json"),Bytes.toBytes("json"));
		Result detail = table.get(get);
		System.out.println(Bytes.toString(detail.getValue(Bytes.toBytes("json"),Bytes.toBytes("json"))));	
		//con.put(Bytes.toString(Bytes.add(Bytes.toBytes(md5UrlHash),Bytes.toBytes(siteCodeHash))), connMap);
		//res.addAll(con.entrySet());
	}

	//直接要据int值转为byte后查找
	public static void find2() throws IOException{
		int md5UrlHash="ea67a96f233d6fcfd7cabc9a6a389283".hashCode();				
		int siteCodeHash="1509250008".hashCode();
		int sTimeHash="1481272834722".hashCode();		
		Get get=new Get(Bytes.add(Bytes.toBytes(md5UrlHash),Bytes.toBytes(siteCodeHash),Bytes.toBytes(sTimeHash)));
		get.addColumn(Bytes.toBytes("json"),Bytes.toBytes("json"));
		Result detail = table.get(get);
		System.out.println(Bytes.toString(detail.getValue(Bytes.toBytes("json"),Bytes.toBytes("json"))));
	}
	
	public static void testIntToByteLen(){
		int md5UrlHash=0;				
		int siteCodeHash=0;
		int sTimeHash=0;
		byte[] bs=Bytes.add(Bytes.toBytes(md5UrlHash),Bytes.toBytes(siteCodeHash),Bytes.toBytes(sTimeHash));
		System.out.println(bs.length);
	}
	
	//查找全0 rowkey
	public static void find3() throws IOException{
		int md5UrlHash=0;				
		int siteCodeHash=0;
		int sTimeHash=0;
		
		Get get=new Get(Bytes.add(Bytes.toBytes(md5UrlHash),Bytes.toBytes(siteCodeHash),Bytes.toBytes(sTimeHash)));
		get.addColumn(Bytes.toBytes("json"),Bytes.toBytes("json"));
		Result detail = table.get(get);
		System.out.println(Bytes.toString(detail.getValue(Bytes.toBytes("json"),Bytes.toBytes("json"))));	

	}

	public static void test() throws Exception {
		long start=System.currentTimeMillis();
		
		List<String> rowKey1=findRowKeysByProperty("sitecode","1101010059");
		List<String> rowKey2=findRowKeysByProperty("sdate","20161209");
		List<String> rowKey3=findRowKeysByProperty("md5url","00a18048ed95f1c057fccc8928ddf610");
		
		rowKey1.retainAll(rowKey2);
		rowKey1.retainAll(rowKey3);
		
		for(String str:getJsonByRowKeys(rowKey1)){
			System.out.println(str);			
		}
		long end=System.currentTimeMillis();
		System.out.println(end-start);
	}
	
	//rowkey 由hashcode转字符串再转byte
	public static void test1() throws Exception{
		
		 HbaseUtil_back.createTable(tableName, columns);
		 insert(tableName,loadConnections2());
		 printAll();
		 long start=System.currentTimeMillis();	
		 find1();
		 long end=System.currentTimeMillis();
		 System.out.println(end-start);
	}
	
	//rowkey 由hashcode直接转byte
	public static void test2() throws Exception{
		 HbaseUtil_back.createTable(tableName, columns);
		 insert2(tableName,loadConnections3());
		 printAll();
		 long start=System.currentTimeMillis();	
		 find2();
		 long end=System.currentTimeMillis();
		 System.out.println(end-start);
	}
	//rowkey 值为0
	public static void testZeroKey() throws Exception{
		 HbaseUtil_back.createTable(tableName, columns);
		 insert2(tableName,loadConnections4());
		 printAll();
		 long start=System.currentTimeMillis();	
		 find3();
		 long end=System.currentTimeMillis();
	}
	
	//使用 自定义的比较器
	
	public static void testComparator() throws Exception{
		
//		 HbaseUtil.createTable(tableName, columns);
//		 insert2(tableName,loadConnections3());
//		 printAll();
		
		
		Scan scan = new Scan();	
		List<Filter> filterList = new ArrayList<Filter>();		
		int md5UrlHash=0;//"ea67a96f233d6fcfd7cabc9a6a389283".hashCode();				
		int siteCodeHash="1509250008".hashCode();
		int sTimeHash=0;//"1481272834722".hashCode();		

		Filter rowFilter=new RowFilter(CompareFilter.CompareOp.EQUAL,new RowKeyEqualComparator(Bytes.add(Bytes.toBytes(md5UrlHash),Bytes.toBytes(siteCodeHash),Bytes.toBytes(sTimeHash))));
		
		
		filterList.add(rowFilter);
	
		FilterList fls=new FilterList(filterList);
		
		scan.setFilter(fls);

		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);			
			for (Result result : resultScanner) {
				// HbaseUtil.foreach(result);
				System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("json"),Bytes.toBytes("json"))));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (resultScanner != null) {
				resultScanner.close();
			}			
		}
	}	
	/*
	 * 测试ConnectionRowKeyBitComparator 比较方法是否生效
	 */
	
	public static void testConnectionRowKeyBitComparator(){
		int md5UrlHash="ea67a96f233d6fcfd7cabc9a6a389283".hashCode();				
		int siteCodeHash="1509250008".hashCode();
		int sTimeHash="1481272834722".hashCode();	
		RowKeyEqualComparator crkbc=new RowKeyEqualComparator(Bytes.add(Bytes.toBytes(0),Bytes.toBytes(siteCodeHash),Bytes.toBytes(0)));
		
		int k=crkbc.compareTo(Bytes.add(Bytes.toBytes(md5UrlHash),Bytes.toBytes(0),Bytes.toBytes(sTimeHash)), 0, 12);
		
		System.out.println(k);
		
	}
	
	public static void main(String[] args) throws Exception {

		//testConnectionRowKeyBitComparator();
		testComparator() ;
		/*byte a=(byte) 0xfd;
		byte b=(byte) 0xff;
		byte c=(byte) (a^b);
		System.out.println(c);*/
		
		
	}

}
