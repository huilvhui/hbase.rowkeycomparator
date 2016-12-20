package com.study.hbase.bitcomparator.main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.study.hbase.bitcomparator.core.RowKeyEqualComparator;
import com.study.hbase.bitcomparator.core.RowKeyGLComparator;
import com.study.hbase.bitcomparator.util.HbaseUtil;

import net.sf.json.JSONObject;

public class ConnectionInfo {
	

	private static String tableName = "connectionInfo";
	private static String[] families = new String[] { "property" };
	private static String fileName = "D:\\Workspace\\eclipse\\hbase.bitcomparator\\resources\\connection.json";
	
	
	public static void main(String[] args) throws Exception{
		//HbaseUtil.dropTable(tableName);
		//HbaseUtil.createTable(tableName, families);		
		//HbaseUtil.insert(tableName, loadConnections());
		
		testEqualSelect();
		
		testGLSelect();
		
		testAll();
	}
	
	/**
	 * 精确查找测试
	 * 
	 * 查找md5url=00a18048ed95f1c057fccc8928ddf610,siteCode=1101010059,sdate=20161209 的数据
	 * 
	 * @throws Exception
	 */		
	public static void testEqualSelect() throws Exception{
		Table table=HbaseUtil.getHTable(tableName);
		
		Scan scan = new Scan();	
		List<Filter> filterList = new ArrayList<Filter>();		
		int md5UrlHash="00a18048ed95f1c057fccc8928ddf610".hashCode();				
		int siteCodeHash="1101010059".hashCode();
		int sdate=20161209;
		
		int status=0;//0;
		int code=0;//0;		
		int type=0;
		int free=0;
		int close=0;
		int queue=0;
		int scantype=0;
		
		byte[][] bs={
				Bytes.toBytes(md5UrlHash),
				Bytes.toBytes(siteCodeHash),
				Bytes.toBytes(status),
				Bytes.toBytes(code),
				Bytes.toBytes(sdate),
				Bytes.toBytes(type),
				Bytes.toBytes(free),
				Bytes.toBytes(close),
				Bytes.toBytes(queue),
				Bytes.toBytes(scantype)				
		};
		

		Filter rowFilter=new RowFilter(CompareFilter.CompareOp.EQUAL,new RowKeyEqualComparator(Bytes.add(bs)));
		
		
		filterList.add(rowFilter);
	
		FilterList fls=new FilterList(filterList);
		
		scan.setFilter(fls);

		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);			
			for (Result result : resultScanner) {				
				System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("property"),Bytes.toBytes("json"))));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (resultScanner != null) {
				resultScanner.close();
			}			
		}
	}	
	/**
	 * 测试范围查找 找查日期大于等于20161209  code小于502 queue小于1的数据
	 * @throws Exception  
	 */
	public static void testGLSelect() throws Exception{
		Table table=HbaseUtil.getHTable(tableName);
		
		Scan scan = new Scan();	
		List<Filter> filterList = new ArrayList<Filter>();		
		int md5UrlHash=0;				
		int siteCodeHash=0;
		int sdate=0;		
		int status=0;
		int code=0;		
		int type=0;
		int free=0;
		int close=0;
		int queue=0;
		int scantype=0;
		
		byte[][] bs={
				Bytes.toBytes(md5UrlHash),
				Bytes.toBytes(siteCodeHash),
				Bytes.toBytes(status),
				Bytes.toBytes(code),
				Bytes.toBytes(20161209),
				Bytes.toBytes(type),
				Bytes.toBytes(free),
				Bytes.toBytes(close),
				Bytes.toBytes(queue),
				Bytes.toBytes(scantype)				
		};
		
		byte[][] bs2={
				Bytes.toBytes(md5UrlHash),
				Bytes.toBytes(siteCodeHash),
				Bytes.toBytes(status),
				Bytes.toBytes(502),
				Bytes.toBytes(sdate),
				Bytes.toBytes(type),
				Bytes.toBytes(free),
				Bytes.toBytes(close),
				Bytes.toBytes(queue),
				Bytes.toBytes(scantype)				
		};
		
		byte[][] bs3={
				Bytes.toBytes(md5UrlHash),
				Bytes.toBytes(siteCodeHash),
				Bytes.toBytes(status),
				Bytes.toBytes(code),
				Bytes.toBytes(sdate),
				Bytes.toBytes(type),
				Bytes.toBytes(free),
				Bytes.toBytes(close),
				Bytes.toBytes(1),
				Bytes.toBytes(scantype)				
		};
		
		

		Filter rowFilter=new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,new RowKeyGLComparator(Bytes.add(bs)));
		
		Filter rowFilter2=new RowFilter(CompareFilter.CompareOp.LESS,new RowKeyGLComparator(Bytes.add(bs2)));
		Filter rowFilter3=new RowFilter(CompareFilter.CompareOp.LESS,new RowKeyGLComparator(Bytes.add(bs3)));
		
		filterList.add(rowFilter);
		filterList.add(rowFilter2);
		filterList.add(rowFilter3);
	
		FilterList fls=new FilterList(filterList);
		
		scan.setFilter(fls);

		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);			
			for (Result result : resultScanner) {				
				System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("property"),Bytes.toBytes("json"))));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (resultScanner != null) {
				resultScanner.close();
			}			
		}
	}
	/**
	 * 两种查找方式相结合 查找md5url=ea67a96f233d6fcfd7cabc9a6a389283  sdate>=20161209  cod<502 的数据
	 * @throws Exception 
	 */
	public static void testAll() throws Exception{
		Table table=HbaseUtil.getHTable(tableName);
		
		Scan scan = new Scan();	
		List<Filter> filterList = new ArrayList<Filter>();		
		int md5UrlHash=0;
		int siteCodeHash=0;
		int sdate=0;
		
		int status=0;
		int code=0;		
		int type=0;
		int free=0;
		int close=0;
		int queue=0;
		int scantype=0;
		
		byte[][] bs={
				Bytes.toBytes("ea67a96f233d6fcfd7cabc9a6a389283".hashCode()),
				Bytes.toBytes(siteCodeHash),
				Bytes.toBytes(status),
				Bytes.toBytes(code),
				Bytes.toBytes(sdate),
				Bytes.toBytes(type),
				Bytes.toBytes(free),
				Bytes.toBytes(close),
				Bytes.toBytes(queue),
				Bytes.toBytes(scantype)				
		};
		
		byte[][] bs2={
				Bytes.toBytes(md5UrlHash),
				Bytes.toBytes(siteCodeHash),
				Bytes.toBytes(status),
				Bytes.toBytes(code),
				Bytes.toBytes(20161209),
				Bytes.toBytes(type),
				Bytes.toBytes(free),
				Bytes.toBytes(close),
				Bytes.toBytes(queue),
				Bytes.toBytes(scantype)				
		};
		
	
		byte[][] bs3={
				Bytes.toBytes(md5UrlHash),
				Bytes.toBytes(siteCodeHash),
				Bytes.toBytes(status),
				Bytes.toBytes(502),
				Bytes.toBytes(sdate),
				Bytes.toBytes(type),
				Bytes.toBytes(free),
				Bytes.toBytes(close),
				Bytes.toBytes(queue),
				Bytes.toBytes(scantype)				
		};
		
		

		Filter rowFilter=new RowFilter(CompareFilter.CompareOp.EQUAL,new RowKeyEqualComparator(Bytes.add(bs)));
		
		Filter rowFilter2=new RowFilter(CompareFilter.CompareOp.GREATER,new RowKeyGLComparator(Bytes.add(bs2)));
		Filter rowFilter3=new RowFilter(CompareFilter.CompareOp.LESS,new RowKeyGLComparator(Bytes.add(bs3)));
		
		filterList.add(rowFilter);
		filterList.add(rowFilter2);
		filterList.add(rowFilter3);		
	
		FilterList fls=new FilterList(filterList);
		
		scan.setFilter(fls);

		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);			
			for (Result result : resultScanner) {				
				System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("property"),Bytes.toBytes("json"))));
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
		
		
		int md5UrlHash=0;//"00a18048ed95f1c057fccc8928ddf610".hashCode();				
		int siteCodeHash=0;//"1509250008".hashCode();
		int sdate=20161209;
		
		int status=0;
		int code=0;		
		int type=0;
		int free=0;
		int close=0;
		int queue=0;
		int scantype=0;
		
		byte[][] bs={
				Bytes.toBytes(md5UrlHash),
				Bytes.toBytes(siteCodeHash),
				Bytes.toBytes(status),
				Bytes.toBytes(code),
				Bytes.toBytes(sdate),
				Bytes.toBytes(type),
				Bytes.toBytes(free),
				Bytes.toBytes(close),
				Bytes.toBytes(queue),
				Bytes.toBytes(scantype)				
		};
		
		
		byte[][] bs2={
				Bytes.toBytes(md5UrlHash),
				Bytes.toBytes(siteCodeHash),
				Bytes.toBytes(status),
				Bytes.toBytes(code),
				Bytes.toBytes(20161207),
				Bytes.toBytes(0),
				Bytes.toBytes(1),
				Bytes.toBytes(2),
				Bytes.toBytes(queue),
				Bytes.toBytes(scantype)				
		};
	
		RowKeyEqualComparator crkbc=new RowKeyEqualComparator(Bytes.add(bs));
		RowKeyGLComparator crkglc=new RowKeyGLComparator(Bytes.add(bs));
		
		int k=crkbc.compareTo(Bytes.add(bs2), 0, Bytes.add(bs).length);
		int t=crkglc.compareTo(Bytes.add(bs2), 0, Bytes.add(bs).length);
		
		System.out.println(k);
		System.out.println(t);
		
	}
	
	
	
	
	

	
	public static Map<byte[],Map<String,Map<String,String>>> loadConnections() {
		Map<byte[],Map<String,Map<String,String>>> res=new HashMap<byte[],Map<String,Map<String,String>>>();
		try {
			//读取文件中的每一行
			List<String> lines = FileUtils.readLines(new File(fileName));

			//处理
			for (String str : lines) {
				//属性与值的Map
				Map<String, String> connMap = new HashMap<String, String>();
				
				//将每一行的json字符串进行解析，将其每一个属性与值都存上MAP中
				JSONObject jobj = JSONObject.fromObject(str);
				
				for (Object key : jobj.keySet()) {
					
					connMap.put(key.toString(), jobj.get(key).toString());
					
				}
				//将整行保存到一个名为json的属性中，方便我们直接取json进行解析
				connMap.put("json", str);
				Map<String,Map<String,String>> family=new HashMap<String,Map<String,String>>();
				family.put("property", connMap);
				
			
				//构建rowkey，注意区分数字字段与字符串字段，使用json取出来的都是字符串，如果是数字字段需要将其转化为数字，如果是字符串字段需要求其hash值
				int md5urlHash=connMap.get("md5url").hashCode();
				int siteCodeHash=connMap.get("sitecode").hashCode();
				int status=Integer.parseInt(connMap.get("status"));
				int code=Integer.parseInt(connMap.get("code"));
				int sdate=Integer.parseInt(connMap.get("sdate"));
				int type=Integer.parseInt(connMap.get("type"));
				int free=Integer.parseInt(connMap.get("free"));
				int close=Integer.parseInt(connMap.get("close"));
				int queue=Integer.parseInt(connMap.get("queue"));
				int scantype=Integer.parseInt(connMap.get("scantype"));
				
				byte[][] bs={
						Bytes.toBytes(md5urlHash),
						Bytes.toBytes(siteCodeHash),
						Bytes.toBytes(status),
						Bytes.toBytes(code),
						Bytes.toBytes(sdate),
						Bytes.toBytes(type),
						Bytes.toBytes(free),
						Bytes.toBytes(close),
						Bytes.toBytes(queue),
						Bytes.toBytes(scantype)				
				};
				res.put(Bytes.add(bs), family);
			}
			return res;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
