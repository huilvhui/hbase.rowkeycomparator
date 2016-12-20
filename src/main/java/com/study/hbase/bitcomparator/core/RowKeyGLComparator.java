package com.study.hbase.bitcomparator.core;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.study.hbase.bitcomparator.core.protos.RowKeyBitProtos;

/**
 * 判断RowKey中某一段大于或小于某值，注意不能比较大于0或小于0
 * 如果构造比较器的值有多段大于0的值则只判断左起第一个不为0的数字
 * @author 410s
 *
 */
public class RowKeyGLComparator  extends ByteArrayComparable{
	Logger logger=Logger.getLogger(RowKeyGLComparator.class);
	protected byte[] data;	

	public RowKeyGLComparator(byte[] value) {
		super(value);		
		this.data=value;
	}
	

	@Override
	public int compareTo(byte[] value) {
		return this.compareTo(value, 0, value.length);
	}


	@Override
	public int compareTo(byte[] value, int offset, int length) {	
		//0 的话相等，大于小于，
		if(length!=this.data.length){
			return 0;
		}
		for(int i=0;i<this.data.length;i=i+4){
			int k=Bytes.toInt(this.data,i,4);
			if(k!=0){
				logger.info("tatatata:"+(Bytes.toInt(value,i+offset,4)-k));
				return k-Bytes.toInt(value,i+offset,4);
			}
		}
		return 0;
	}


	@Override
	public byte[] toByteArray() {
		RowKeyBitProtos.RowKeyBitComparator.Builder builder=
				RowKeyBitProtos.RowKeyBitComparator.newBuilder();
		int md5urlHash=Bytes.toInt(this.data, 0, 4);
		int siteCodeHash=Bytes.toInt(this.data,4,4);
		int status=Bytes.toInt(this.data,8,4);
		int code=Bytes.toInt(this.data,12,4);
		int sdate=Bytes.toInt(this.data,16,4);
		int type=Bytes.toInt(this.data,20,4);
		int free=Bytes.toInt(this.data,24,4);
		int close=Bytes.toInt(this.data,28,4);
		int gueue=Bytes.toInt(this.data,32,4);
		int scantype=Bytes.toInt(this.data,36,4);
		
		builder.setMd5Urlhash(md5urlHash);
		builder.setSitecodehash(siteCodeHash);
		builder.setStatus(status);
		builder.setCode(code);
		builder.setSdate(sdate);
		builder.setType(type);
		builder.setFree(free);
		builder.setClose(close);
		builder.setQueue(gueue);
		builder.setScantype(scantype);
		
		return builder.build().toByteArray();
	}
	
	public static RowKeyGLComparator parseFrom(final byte[] bytes) throws DeserializationException{
		RowKeyBitProtos.RowKeyBitComparator proto=null;
		try{
			proto=RowKeyBitProtos.RowKeyBitComparator.parseFrom(bytes);
			
			int md5urlHash=proto.getMd5Urlhash();
			int siteCodeHash=proto.getSitecodehash();
		
			
			int status=proto.getStatus();
			int code=proto.getCode();
			int sdate=proto.getSdate();
			int type=proto.getType();
			int free=proto.getFree();
			int close=proto.getClose();
			int queue=proto.getQueue();
			int scantype=proto.getScantype();
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
			return new RowKeyGLComparator(Bytes.add(bs));
		}catch (InvalidProtocolBufferException e) {
			throw new DeserializationException(e);		
		}
		
	}
	
}
