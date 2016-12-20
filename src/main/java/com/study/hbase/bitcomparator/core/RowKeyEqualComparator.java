package com.study.hbase.bitcomparator.core;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.study.hbase.bitcomparator.core.protos.RowKeyBitProtos;
/**
 * 
 * @author 410s
 *
 */
public class RowKeyEqualComparator extends ByteArrayComparable{
	
	Logger logger=Logger.getLogger(RowKeyEqualComparator.class);
	
	protected byte[] data;
	
	protected byte[] templete=null;

	public RowKeyEqualComparator(byte[] value) {
		super(value);
		this.data=value;		
		if(this.templete==null)
		{
			logger.info("hahahahahahahahhaha1");
			templete=Bytes.copy(this.data);			
			for(int i=0;i<templete.length;i=i+4){
				int k=Bytes.toInt(templete,i,4);
				if(k!=0){
					for(int j=0;j<4;j++){
						templete[i+j]|=0xff;
					}
				}				
			}			
		}else{
			logger.info("dadadadadadada2");
		}
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
	
	public static RowKeyEqualComparator parseFrom(final byte[] bytes) throws DeserializationException{
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
			return new RowKeyEqualComparator(Bytes.add(bs));
		}catch (InvalidProtocolBufferException e) {
			throw new DeserializationException(e);		
		}
		
	}
	
	public int compareTo(byte[] value){
		return compareTo(value,0,value.length);
	}

	@Override
	public int compareTo(byte[] value, int offset, int length) {
		//0 的话相等，大于小于，
		if(length!=this.data.length){
			return 1;
		}
		//复制一份data对像,用于修改，注意不可以直接 byte[] tmp=this.data;
		byte[] tmp=Bytes.copy(this.data);

	
	
		//第二步使用过滤模板与rowkey进行 与 操作,并将值存入tmp中	
		for(int i=templete.length-1;i>=0;i--){
			//与操作，过滤不作比较的字段
			tmp[i]=(byte) ((templete[i]&value[i+offset])&0xff);
		}
		
		//第三步判断是否相同
		for(int i=tmp.length-1;i>=0;i--){
			if(tmp[i]!=this.data[i])
				return 1;
		}
		return 0;	
	}

}
