package com.autohome.adrd.algo.sessionlog.plugin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.autohome.adrd.algo.sessionlog.interfaces.Extractor;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.sessionlog.util.OperationType;
import com.autohome.adrd.algo.protobuf.ApplogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;

/**
 * 
 * 
 * @author [huawei: huawei@autohome.com.cn ]
 */

public class APPLOGExtractor implements Extractor {
	
	private List<ExtractorEntry> entryList;
	private ByteArrayOutputStream buffer;
	public APPLOGExtractor() {
		entryList = new ArrayList<ExtractorEntry>();
		buffer = new ByteArrayOutputStream();
	}
	
	public List<ExtractorEntry> extract(String line) {
		
		entryList.clear();
		buffer.reset();
		
		String[] tokens=line.replace("\\t", "\t").split("\t",-1);
		if(tokens.length<5) 
			return entryList;
		
		long timestamp = 0;
		try {
			timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(tokens[1]).getTime();
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}				
		ExtractorEntry entry = new ExtractorEntry();
		entry.setOperation(OperationType.APPPV);
		//entry.setUserKey(tokens[6]);
		entry.setTimestamp(timestamp);
		
		//PvlogOperation.AutoPVInfo.Builder builder=PvlogOperation.AutoPVInfo.newBuilder();
		ApplogOperation.AutoAppInfo.Builder builder=ApplogOperation.AutoAppInfo.newBuilder();
		builder.clear();
		if("clt".equalsIgnoreCase(tokens[0].trim())){
			if(tokens.length!=16)
				return entryList;
			entry.setUserKey(tokens[6]);
			
			builder.setLogtype(tokens[0]);
			builder.setDates(tokens[1]);
			builder.setAppkey(tokens[2]);
			builder.setPlatform(tokens[3]);
			builder.setSystem(tokens[4]);
			builder.setLanguage(tokens[5]);
			builder.setDeviceid(tokens[6]);
			builder.setSessionid(tokens[7]);
			builder.setResolution(tokens[8]);
			builder.setDevicemodel(tokens[9]);
			builder.setVersion(tokens[10]);
			builder.setNetwork(tokens[11]);
			builder.setIsjailbroken(tokens[12]);
			builder.setUserid(tokens[13]);
			builder.setIp(tokens[14]);
			builder.setChannel(tokens[15]);
					
		}else if("act".equalsIgnoreCase(tokens[0].trim())){
			if(tokens.length!=10)
				return entryList;
			entry.setUserKey(tokens[7]);
						
			builder.setLogtype(tokens[0]);
			builder.setDates(tokens[1]);
			builder.setAppkey(tokens[2]);
			builder.setSessionid(tokens[3]);
			builder.setStarttime(tokens[4]);
			builder.setEndtime(tokens[5]);
			builder.setActivity(tokens[6]);
			builder.setDeviceid(tokens[7]);
			builder.setChannel(tokens[8]);
			builder.setVersion(tokens[9]);
					
		}else if("evt".equalsIgnoreCase(tokens[0].trim())){
			if(tokens.length!=22)
				return entryList;
			entry.setUserKey(tokens[7]);
			
			builder.setLogtype(tokens[0]);
			builder.setDates(tokens[1]);
			builder.setAppkey(tokens[2]);
			builder.setEvent(tokens[3]);
			builder.setActivity(tokens[4]);
			builder.setStarttime(tokens[5]);
			builder.setEndtime(tokens[6]);
			builder.setDeviceid(tokens[7]);
			builder.setSessionid(tokens[8]);
			builder.setChannel(tokens[9]);
			builder.setVersion(tokens[10]);
			builder.setCustargv1(tokens[11]);
			builder.setCustargv2(tokens[12]);
			builder.setCustargv3(tokens[13]);
			builder.setCustargv4(tokens[14]);
			builder.setCustargv5(tokens[15]);
			builder.setCustargv6(tokens[16]);
			builder.setCustargv7(tokens[17]);
			builder.setCustargv8(tokens[18]);
			builder.setCustargv9(tokens[19]);
			builder.setCustargv10(tokens[20]);
			builder.setIp(tokens[21]);
			
		}
	
		
		ApplogOperation.AutoAppInfo apploginfo=builder.build();
		try {
			buffer.write(apploginfo.toByteArray());
			entry.setValue(buffer.toByteArray());			
			entryList.add(entry);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			buffer.reset();
			e.printStackTrace();
		}
		
		return entryList;
	}
	

}
