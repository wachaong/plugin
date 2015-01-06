package com.autohome.adrd.algo.sessionlog.plugin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.autohome.adrd.algo.sessionlog.interfaces.Extractor;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.sessionlog.util.OperationType;
import com.autohome.adrd.algo.protobuf.PvlogOperation;

/**
 * 
 * 
 * @author [huawei: huawei@autohome.com.cn ]
 */

public class PVLOGExtractor implements Extractor {
	
	private List<ExtractorEntry> entryList;
	private ByteArrayOutputStream buffer;
	public PVLOGExtractor() {
		entryList = new ArrayList<ExtractorEntry>();
		buffer = new ByteArrayOutputStream();
	}
	
	public List<ExtractorEntry> extract(String line) {
		
		entryList.clear();
		buffer.reset();
		
		String[] tokens=line.replace("\\t", "\t").split("\t",-1);
		if(tokens.length!=32) 
			return entryList;
		
		long timestamp = 0;
		try {
			timestamp = new java.text.SimpleDateFormat ("yyyy-MM-dd HH:mm:ss").parse(tokens[0]).getTime();
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}				
		ExtractorEntry entry = new ExtractorEntry();
		entry.setOperation(OperationType.PV);
		if(tokens[29].length() > 36)
			entry.setUserKey(tokens[29].substring(0,36));
		else
			entry.setUserKey(tokens[29]);
		entry.setTimestamp(timestamp);
		
		PvlogOperation.AutoPVInfo.Builder builder=PvlogOperation.AutoPVInfo.newBuilder();
		builder.clear();
		builder.setVisittime(tokens[0]);
		builder.setLogid(tokens[1]);
		builder.setSite1Id(tokens[2]);
		builder.setSite1Name(tokens[3]);
		builder.setSite2Id(tokens[4]);
		builder.setSite2Name(tokens[5]);
		builder.setSite3Id(tokens[6]);
		builder.setSite3Name(tokens[7]);
		builder.setObjectid(tokens[8]);
		builder.setCurdomain(tokens[9]);
		builder.setCururl(tokens[10]);
		builder.setReferdomain(tokens[11]);
		builder.setReferurl(tokens[12]);
		builder.setIp(tokens[13]);
		builder.setSeriesid(tokens[14]);
		builder.setSeriesname(tokens[15]);
		builder.setSpecid(tokens[16]);
		builder.setSpecname(tokens[17]);
		builder.setCateid(tokens[18]);
		builder.setSubcateid(tokens[19]);
		builder.setJbid(tokens[20]);
		builder.setSearchword(tokens[21]);
		builder.setUserid(tokens[22]);
		builder.setPvareaid(tokens[23]);
		builder.setDealerid(tokens[24]);
		builder.setProvinceid(tokens[25]);
		builder.setProvincename(tokens[26]);
		builder.setCityid(tokens[27]);
		builder.setCityname(tokens[28]);
		builder.setCoockies(tokens[29]);
		builder.setSessionid(tokens[30]);
		builder.setUseragent(tokens[31]);
		
		PvlogOperation.AutoPVInfo pvloginfo=builder.build();
		try {
			buffer.write(pvloginfo.toByteArray());
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
