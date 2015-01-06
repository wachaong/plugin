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
import com.autohome.adrd.algo.protobuf.SearchOperation;

/**
 * 
 * 
 * @author [wangchao: wangchao@autohome.com.cn ]
 */

public class SearchExtractor implements Extractor {
	
	private List<ExtractorEntry> entryList;
	private ByteArrayOutputStream buffer;
	public SearchExtractor() {
		entryList = new ArrayList<ExtractorEntry>();
		buffer = new ByteArrayOutputStream();
	}
	
	public List<ExtractorEntry> extract(String line) {
		
		entryList.clear();
		buffer.reset();
		
		String[] tokens=line.replace("\\t", "\t").split("\t",-1);
		if(tokens.length!=29) 
			return entryList;
		
		long timestamp = 0;
		try {
			timestamp = new java.text.SimpleDateFormat ("yyyy-MM-dd HH:mm:ss").parse(tokens[1]).getTime();
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}				
		ExtractorEntry entry = new ExtractorEntry();
		entry.setOperation(OperationType.SEARCH);
		if(tokens[4].length() > 36)
			entry.setUserKey(tokens[4].substring(0,36));
		else
			entry.setUserKey(tokens[4]);
		entry.setTimestamp(timestamp);
		
		SearchOperation.SearchInfo.Builder builder=SearchOperation.SearchInfo.newBuilder();
		builder.clear();
		builder.setAnalysislogId(tokens[0]);
		builder.setDatetime(tokens[2]);
		builder.setIshaveresult(tokens[3]);
		builder.setArea(tokens[4]);
		builder.setAutouserid(tokens[5]);
		builder.setUniqueuserid(tokens[6]);
		builder.setUrl(tokens[8]);
		builder.setReferurl(tokens[9]);
		
		
		
		SearchOperation.SearchInfo pvloginfo=builder.build();
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
