package com.autohome.adrd.algo.sessionlog.plugin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.autohome.adrd.algo.protobuf.AdLogOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.TargetingOperation;
import com.autohome.adrd.algo.sessionlog.interfaces.Extractor;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.sessionlog.util.OperationType;
/**
 * @author [huawei: huawei@autohome.com.cn ]
 */
public class TargetingExtractor implements Extractor {
	
	private List<ExtractorEntry> entryList;
	private ByteArrayOutputStream buffer;
	public TargetingExtractor() {
		entryList = new ArrayList<ExtractorEntry>();
		buffer = new ByteArrayOutputStream();
	}

	@Override
	public List<ExtractorEntry> extract(String line) {
		
		entryList.clear();
		buffer.reset();
		
		String [] tokens = line.split("\t" ,-1);
				
		long timestamp = 0;
		try {
			if (tokens[1].equalsIgnoreCase("0")) {
				timestamp = 0;
			}else{
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
				timestamp = new java.text.SimpleDateFormat ("yyyy-MM-dd HH:mm:ss").parse(df.format(tokens[1])).getTime();
			}
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		ExtractorEntry entry = new ExtractorEntry();
		entry.setOperation(OperationType.TAGS);
		entry.setUserKey(tokens[0]);
		entry.setTimestamp(timestamp);
		
		TargetingOperation.TargetingInfoList.Builder builder=TargetingOperation.TargetingInfoList.newBuilder();
		builder.clear();
		TargetingOperation.TargetingInfo.Builder builder2 =TargetingOperation.TargetingInfo.newBuilder();
		
		builder.setCookietime(tokens[1]);
		for (int i=2 ; i<=tokens.length ; i++) 
		{			
			String [] features = tokens[i].split(",",-1);			
			for (int j=0 ; j<=features.length ; j++) 
			{
				builder2.clear();
				String [] elems = features[j].split(":",-1);
				if (elems.length == 2) 
				{
					builder2.setTagid(elems[0]);
					float score = Long.parseLong(elems[1]);
					builder2.setScore(score);
					builder.addTag(builder2.build());
				}							
			}
		}
		
		TargetingOperation.TargetingInfoList taginfo = builder.build();
		try {
			buffer.write(taginfo.toByteArray());
			entry.setValue(buffer.toByteArray());			
			entryList.add(entry);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			buffer.reset();
			e.printStackTrace();
		}
		
		// TODO Auto-generated method stub
		return entryList;
	}

}
