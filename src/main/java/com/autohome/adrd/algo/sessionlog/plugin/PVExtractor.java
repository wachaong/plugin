package com.autohome.adrd.algo.sessionlog.plugin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.autohome.adrd.algo.sessionlog.interfaces.Extractor;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.sessionlog.util.OperationType;
import com.autohome.adrd.algo.protobuf.AdLogOperation;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */

public class PVExtractor implements Extractor {

	private List<ExtractorEntry> entryList;
	private ByteArrayOutputStream buffer;
	public PVExtractor() {
		entryList = new ArrayList<ExtractorEntry>();
		buffer = new ByteArrayOutputStream();
	}
	
	private String getCookie(String sessionid, String ip, String psid, String CreativeId)
	{
		String cookie = null;
		if( (!sessionid.equals("-1")) && (!sessionid.equals("null")) )
		{
			cookie = sessionid.split("%7C",-1)[0].split("\\|\\|",-1)[0];
		}
		else
		{
			cookie = "nocookie&" + ip + "_" + psid + "_" + CreativeId;
		}
		return cookie;
	}
	
	public List<ExtractorEntry> extract(String line) {
		entryList.clear();
		buffer.reset();
		
		String[] tokens=line.split("\t",-1);
		if(tokens.length!=11) 
			return entryList;
		
		String cookie = getCookie(tokens[9],tokens[4],tokens[3],tokens[0]);
		
		long timestamp = Long.parseLong(tokens[6]);
		ExtractorEntry entry = new ExtractorEntry();
		entry.setOperation(OperationType.AD_DISPLAY);
		entry.setUserKey(cookie);		
		entry.setTimestamp(timestamp);
		
		AdLogOperation.AdPVInfo.Builder builder = AdLogOperation.AdPVInfo.newBuilder();
		builder.clear();
		builder.setCookie(tokens[9]);
		builder.setAdtype(tokens[1]);
		builder.setCreativeid(tokens[0]);
		builder.setPageid(tokens[2]);
		builder.setPsid(tokens[3]);
		builder.setIp(tokens[4]);
		builder.setRegionid(tokens[5]);
		builder.setVtime(tokens[6]);
		builder.setCarid(tokens[7]);
		builder.setPvid(tokens[8]);
		builder.setReqid(tokens[10]);
				
		AdLogOperation.AdPVInfo pvinfo = builder.build();
		try {
			buffer.write(pvinfo.toByteArray());
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
