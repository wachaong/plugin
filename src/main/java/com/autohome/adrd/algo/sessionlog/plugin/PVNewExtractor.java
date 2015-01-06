package com.autohome.adrd.algo.sessionlog.plugin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.autohome.adrd.algo.sessionlog.interfaces.Extractor;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.sessionlog.util.OperationType;
import com.autohome.adrd.algo.protobuf.NewAdLogOperation;;

public class PVNewExtractor implements Extractor{
	
	private List<ExtractorEntry> entryList;
	private ByteArrayOutputStream buffer;
	public PVNewExtractor(){
		entryList= new ArrayList<ExtractorEntry>();
		buffer=new ByteArrayOutputStream();		
	}
	
	private String getCookie(String sessionid,String ip,String ua) {
		String cookie="";
		if ( !(sessionid.equals(null))) {
				cookie=sessionid.split("\\|\\|",-1)[0];			
			}
		else {
			cookie="nocookie&"+ip+"_"+ua;
		}		
		return cookie;		
	}
	
	private long parsetime(String time) {
		long vtime=0;		
		SimpleDateFormat format =   new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );  	     
	    Date date;
		try {
			date = format.parse(time);
			vtime=date.getTime();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  	   	    
	    return vtime;
	}
	
	public List<ExtractorEntry> extract(String line) {
		entryList.clear();
		buffer.reset();
		
		String[] tokens=line.split("\t",-1);
		
		//if(tokens.length!=41)
		if(tokens.length < 41) 
			return entryList;
		
		String cookie= getCookie(tokens[15], tokens[16],tokens[19]);
		long timestamp=parsetime(tokens[0]);
				
		ExtractorEntry entry= new ExtractorEntry();
		entry.setOperation(OperationType.AD_DISPLAY_NEW);
		entry.setUserKey(cookie);
		entry.setTimestamp(timestamp);
		//some extra useful info 
		String Reg_time="";
		String Channel="";
		if(!(tokens[15].equals(null)))
		{
			if (tokens[15].split("\\|\\|",1).length>=2) {
				Reg_time=tokens[15].split("\\|\\|",-1)[1];
			}
			if (tokens[15].split("\\|\\|",1).length>=3) {
				Channel=tokens[15].split("\\|\\|",-1)[2];	
			}				
		}
		
		NewAdLogOperation.AdPVInfo.Builder builder= NewAdLogOperation.AdPVInfo.newBuilder();
		
		builder.clear();
		builder.setRegTime(Reg_time);
		builder.setChannel(Channel);
		builder.setVtime(tokens[0]);
		builder.setVersion(tokens[1]);
		builder.setPath(tokens[2]);
		builder.setPageid(tokens[3]);
		builder.setReqid(tokens[4]);
		builder.setPvid(tokens[5]);		
		builder.setBucketid(tokens[6]);	
		builder.setRt(tokens[7]);	
		builder.setPsid(tokens[8]);	
		builder.setPaltform(tokens[9]);	
		builder.setRefferurl(tokens[10]);	
		builder.setPageurl(tokens[11]);	
		builder.setSiteid(tokens[12]);
		builder.setCategoryid(tokens[13]);	
		builder.setSubcategoryid(tokens[14]);	
		builder.setSessionid(tokens[15]);	
		builder.setIp(tokens[16]);
		builder.setProvince(tokens[17]);
		builder.setCity(tokens[18]);
		builder.setUa(tokens[19]);
		builder.setUid(tokens[20]);
		builder.setUserinfo(tokens[21]);
		builder.setPageinfo(tokens[22]);
		builder.setLan(tokens[23]);
		builder.setOsversion(tokens[24]);
		builder.setAppid(tokens[25]);
		builder.setBrand(tokens[26]);
		builder.setScreenWidth(tokens[27]);
		builder.setScreenHight(tokens[28]);
		builder.setLatitude(tokens[29]);
		builder.setLongitude(tokens[30]);
		builder.setCampaignid(tokens[31]);
		builder.setGroupid(tokens[32]);
		builder.setCreativeid(tokens[33]);
		builder.setSellmodel(tokens[34]);
		builder.setCreativesize(tokens[35]);
		builder.setCreativeform(tokens[36]);
		builder.setMatchuserinfo(tokens[37]);
		builder.setMatchpageinfo(tokens[38]);
		builder.setFilter(tokens[39]);	
		
		NewAdLogOperation.AdPVInfo pvinfo=builder.build();
		try {
			buffer.write(pvinfo.toByteArray());
			entry.setValue(buffer.toByteArray());
			entryList.add(entry);
		} catch (IOException e) {
			// TODO: handle exception
			buffer.reset();
			e.printStackTrace();
		}		
		return entryList;
	}

}
