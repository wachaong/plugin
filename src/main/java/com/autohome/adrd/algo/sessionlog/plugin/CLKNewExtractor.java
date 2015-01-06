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

public class CLKNewExtractor implements Extractor{
	
	private List<ExtractorEntry> entryList;
	private ByteArrayOutputStream buffer;
	public CLKNewExtractor(){
		entryList= new ArrayList<ExtractorEntry>();
		buffer=new ByteArrayOutputStream();		
	}
	
	private String  getCookie(String sessionid,String ip,String ua) {
		String cookie="";
		if(!(sessionid.equals("null")))
		{
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
		
		//if(tokens.length!=43)
		if(tokens.length < 43) 
		{			
			return entryList;
		}
		
		String cookie=getCookie(tokens[9], tokens[6], tokens[14]);
		//String cookie = "11";
		
		long timestamp=parsetime(tokens[0]);
				
		ExtractorEntry entry= new ExtractorEntry();
		entry.setOperation(OperationType.AD_CLICK_NEW);
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
		
		NewAdLogOperation.AdCLKInfo.Builder builder= NewAdLogOperation.AdCLKInfo.newBuilder();
		
		builder.clear();
		builder.setRegTime(Reg_time);
		builder.setChannel(Channel);
		builder.setClktime(tokens[0]);
		builder.setVersion(tokens[1]);
		builder.setPvid(tokens[2]);
		builder.setClkid(tokens[3]);
		builder.setImpresstime(tokens[4]);
		builder.setImpressip(tokens[5]);		
		builder.setClkip(tokens[6]);	
		builder.setProvince(tokens[7]);	
		builder.setCity(tokens[8]);	
		builder.setPvcookie(tokens[9]);	
		builder.setClkcookie(tokens[10]);	
		builder.setHost(tokens[11]);	
		builder.setPvuid(tokens[12]);
		builder.setClkuid(tokens[13]);	
		builder.setUseragent(tokens[14]);	
		builder.setPageid(tokens[15]);	
		builder.setLan(tokens[16]);
		builder.setOsversion(tokens[17]);
		builder.setAppid(tokens[18]);
		builder.setBrand(tokens[19]);
		builder.setScreenWidth(tokens[20]);
		builder.setScreenHight(tokens[21]);
		builder.setLatitude(tokens[22]);
		builder.setLongitude(tokens[23]);
		builder.setPsid(tokens[24]);
		builder.setPaltform(tokens[25]);
		builder.setReferer(tokens[26]);
		builder.setDesturl(tokens[27]);
		builder.setSiteid(tokens[28]);
		builder.setCategoryid(tokens[29]);
		builder.setSubcategoryid(tokens[30]);
		builder.setCampaignid(tokens[31]);
		builder.setGroupid(tokens[32]);
		builder.setCreativeid(tokens[33]);
		builder.setBehavior(tokens[34]);
		builder.setFrame(tokens[35]);
		builder.setPageinfo(tokens[36]);
		builder.setMatchuserinfo(tokens[37]);
		builder.setMatchpageinfo(tokens[38]);
		builder.setCreativesize(tokens[39]);
		builder.setCreativeform(tokens[40]);
		builder.setFilter(tokens[41]);	
		
		
		
		NewAdLogOperation.AdCLKInfo clkinfo=builder.build();
		try {
			buffer.write(clkinfo.toByteArray());
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
