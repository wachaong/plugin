package com.autohome.adrd.algo.sessionlog.plugin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.autohome.adrd.algo.sessionlog.interfaces.Extractor;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.sessionlog.util.OperationType;
import com.autohome.adrd.algo.protobuf.BehaviorInfoOperation;
import com.autohome.adrd.algo.protobuf.PvlogOperation;
import com.autohome.adrd.algo.protobuf.SaleleadsInfoOperation;

/**
 * 
 * 
 * @author [huawei: huawei@autohome.com.cn ]
 */

public class SaleleadsExtractor implements Extractor {
	
	private List<ExtractorEntry> entryList;
	private ByteArrayOutputStream buffer;
	public SaleleadsExtractor() {
		entryList = new ArrayList<ExtractorEntry>();
		buffer = new ByteArrayOutputStream();
	}
	
	public List<ExtractorEntry> extract(String line) {
		
		entryList.clear();
		buffer.reset();
		
		String[] tokens=line.replace("\\t", "\t").split("\t",-1);
		if(tokens.length<65) 
			return entryList;
		
		long timestamp = 0;
		try {
			timestamp = new java.text.SimpleDateFormat ("yyyy-MM-dd HH:mm:ss").parse(tokens[18]).getTime();
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}				
		ExtractorEntry entry = new ExtractorEntry();
		entry.setOperation(OperationType.SALE_LEADS);
		if(tokens[3].length() > 36)
			entry.setUserKey(tokens[3].substring(0,36));
		else
			entry.setUserKey(tokens[3]);
		entry.setTimestamp(timestamp);
		
		
		//BehaviorInfoOperation.BehaviorInfo.Builder builder=BehaviorInfoOperation.BehaviorInfo.newBuilder();
		SaleleadsInfoOperation.SaleleadsInfo.Builder builder=SaleleadsInfoOperation.SaleleadsInfo.newBuilder();
		builder.clear();
		builder.setDealerorederid(tokens[0]);
		builder.setObjectid(tokens[1]);
		builder.setMemberid(tokens[2]);
		builder.setCookie(tokens[3]);
		builder.setDealerid(tokens[4]);
		builder.setProvinceid(tokens[5]);
		builder.setProvincename(tokens[6]);
		builder.setCityid(tokens[7]);
		builder.setCityname(tokens[8]);
		builder.setUsername(tokens[9]);
		builder.setPhone(tokens[10]);
		builder.setSpecid(tokens[11]);
		builder.setSpecname(tokens[12]);
		builder.setSeriesid(tokens[13]);
		builder.setContent(tokens[14]);
		builder.setState(tokens[15]);
		builder.setIspublic(tokens[16]);
		builder.setRedealerid(tokens[17]);
		builder.setDtime(tokens[18]);
		builder.setIsdel(tokens[19]);
		builder.setSiteid(tokens[20]);
		builder.setHandletime(tokens[21]);
		builder.setIp(tokens[22]);
		builder.setPid(tokens[23]);
		builder.setCid(tokens[24]);
		builder.setSid(tokens[25]);
		builder.setPublicdate(tokens[26]);
		builder.setColorname(tokens[27]);
		builder.setColorid(tokens[28]);
		builder.setRemark(tokens[29]);
		builder.setBuytime(tokens[30]);
		builder.setBuypriceoff(tokens[31]);
		builder.setSalesname(tokens[32]);
		builder.setSendlowprice(tokens[33]);
		builder.setSalesid(tokens[34]);
		builder.setTracktype(tokens[35]);
		builder.setTracktime(tokens[36]);
		builder.setUsersex(tokens[37]);
		builder.setModifytime(tokens[38]);
		builder.setReceiveddealerid(tokens[39]);
		builder.setFirsttracktime(tokens[40]);
		builder.setSalesmemo(tokens[41]);
		builder.setAssigntype(tokens[42]);
		builder.setFirsttracktype(tokens[43]);
		builder.setFirsttracksalesid(tokens[44]);
		builder.setTracksource(tokens[45]);
		builder.setFirstconnecttime(tokens[46]);
		builder.setSitename(tokens[47]);
		builder.setTracksaleid(tokens[48]);
		builder.setConfirmedtime(tokens[49]);
		builder.setUseroperateid(tokens[50]);
		builder.setUsernameOperator(tokens[51]);
		builder.setObjecttype(tokens[52]);
		builder.setObjectsubtype(tokens[53]);
		builder.setSourcetype(tokens[54]);
		builder.setObjectdata(tokens[55]);
		builder.setIpOperator(tokens[56]);
		builder.setSidOperator(tokens[57]);
		builder.setCururl(tokens[58]);
		builder.setRefurl(tokens[59]);
		builder.setSite1Id(tokens[60]);
		builder.setSite2Id(tokens[61]);
		builder.setSite3Id(tokens[62]);
		builder.setRefcookie(tokens[63]);
		builder.setOrderguide(tokens[64]);
		
		
		//BehaviorInfoOperation.BehaviorInfo bhvinfo=builder.build();
		SaleleadsInfoOperation.SaleleadsInfo saleinfo=builder.build();
		try {
			buffer.write(saleinfo.toByteArray());
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
