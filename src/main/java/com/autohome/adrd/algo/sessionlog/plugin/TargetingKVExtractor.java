package com.autohome.adrd.algo.sessionlog.plugin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.autohome.adrd.algo.protobuf.TargetingKVOperation;
import com.autohome.adrd.algo.sessionlog.interfaces.Extractor;
import com.autohome.adrd.algo.sessionlog.interfaces.ExtractorEntry;
import com.autohome.adrd.algo.sessionlog.util.OperationType;
/**
 * @author [huawei: huawei@autohome.com.cn ]
 */
public class TargetingKVExtractor implements Extractor {
	
	private List<ExtractorEntry> entryList;
	private ByteArrayOutputStream buffer;
	public TargetingKVExtractor() {
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
				//SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
				timestamp = new java.text.SimpleDateFormat ("yyyy-MM-dd HH:mm:ss").parse(tokens[1]).getTime();
			}
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			timestamp = 0;
		}
		
		ExtractorEntry entry = new ExtractorEntry();
		entry.setOperation(OperationType.TAGS);
		entry.setUserKey(tokens[0]);
		entry.setTimestamp(timestamp);
		
		TargetingKVOperation.TargetingInfo.Builder builder3=TargetingKVOperation.TargetingInfo.newBuilder();
		TargetingKVOperation.TargetingInfo.Brand.Builder builderBrand=TargetingKVOperation.TargetingInfo.Brand.newBuilder();
		TargetingKVOperation.TargetingInfo.Series.Builder builderSeries=TargetingKVOperation.TargetingInfo.Series.newBuilder();
		TargetingKVOperation.TargetingInfo.Spec.Builder builderSpec=TargetingKVOperation.TargetingInfo.Spec.newBuilder();
		TargetingKVOperation.TargetingInfo.Level.Builder builderLevel=TargetingKVOperation.TargetingInfo.Level.newBuilder();
		TargetingKVOperation.TargetingInfo.Price.Builder builderPrice=TargetingKVOperation.TargetingInfo.Price.newBuilder();
		//TargetingKVOperation.TargetingInfo.ExtendInfoList.Builder builderExtendlist=TargetingKVOperation.TargetingInfo.ExtendInfoList.newBuilder();
		//TargetingKVOperation.TargetingInfo.ExtendInfo.Builder builderExtend=TargetingKVOperation.TargetingInfo.ExtendInfo.newBuilder();
		TargetingKVOperation.TargetingInfo.ExtendInfo.Builder builderExtend=TargetingKVOperation.TargetingInfo.ExtendInfo.newBuilder();
		
		builder3.clear();
		builder3.setCookieTime(tokens[1]);
		
		String [] speclist = tokens[2].split(",");
		for (int i=0;i<speclist.length;i++) {
			String [] elems = speclist[i].split(":");
			builderSpec.clear();
			if(elems.length == 2) {
				builderSpec.setSpecid(elems[0]);
				builderSpec.setScore(Float.parseFloat(elems[1]));				
				builder3.addSpecList(builderSpec);
			}			
		}
		
		String [] serieslist = tokens[3].split(",");
		for (int i=0;i<serieslist.length;i++) {
			String [] elems = serieslist[i].split(":");
			builderSeries.clear();
			if(elems.length == 2) {
				builderSeries.setSeriesid(elems[0]);
				builderSeries.setScore(Float.parseFloat(elems[1]));
				builder3.addSeriesList(builderSeries);
			}			
		}
		
		String [] brandlist = tokens[4].split(",");
		for (int i=0;i<brandlist.length;i++) {
			String [] elems = brandlist[i].split(":");
			builderBrand.clear();
			if(elems.length == 2) {
				builderBrand.setBrandid(elems[0]);
				builderBrand.setScore(Float.parseFloat(elems[1]));
				builder3.addBrandList(builderBrand);
			}			
		}
		
		String [] levellist = tokens[5].split(",");
		for (int i=0;i<levellist.length;i++) {
			String [] elems = levellist[i].split(":");
			builderLevel.clear();
			if(elems.length == 2) {
				builderLevel.setLevelid(elems[0]);
				builderLevel.setScore(Float.parseFloat(elems[1]));
				builder3.addLevelList(builderLevel);
			}			
		}
		
		String [] pricelist = tokens[6].split(",");
		for (int i=0;i<pricelist.length;i++) {
			String [] elems = pricelist[i].split(":");
			builderPrice.clear();
			if(elems.length == 2) {
				builderPrice.setPriceid(elems[0]);
				builderPrice.setScore(Float.parseFloat(elems[1]));
				builder3.addPriceList(builderPrice);
			}			
		}
		

		for (int i=7 ; i<tokens.length ; i++){
			String [] extendlist = tokens[i].split(",",-1);
			for (int j=0 ; j<extendlist.length ; j++) 
			{
				String [] elems = extendlist[j].split(":");
				builderExtend.clear();
				if(elems.length == 2) {
					builderExtend.setTagid(elems[0]);
					builderExtend.setScore(Float.parseFloat(elems[1]));
					builder3.addExtendInfoList(builderExtend);
				}											
			}
		}


		
		//TargetingKVOperation.TargetingInfoList taginfo = builder.build();
		TargetingKVOperation.TargetingInfo taginfo =  builder3.build();
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
