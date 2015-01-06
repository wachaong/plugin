package com.autohome.adrd.algo.sessionlog.interfaces;

import java.util.List;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */

public interface Extractor {
	
    public List<ExtractorEntry> extract(String strs);
    
}
