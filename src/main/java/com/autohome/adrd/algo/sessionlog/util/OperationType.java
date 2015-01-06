package com.autohome.adrd.algo.sessionlog.util;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * 
 * @author [Wangchao: wangchao@autohome.com.cn ]
 */

public enum OperationType {

	PV((byte) 1, "pv"), 
	SEARCH((byte) 2, "search"), 
	AD_CLICK((byte) 3,"adclick"), 
	AD_DISPLAY((byte) 4, "addisplay"), 
	BEHAVIOR((byte) 5, "behavior"),
	SALE_LEADS((byte) 6, "saleleads"),
	TAGS((byte) 7, "tags"), 
	EXTEND((byte) 8, "extend"), 
	FILTER((byte) 9, "filter"), 
	USERINFO((byte) 10, "userinfo"),
	APPPV((byte) 11, "apppv"),
	AD_CLICK_NEW((byte) 12,"adclick_new"), 
	AD_DISPLAY_NEW((byte) 13, "addisplay_new");

	private final byte OperateId;
	private final String OperateName;

	OperationType(byte operateId, String operateName) {
		this.OperateId = operateId;
		this.OperateName = operateName;
	}

	public byte getOperateId() {
		return this.OperateId;
	}

	public String getOperateName() {
		return this.OperateName;
	}

	private static final Map<String, OperationType> byName = new HashMap<String, OperationType>();
	static {
		for (OperationType operate : EnumSet.allOf(OperationType.class)) {
			byName.put(operate.getOperateName(), operate);
		}
	}

	public static OperationType findByOperateId(int OperateId) {
		switch (OperateId) {
		case 1:
			return PV;
		case 2:
			return SEARCH;
		case 3:
			return AD_CLICK;
		case 4:
			return AD_DISPLAY;
		case 5:
			return BEHAVIOR;
		case 6:
			return SALE_LEADS;
		case 7:
			return TAGS;
		case 8:
			return EXTEND;
		case 9:
			return FILTER;
		case 10:
			return USERINFO;
		case 11:
			return APPPV;
		case 12:
			return AD_CLICK_NEW;
		case 13:
			return AD_DISPLAY_NEW;
		default:
			return null;
		}
	}

	public static OperationType findByOperateIdOrThrow(int OperateId) {
		OperationType operates = findByOperateId(OperateId);
		if (operates == null) {
			throw new IllegalArgumentException("OperateId " + OperateId
					+ " doesn't exist!");
		}
		return operates;
	}

	public static OperationType findByOperateName(String name) {
		return byName.get(name);
	}

}
