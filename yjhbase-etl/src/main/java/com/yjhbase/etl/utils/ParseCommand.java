package com.yjhbase.etl.utils;

import java.text.SimpleDateFormat;
import java.util.*;

public class ParseCommand {
	
	public static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	public static final SimpleDateFormat dtDateFormat = new SimpleDateFormat("yyyyMMdd");
	public static final String CURRENT_DATE_REGEX = "^[-|+]{0,1}[0-9]*[YyMmDd]{1}$";
	public static final String STANDARD_DATE_REGEX2 = "^[0-9]{4}-[0-9]{2}-[0-9]{2}[-|+]{1}[0-9]*[YyMmDd]{1}$";	public static final String STANDARD_DATE_REGEX3 = "^[0-9]{4}[0-9]{2}[0-9]{2}[-|+]{1}[0-9]*[YyMmDd]{1}$";
	public static final String STANDARD_DATE_REGEX4 = "^[0-9]{1,2}[-|+]{1}[0-9]*[Hh]{1}$";
    public static final String STANDARD_DATE_REGEX5 = "^[0-9]{1,2}[-|+]{1}[0-9]*[Ff]{1}$";
    public static final String STANDARD_DATE_REGEX6 = "^[0-9]{4}[-|+]{1}[0-9]*[Pp]{1}$";

	public static final String BEGIN = "${";

	public static final String END= "}";

	public static final String DATE_REGEX = "^[YyMmDd]{0,1}[-|+]*[0-9]*$";
	
	private Map<String, String> map = null;
	private List<String> command = null;
	
	public ParseCommand(String[] argc){
		map =new HashMap<String, String>();
		command = new ArrayList<String>();
		if(argc.length==0){
			return;
		}
		String key="";
		String value="";
		boolean isEscaping=false;
		for(String par: argc){
			if(par.contains("={")){
				int idx=par.indexOf("={");
				if(par.endsWith("}")){
					key = par.substring(0, idx).trim();
					value=par.substring(idx+2, par.length()-1).trim();
					map.put(key, value);
				}else{
					key = par.substring(0, idx).trim();
					value=par.substring(idx+2).trim();
					isEscaping=true;	
				}
				continue;
			}

			if(par.endsWith("}")){
				value=value + " " + par.substring(0, par.length()-1);
				map.put(key, value);
				isEscaping=false;	
				continue;
			}
			
			if(isEscaping){
				value= value + " " + par;
				continue;
			}
			
			if(par.contains("=")){
				int idx=par.indexOf("=");
				key = par.substring(0, idx).trim();
				value=par.substring(idx+1).trim();
				map.put(key, value);
			}else{
				command.add(par);
			}
		}	
	}
	

	public static String parseCommand(String raw){
		return parseCommand(raw, null);
	}
	
	public static String parseCommand(String raw, Map<String, String> map){
		String res=raw;
		int begin=res.indexOf(BEGIN);
		while(begin!=-1){
			String suffix= res.substring(begin+BEGIN.length());
			int end=begin +BEGIN.length() + suffix.indexOf(END);
			//int end=res.indexOf(END);
			String tmp = res.substring(begin+BEGIN.length(), end).trim();
			if(tmp.matches(DATE_REGEX)
                    ||tmp.matches(CURRENT_DATE_REGEX)
                    ||tmp.matches(STANDARD_DATE_REGEX2)
                    ||tmp.matches(STANDARD_DATE_REGEX3)
                    || tmp.matches(STANDARD_DATE_REGEX5)) {
				try{
					res = res.substring(0, begin) + getParsedDate(tmp) + res.substring(end + 1);
				}catch(Exception ex){
					throw new RuntimeException("Invalid date expression for : " + tmp + ".", ex);
				}
				
			}else{
				if(tmp.contains("-")||tmp.contains("+")){
					int idx=tmp.indexOf("-");
					if(tmp.contains("+") && tmp.indexOf("+")>idx){
						idx=tmp.indexOf("+");
					}
					String key=tmp.substring(0, idx);
					if(map!=null && map.get(key)!=null){
						try {
							res = res.substring(0, begin) + getParsedDate(map.get(key)+tmp.substring(idx)) + res.substring(end+1, res.length());
						} catch (Exception e) {
							throw new RuntimeException("Invalid expression :" + BEGIN + key + END + ".", e);	
						}
					}else{
						throw new RuntimeException("Invalid expression :" + BEGIN + tmp + END + ".");	
					}
				}else if(map!=null && map.get(tmp)!=null){
					res = res.substring(0, begin) + map.get(tmp) + res.substring(end+1, res.length());	
				}
				else if(tmp.contains("#") && map.get(tmp.split("\\#")[0]) != null){
					String mode = tmp.split("\\#")[1];
					String value = map.get(tmp.split("\\#")[0]);
					if(mode.equals("d")){
						value = value.substring(8);
					}
					if(mode.equals("m")){
						value = value.substring(5,7);
					}
					if(mode.equals("y")){
						value = value.substring(0,4);
					}
					res = res.substring(0, begin) + value + res.substring(end+1, res.length());	
				}
				else if(!tmp.toLowerCase().contains("condition")){
					throw new RuntimeException("Invalid expression :" + BEGIN + tmp + END + ".");				
				}	
				
			}			
			begin=res.indexOf(BEGIN);
		}		
		return res;
	}

	public static  String getParsedDateOld(String exp, Map<String, String> param){
		Long startTime=0L;
		Long offset=0L;
		char unit=0;
		SimpleDateFormat format=null;
		boolean isDtFormat=false;

		int index=-1;
		if(exp.contains("+")){
			index=exp.indexOf("+");
		}
		if(exp.contains("-")){
			index=exp.indexOf("-");
		}
		if(index==-1){
			throw new RuntimeException("The timeExp "+ exp+" is not offset date expression.");
		}


		try{
			String offsetExp=exp.substring(index);
			if(index==0){
				startTime=System.currentTimeMillis();
			}else{
				String dateValue=param.get(exp.substring(0,index));
				isDtFormat=!dateValue.contains("-");
				format=dateValue.contains("-")?dateFormat :dtDateFormat;
				startTime=format.parse(param.get(exp.substring(0, index))).getTime();
			}
			String offsetStr=exp.substring(index);
			unit=offsetStr.charAt(offsetStr.length()-1);
			offset=Long.valueOf(offsetStr.substring(0, offsetStr.length()-1));

		} catch(Exception ex){
			throw new RuntimeException("The timeExp "+ exp+" is not offset date expression.");
		}

		Date startDate=new Date(startTime);
		Long startYear=startDate.getYear()+1900L;
		if(unit=='Y'||unit=='y'){
			return new Long(startYear+ offset).toString();
		}else if(unit=='M'||unit=='m'){
			Long month=startDate.getMonth()+offset;
			Long mOff=month%12;
			Long yOff=(Long)((month-mOff)/12);
			if(mOff<=0){
				mOff+=12;
				yOff-=1;
			}


			return (startYear+yOff)+ (isDtFormat?"":"-") + (mOff>10?mOff.toString():("0"+mOff.toString()));

		}else if(unit=='H'||unit=='h'){
			Long month=startDate.getMonth()+offset;
			Long mOff=month%12;
			Long yOff=(Long)((month-mOff)/12);
			if(mOff<=0){
				mOff+=12;
				yOff-=1;
			}


			return (startYear+yOff)+ (isDtFormat?"":"-") + (mOff>10?mOff.toString():("0"+mOff.toString()));

		}else{
			return format.format(new Date(startTime+ offset* 86400000));
		}
	}


	/**
	 * 对SQL文件进行变量替换，如果是测试，则执行测试Schema重定向
	 * @param raw
	 * @param map
	 * @return
	 */
	public static String parseCommandOld(String raw, Map<String, String> map){
		String res=raw;
		String beginMask="AAAAAAAAAAAAAAAAA";
		String endMask="BBBBBBBBBBBBBBBBBBBB";
		int begin=res.indexOf(BEGIN);
		while(begin!=-1){
			String suffix= res.substring(begin+BEGIN.length());
			int end=begin +BEGIN.length() + suffix.indexOf(END);
			String tmp = res.substring(begin+BEGIN.length(), end).trim();
			if(tmp.contains("+")||tmp.contains("-")){
				int idx=tmp.indexOf("-");
				if(tmp.contains("+") && tmp.indexOf("+")>idx){
					idx=tmp.indexOf("+");
				}
				String key=tmp.substring(0, idx);
//				res = res.substring(0, begin) + getParsedDateOld(tmp, map) + res.substring(end+1, res.length());
				if(map!=null && map.get(key)!=null){
					try {
						res = res.substring(0, begin) + getParsedDate(map.get(key)+tmp.substring(idx)) + res.substring(end+1, res.length());
					} catch (Exception e) {
						throw new RuntimeException("Invalid expression :" + BEGIN + key + END + ".", e);
					}
				}else{
					throw new RuntimeException("Invalid expression :" + BEGIN + tmp + END + ".");
				}
			}else{
				if(map!=null && map.get(tmp)!=null){
					res = res.substring(0, begin) + map.get(tmp) + res.substring(end+1, res.length());
				}else{
					res = res.substring(0, begin) + beginMask+tmp + endMask+ res.substring(end+1, res.length());
					//throw new RuntimeException("Invalid expression :" + BEGIN + tmp + END + ".");
				}
			}
			begin=res.indexOf(BEGIN);
		}
		res=res.replace(beginMask, BEGIN).replace(endMask, END);
		return res;
	}

	public static String getParsedDate_old(String exp){
		Date begin = new Date();
		String today = dateFormat.format(begin);
		String[] date= today.split("-");
		int year = Integer.parseInt(date[0]);
		int month = Integer.parseInt(date[1]);
		int day = Integer.parseInt(date[2]);
		int[] dateArray = new int[]{year, month, day};		

		String value = exp;
		int index = 2;
		if(exp.startsWith("y")|| exp.startsWith("Y")) {
			index=0; 
			value = value.substring(1, value.length()).trim();
		}else if(exp.startsWith("M")||exp.startsWith("m")){
			index=1;
			value = value.substring(1, value.length()).trim();
		}else if(exp.startsWith("D")||exp.startsWith("d")){
			index=2;
			value = value.substring(1, value.length()).trim();
		}

		int offset=0;
		if(value.length()>0){
			if(value.startsWith("+")){
				value=value.substring(1, value.length()).trim();
			}
			offset = Integer.valueOf(value);
		}			
		if(index<2){
			dateArray[index]= dateArray[index]+ offset;
			if(index==1){
				if(dateArray[1]<=0 || dateArray[1]>12){
					int numY=dateArray[1]/12;
					int numM = dateArray[1]%12;
					dateArray[0]= dateArray[0] + numY;
					dateArray[1]=numM;						
					if(dateArray[1]<=0){
						dateArray[0]= dateArray[0] - 1;
						dateArray[1]= dateArray[1]+12;
					}						
				}
			}				
		}else{
			
			//注意: 此处offset 必须进行往 Long 型的转化，否则会出现隐藏错误
		    //long time = begin.getTime()+ 1000* 3600 * 24 * offset;
			long time = begin.getTime()+ 1000* 3600 * 24 * (long)offset;
			return dateFormat.format(new Date(time));
		}

		if(index==0){
			return new Integer(dateArray[0]).toString();
		}
		if(index==1 && dateArray[1]<10){
			return dateArray[0]+ "-0"+ dateArray[1];
		}
		return dateArray[0]+ "-"+ dateArray[1];			
	}

	
	public static String getParsedDate(String exp) throws Exception{
		Long startTime=0L;
		Long offset=0L;
		if(exp.length()==0){
			Date begin = new Date();
			long time = begin.getTime()+ 1000* 3600 * 24 * offset;
			return dateFormat.format(new Date(time));
		}
		if(exp.matches(CURRENT_DATE_REGEX)){
			startTime=System.currentTimeMillis();
			offset=Long.valueOf(exp.substring(0, exp.length()-1));
		}else if(exp.matches(STANDARD_DATE_REGEX2)) {
			startTime=dateFormat.parse(exp.substring(0,10)).getTime();
			offset=Long.valueOf(exp.substring(10, exp.length()-1));
		}else if(exp.matches(STANDARD_DATE_REGEX3)) {
			startTime=dateFormat.parse(exp.substring(0,4)+"-"+exp.substring(4,6)+"-"+exp.substring(6,8)).getTime();
			offset=Long.valueOf(exp.substring(8, exp.length()-1));
		}else if(exp.matches(STANDARD_DATE_REGEX4)
                ||exp.matches(STANDARD_DATE_REGEX5)) {
			if(exp.contains("-")) {
				startTime = Long.parseLong(exp.split("\\-")[0]);
				offset = -Long.parseLong(exp.split("\\-")[1].substring(0, exp.split("\\-")[1].length() - 1));
			}else if(exp.contains("+")){
				startTime = Long.parseLong(exp.split("\\+")[0]);
				offset = Long.parseLong(exp.split("\\+")[1].substring(0, exp.split("\\+")[1].length() - 1));
			}
		}else if(exp.matches(STANDARD_DATE_REGEX6)) {
			String pre = exp.split("\\-")[0];
			String hour = pre.substring(0,2);
			String minute = pre.substring(2,4);
			String time = String.valueOf(Integer.valueOf(hour)*60+Integer.valueOf(minute));
			if(exp.contains("-")) {
				startTime = Long.parseLong(time);
				offset = -Long.parseLong(exp.split("\\-")[1].substring(0, exp.split("\\-")[1].length() - 1));
			}else if(exp.contains("+")){
				startTime = Long.parseLong(time);
				offset = Long.parseLong(exp.split("\\+")[1].substring(0, exp.split("\\+")[1].length() - 1));
			}
		}
		Date startDate=new Date(startTime);
		char unit=exp.charAt(exp.length()-1);
		Long startYear=startDate.getYear()+1900L;
		if(unit=='Y'||unit=='y'){
			return new Long(startYear+ offset).toString();
		}else if(unit=='M'||unit=='m'){
			Long month=startDate.getMonth()+offset+1;
			Long mOff=month%12;
			Long yOff=(Long)((month-mOff)/12);
			if(mOff<=0){
				mOff+=12;
				yOff-=1;
			}
			if(exp.matches(CURRENT_DATE_REGEX)||exp.matches(STANDARD_DATE_REGEX2))
				return (startYear+yOff)+ "-" + (mOff>=10?mOff.toString():("0"+mOff.toString()));
			else
				return (startYear+yOff)+ (mOff>=10?mOff.toString():("0"+mOff.toString()));
		}else if(unit=='D'||unit=='d'){
			if(exp.matches(CURRENT_DATE_REGEX)||exp.matches(STANDARD_DATE_REGEX2))
				return dateFormat.format(new Date(startTime+ offset* 86400000));
			else
				return new SimpleDateFormat("yyyyMMdd").format(new Date(startTime + offset * 86400000));

		} else if (unit == 'H' || unit == 'h') {
			String hour;
			if(startTime + offset >= 0) {
				hour = String.valueOf((startTime + offset)%24);
				return hour.length() == 2 ? hour : "0" + hour;
			} else {
				hour = String.valueOf(24 - Math.abs(startTime + offset)%24);
				return hour.length() == 2 ? hour : "0" + hour;
			}
		} else if (unit == 'F' || unit == 'f') {
            String minute;
            if(startTime + offset >= 0) {
                minute = String.valueOf((startTime + offset)%60);
                return minute.length() == 2 ? minute : "0" + minute;
            } else {
                minute = String.valueOf(60 - Math.abs(startTime + offset)%60);
                return minute.length() == 2 ? minute : "0" + minute;
            }
        } else if (unit == 'P' || unit == 'p') {
			String hour;
			String minute;
			if(startTime + offset >= 0) {
				long newTime = startTime+offset;
				hour = String.valueOf((newTime/60)%24);
				minute = String.valueOf(newTime%60);
				String newhour = hour.length() == 2 ? hour : "0" + hour;
				String newminute = minute.length() == 2 ? minute : "0" + minute;
				return newhour+newminute;
			} else {
				long newTime = Math.abs(startTime+offset);
				hour = String.valueOf(24-((int)Math.ceil(newTime/(double)60))%24);
				minute = String.valueOf(60-newTime%60);
				String newhour = hour.length() == 2 ? hour : "0" + hour;
				String newminute = minute.length() == 2 ? minute : "0" + minute;
				return newhour+newminute;
			}
		}
        throw new RuntimeException("Invalid expression :" + exp + ".");
	}

	
	public Map<String, String> getParamMap(){
		return map;
	}
	
	public List<String> getParamList(){
		return command;
	}
	
	public String getParam(String name){
		return map.get(name);
	}   
	

	public String getParam(String name, String defaultValue){
		return map.get(name)==null ? defaultValue: map.get(name);
	}
    	
	public boolean useCommand(String cmd){
		return command.contains(cmd);
	}   
  
	
	public static void main(String[] argc) throws Exception{
		String[] test = {"query", "db.name=my_test", "query={select", "id", "from", "my_test.users}"};
		
		ParseCommand command = new ParseCommand(test);
		
		//System.out.println(getParsedDate("2014-09-14+100D"));
		//System.out.println(getParsedDate("2014-09-14+10M"));
		//System.out.println(getParsedDate("2014-09-14-15M"));
		//System.out.println(getParsedDate("2014-09-14+100M"));
		//System.out.println(getParsedDate("2014-09-14+1Y"));
		//System.out.println(getParsedDate("2014-09-14-10Y"));

		//System.out.println(getParsedDate("20140914+100D"));
		//System.out.println(getParsedDate("20140914+10M"));
		//System.out.println(getParsedDate("20140914-15M"));
		//System.out.println(getParsedDate("20140914+100M"));
		//System.out.println(getParsedDate("20140914+1Y"));
		//System.out.println(getParsedDate("20140914-10Y"));



		Map<String, String> par=new HashMap<String,String>();
		par.put("dt", "20160503");
		par.put("day", "2016-05-07");
		par.put("hour","00");
		par.put("hour2","0");
		par.put("check","0");
		par.put("start","${dt}${hour}");
        par.put("end","${dt-1d}");

		String sql="create table temp.qsc_dw_trd_order_wide_bu_hour_test as \nselect \nutp_id, \nbu_id," +
				" \nconcat(\'{'polygon':[\',concat_ws(\',\',collect_set(lat_lng)),\']}\') as geojson \nfrom \n( \nselect \nutp_id, \nbu_id, \nconcat(\'{'lat':\',split(geo_range_1,\' \')[0],\','lng':\',split(geo_range_1,\' \')[1],\'}\') as lat_lng \nfrom \n( \nselect \nbusiness_package_id as utp_id, \nbu_id, \nsplit(regexp_replace(geo_range,\'POLYGON\\\\(\\\\(|\\\\)\\\\)\',\'\'),\',\') as geo_range \nfrom dw.dw_bu_bpm_business_package_range_hour \nwhere dt=\'${day}\' and hour=\'${hour}\' \n) t \nlateral view explode(t.geo_range) adtable as geo_range_1 \n) t1 \ngroup by utp_id,bu_id \n;";
		String sql1="drop table if exists temp.temp_dm_restanurant_carousel_01_${dt-1d}; \n" +
				"create table temp.temp_dm_restanurant_carousel_${}_01_${dt-1d} as \n" +
				"SELECT 1 is_rescued from temp.temp_dm_trd_restaurant_utp_sale_13; ";

		String sql12 ="SELECT t.bill_code,t.sign_date,t.record_site_id,t.record_site,record_man_code,t.ds\n" +
                "from ods.zto_sign  T\n" +
                "where t.ds>='${start}' and t.ds'<${end}'";
//		System.out.println(parseCommand("date='${d}'", par));
//		System.out.println(parseCommand(sql1, par));
//		System.out.println(parseCommand("drop table if exists temp.temp_dm_restanurant_carousel_01_${}_${dt-1d};", par));
		//System.out.println(parseCommand(sql12, par));
//		System.out.println(parseCommand("date='${day#d}'", par));
//		System.out.println(parseCommand("date='${day#m}'", par));
//		System.out.println(parseCommand("date='${day#y}'", par));

//		System.out.println(parseCommand("date='${day+1m}'", par));
//		System.out.println(parseCommand("date='${day-1m}'", par));
//		System.out.println(parseCommand("date='${day+1y}'", par));
//		System.out.println(parseCommand("date='${day-1y}'", par));
//		System.out.println(parseCommand("date='${dt+1d}'", par));
//		System.out.println(parseCommand("date='${dt-1d}'", par));
//		System.out.println(parseCommand("date='${dt+1m}'", par));
//		System.out.println(parseCommand("date='${dt-1m}'", par));
//		System.out.println(parseCommand("date='${dt+1y}'", par));
//		System.out.println(parseCommand("date='${dt-1y}'", par));
//		System.out.println("sss"+parseCommand("date='${}'", par));
		System.out.println(parseCommand("ds='${dt,yyyyMM}", par));
//		System.out.println(parseCommand("20160517", par));
//		System.out.println(parseCommand("2016-05-17", par));
//		System.out.println(parseCommand("date='${-1d}'", par));
//		System.out.println(parseCommand("date='${-2m}'", par));
//		System.out.println(parseCommand("date='${-3y}'", par));
//		System.out.println(parseCommand("date='${1d}'", par));
//		System.out.println(parseCommand("date='${2m}'", par));
//		System.out.println(parseCommand("date='${3y}'", par));
//		System.out.println(parseCommand("hour='${hour1-1h}'", par));
//		System.out.println(parseCommand("hour='${hour1+1h}'", par));
//		System.out.println(parseCommand("hour='${hour1-30h}'", par));
//		System.out.println(parseCommand("hour='${hour1+30h}'", par));
//		System.out.println(parseCommand("hour='${hour2-1h}'", par));
//		System.out.println(parseCommand("hour='${hour2+1h}'", par));
//		System.out.println(parseCommand("hour='${hour2-30h}'", par));
//		System.out.println(parseCommand("hour='${hour2+30h}'", par));

	}

}
