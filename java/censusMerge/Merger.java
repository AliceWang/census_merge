package censusMerge;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import com.linuxense.javadbf.*;

/**	
 * Copyright (C) 2009
 * @author Joshua Justice
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *	The JavaDBF library is licensed under the GNU Lesser General Public License.
 *	A copy of the GNU LGPL should be included in the merge-xbase folder.
 *	If it is not, it can be found on http://www.gnu.org/licenses/lgpl-3.0.txt .
 *	For details regarding JavaDBF, see http://javadbf.sarovar.org/
 *	
 *
*/
public class Merger {
	FileInputStream shapeFile;
	RandomAccessFile popFile1=null;
	RandomAccessFile popFile2=null;
	RandomAccessFile popGeo;
	File combinedFile;
	HashMap<String, Object[]> popRecords;
	boolean useRace;
	private int countyShape;
	private int countyPop=1;
	private int tractShape;
	private int tractPop=2;
	private int blockShape;
	private int blockPop=4;
	private static final int PRELIMFIELDS = 297;
	
	/**Combines Census population files with the Census shapefile DBF.
	 * @param s The shapefile's location, as a String.
	 * @param g The population header file's location, as a String. 
	 * @param useRaceData Whether or not to use race data. A boolean.
	 * Joshua.Justice@gatech.edu
	 */
	public Merger(String s, String g, boolean useRaceData){
		try{
			shapeFile = new FileInputStream(s);
			
		}catch(FileNotFoundException e){
			throw new IllegalArgumentException("Shapefile does not exist");
		}try{
			popGeo = new RandomAccessFile(new File(g), "r");
		}catch(FileNotFoundException e){
			throw new IllegalArgumentException("Popfile does not exist");
		}
		
		String c = "";
		useRace = useRaceData;
		if(useRace){
			c = s.replace(".dbf", "combinedall.dbf");
		}else{
			c = s.replace(".dbf", "combinednd.dbf");
		}
		combinedFile = new File(c);
	}
	
	/**Combines Census population files with the Census shapefile DBF.
	 * @param s The shapefile.
	 * @param g The population header file. 
	 * @param useRaceData Whether or not to use race data. A boolean.
	 * Joshua.Justice@gatech.edu
	 */
	public Merger(File s, File g, boolean useRaceData){
		try{
			shapeFile = new FileInputStream(s);
			popGeo = new RandomAccessFile(g, "r");
		}catch(FileNotFoundException e){
			throw new IllegalArgumentException("File does not exist");
		}
		String c = "";
		useRace = useRaceData;
		if(useRace){
			c = s.getPath().replace(".dbf", "combinedall.dbf");
		}else{
			c = s.getPath().replace(".dbf", "combinednd.dbf");
		}
		combinedFile = new File(c);
		
	}

	/** 
	 * @param p1 The "00001" population file's location, as a String.
	 * @param p2 The "00002" population file's location, as a String.
	 * 
	 * If the racial data is desired, pass in the population filenames here.
	 * The 00001 file is whole population, 00002 file is 18+.
	 * Right now, it is assumed that if you want one, you want both, thus both are required.
	 */
	public void setPopFiles(String p1, String p2){
		try{
			popFile1 = new RandomAccessFile(new File(p1), "r");
			popFile2 = new RandomAccessFile(new File(p2), "r");
		}catch(FileNotFoundException e){
			throw new IllegalArgumentException("File does not exist");
		}
	}

	/** 
	 * @param p1 The "00001" population file.
	 * @param p2 The "00002" population file.
	 * 
	 * If the racial data is desired, pass in the population filenames here.
	 * The 00001 file is whole population, 00002 file is 18+.
	 * Right now, it is assumed that if you want one, you want both, thus both are required.
	 */
	public void setPopFiles(File p1, File p2){
		try{
			popFile1 = new RandomAccessFile(p1, "r");
			popFile2 = new RandomAccessFile(p2, "r");
		}catch(FileNotFoundException e){
			throw new IllegalArgumentException("File does not exist");
		}
	}
	
	/**Sets up the list of fields we get from the population data.*/
	private DBFField[] setUpAllPopFields(){
		DBFField fields[] = new DBFField[297];
		/* There are 71+73 fields in each pop file plus 
		 * 9 we're taking from the geo file.*/
		
		//Field definitions
		for(int i=0; i<PRELIMFIELDS; i++){
			fields[i] = new DBFField();
			fields[i].setDataType(DBFField.FIELD_TYPE_C);
		}
		// These field lengths come from Census documentation.
		// www.census.gov/prod/www/abs/pl94-171.pdf
		fields[0].setName("LOGRECNO");
		fields[0].setFieldLength(7);
		fields[1].setName("COUNTY");
		fields[1].setFieldLength(3);
		fields[2].setName("TRACT");
		fields[2].setFieldLength(6);
		fields[3].setName("BLKGRP");
		fields[3].setFieldLength(1);
		fields[4].setName("BLOCK");
		fields[4].setFieldLength(4);
		fields[5].setName("AREALAND");
		fields[5].setFieldLength(14);
		fields[6].setName("AREAWATER");
		fields[6].setFieldLength(14);
		fields[7].setName("NAME");
		fields[7].setFieldLength(90);
		fields[8].setName("POP100");
		fields[8].setFieldLength(9);
		for(int i=9; i<PRELIMFIELDS; i++){
			//I am not typing out all the racial field names.
			fields[i].setName(""+i);
			fields[i].setFieldLength(9); //Since it can't be higher than POP100
		}
		fields[9].setName("TBL1STRT");
		fields[79].setName("TBL2STRT");
		fields[152].setName("TBL3STRT");
		fields[223].setName("TBL4STRT");
		return fields;
	}
	/**Sets up the list of fields we get from the header data.*/
	private DBFField[] setUpHeaderFields(){
		DBFField fields[] = new DBFField[9];
		
		//Field definitions
		for(int i=0; i<9; i++){
			fields[i] = new DBFField();
			fields[i].setDataType(DBFField.FIELD_TYPE_C);
		}
		
		// These field lengths come from Census documentation.
		// www.census.gov/prod/www/abs/pl94-171.pdf
		fields[0].setName("LOGRECNO");
		fields[0].setFieldLength(7);
		fields[1].setName("COUNTY");
		fields[1].setFieldLength(3);
		fields[2].setName("TRACT");
		fields[2].setFieldLength(6);
		fields[3].setName("BLKGRP");
		fields[3].setFieldLength(1);
		fields[4].setName("BLOCK");
		fields[4].setFieldLength(4);
		fields[5].setName("AREALAND");
		fields[5].setFieldLength(14);
		fields[6].setName("AREAWATER");
		fields[6].setFieldLength(14);
		fields[7].setName("NAME");
		fields[7].setFieldLength(90);
		fields[8].setName("POP100");
		fields[8].setFieldLength(9);
		return fields;
	}
	
	/** The fields (in shapeFields) are as follows:
	 * State block fields: 10
	 * STATEFP00 COUNTYFP00 TRACTCE00 BLOCKCE00 BLKIDFP00
	 * NAME00 MTFCC00 UR00 UACE00 FUNCSTAT00 
	 * State tract fields: 8
	 * STATEFP00 COUNTYFP00 TRACTCE00 CTIDFP00
	 * NAME00 NAMELSAD00 MTFCC00 FUNCSTAT00
	 * County block fields: 10
	 * STATEFP00 COUNTYFP00 TRACTCE00 BLOCKCE00 BLKIDFP00
	 * NAME00 MTFCC00 UR00 UACE00 FUNCSTAT00
	 * County tract fields: 8
	 * STATEFP00 COUNTYFP00 TRACTCE00 CTIDFP00
	 * NAME00 NAMELSAD00 MTFCC00 FUNCSTAT00
	 * 
	 *  The code to find the matching integer is going to make global variables.
	 *  This is so it only has to do it once, instead of on every record.
	 */
	private void setUpFieldNumbers(DBFField[] shapeFields){
		for(int i=0; i<shapeFields.length; i++){
			if(shapeFields[i].getName().equals("COUNTYFP00")){
				countyShape=i;
			}
			if(shapeFields[i].getName().equals("TRACTCE00")){
				tractShape=i;
			}
			if(shapeFields[i].getName().equals("BLOCKCE00")){
				blockShape=i;
			}
		}
	}
	
	/** Gets a Object[] record from the geofile.*/
	private Object[] getRecordFromGeo(String geoLine){
		// These are from the Geo file.
		// See Census documentation for file info.
		// www.census.gov/prod/www/abs/pl94-171.pdf
		if(geoLine != null && geoLine.length()>301){
			Object[] record = new Object[9];
			record[0]=geoLine.substring(18, 18+7); //LOGRECNO
			record[1]=geoLine.substring(31, 31+3); //COUNTY
			record[2]=geoLine.substring(55, 55+6); //TRACT
			record[3]=geoLine.substring(61, 61+1); //BLKGRP
			record[4]=geoLine.substring(62, 62+4); //BLOCK
			record[5]=geoLine.substring(172, 172+14); //AREALAND
			record[6]=geoLine.substring(186, 186+14); //AREAWATR
			record[7]=geoLine.substring(200, 200+90); //NAME
			record[8]=geoLine.substring(292, 292+9); //POP100
			return record;
		}else{
			return null;
		}
	}
	
	/**
	 * This function should only be called if there is already a record with the header data.
	 * Adds the racial data to the header record.
	 * Yes, there are four tables in the race data.
	 * However, the data from each pair of tables are on the same line.
	 * That is to say:
	 * 00001 has both non-hispanic and hispanic data on same line.
	 * 00002 has both non-hispanic 18+ and hispanic 18+ on same line.
	 */
	private Object[] addRaceToGeo(Object[] geoRecord, String file1Line, String file2Line){
		String[] csv1 = file1Line.split(",");
		String[] csv2 = file2Line.split(",");
		int curpos=9;
		Object[] record = new Object[PRELIMFIELDS];
		System.arraycopy(geoRecord, 0, record, 0, 9);
		//We already have 9 fields from the header file.
		//5 is the first population number.
		for(int i=5; i<csv1.length; i++){
			record[curpos]=csv1[i];
			curpos++;
		}
		for(int i=5; i<csv2.length; i++){
			record[curpos]=csv2[i];
			curpos++;
		}
		return record;
	}
	
	/** 
	 *  Merges the files in-memory. 
	 *  Requires the heap size to be large enough to handle the entire state. 
	 *  
	 * 	The code only cares about what rows are in the shapefile.
	 *  So if you want county data, merge the county shapefile with the
	 *  state population header file. 
	 *  You'll get a combined file that only has the county data.
	 */
	public void merge(){
		if(useRace==true && (popFile1 == null || popFile2 == null)){
			throw new IllegalArgumentException("Cannot use racial data without first setting the files.");
		}
		//set up the DBFWriter
		DBFWriter writer = null;
		if(combinedFile.exists()){
			System.out.println("This file already exists.");
			return;
		}
		try{
			writer = new DBFWriter(combinedFile);
		}catch(DBFException e){
			System.out.println("Sync mode failed.");
		}
		//set up the shapeReader
		DBFReader shapeReader = null;
		try{
			shapeReader = new DBFReader(shapeFile);
		}catch(DBFException e){
			System.out.println("Cannot set up the DBF reader.");
			return;
		}
		//Setting field information.
		int gFields = 0;
		int sFields = 0;
		DBFField[] geoFields = null;
		if(useRace){
			geoFields = setUpAllPopFields();
		}else{
			geoFields = setUpHeaderFields();
		}
		try{
			sFields = shapeReader.getFieldCount();
		}catch(DBFException e){
			System.out.println("Couldn't get the field count!");
			return;
		}
		gFields = geoFields.length;
		DBFField[] shapeFields = new DBFField[sFields];
		DBFField[] combFields = new DBFField[sFields+gFields];
		
		try{
			for(int i=0; i<sFields; i++){
				combFields[i]=shapeReader.getField(i);
				shapeFields[i]=shapeReader.getField(i);
			}
			for(int i=sFields; i<sFields+gFields; i++){
				combFields[i]=geoFields[i-sFields];
			}
		}catch(DBFException e){
			System.out.println("Couldn't get the field contents.");
			return;
		}
		try{
			writer.setFields(combFields);
		}catch(DBFException e){
			System.out.println("Failed to set the fields!");
			return;
		}

		Object[] shapeRecord = null;
		Object[] popRecord = null;
		Object[] combRecord = new Object[gFields+sFields];
		
		setUpFieldNumbers(shapeFields); // so it's available in the inner loop
		
		/* Outer loop: scan through the records in the shapefile.
		 * Inner loop: Scan through the population file to get the matching information.
		 */
		int rCount = shapeReader.getRecordCount();
		setUpHashMap(useRace, rCount);
		for(int i=0; i<rCount; i++){
			try{
				shapeRecord = shapeReader.nextRecord();
			}catch(DBFException e){
				System.out.println("Cannot get next record in shapefile.");
				return;
			}
			if(shapeRecord==null){
				return;
			}
			popRecord = findMatchingHashRecord(shapeRecord);
			if (popRecord==null){
				System.out.println("Merger has failed!");
				return;
			}
			System.arraycopy(shapeRecord, 0, combRecord, 0, sFields);
			System.arraycopy(popRecord, 0, combRecord, sFields, gFields);
			try{
				writer.addRecord(combRecord);
			}catch(DBFException e){
				System.out.println("Failed to add record!");
				System.exit(-1);
			}
		}
		try{
			writer.write();
		}catch(DBFException e){
			System.out.println("Failed to finalize file");
		}
		
	}
	
	/**
	 * Set up a hashmap so we can have a faster matching algorithm.
	 * The string key is the concatenation of:
	 * county+tract+block
	 * This allows for much faster lookup (total of linear time). 
	 * 
	 * However, due to memory restrictions, we can't really do it for the whole state.
	 * If you really want to try, pass in "null" for countyNo.
	 * Make sure to increase the JVM max heap size!
	 * 
	 */
	private void setUpHashMap(boolean useRace, int capacity){
		popRecords = new HashMap<String, Object[]>(capacity);
		try{
			popGeo.seek(0);
			if(useRace){
				popFile1.seek(0);
				popFile2.seek(0);
			}
		}catch(IOException e){
			System.out.println("Could not set up HashMap.");
			return;
		}
		String geoLine="";
		String pop1Line="";
		String pop2Line="";
		String key="";
		Object[] record = null;
		boolean finished=false;
		int count=0;
		while(!finished){
			try{
				geoLine=popGeo.readLine();
				if(useRace){
					pop1Line=popFile1.readLine();
					pop2Line=popFile2.readLine();
				}
			}catch(IOException e){
				finished=true;
			}
			if(geoLine==null){
				finished=true;
			}
			record = getRecordFromGeo(geoLine);
			if(useRace){
				record = addRaceToGeo(record, pop1Line, pop2Line);
			}
			if(record==null){
				finished=true;
			}else{
				key = ""+record[countyPop]+record[tractPop]+record[blockPop];
				popRecords.put(key, record);
				count++;
			}
		}
		return;
	}
	
	private Object[] findMatchingHashRecord(Object[] shapeRecord){
		String key = ""+shapeRecord[countyShape]+shapeRecord[tractShape]+shapeRecord[blockShape];
		return popRecords.get(key);
	}
	
	/** This is used to make life easier in the main method.*/
	public static HashMap<String,String[]> createMappings(){
		HashMap<String, String[]> mappings = new HashMap<String, String[]>();
		String[] al = {"Alabama", "al"}; mappings.put("01", al);
		String[] ak = {"Alaska", "ak"};	mappings.put("02",ak);
		//You know, my life would be a lot easier if we didn't have missing states.
		String[] az = {"Arizona", "az"}; mappings.put("04",az);
		String[] ar = {"Arkansas", "ar"}; mappings.put("05",ar);
		String[] ca = {"California", "ca"}; mappings.put("06",ca); 
		String[] co = {"Colorado", "co"}; mappings.put("08",co);
		String[] ct = {"Connecticut", "ct"}; mappings.put("09",ct); 
		String[] de = {"Delaware", "de"}; mappings.put("10",de);
		String[] dc = {"District_of_Columbia", "dc"}; mappings.put("11",dc); 
		String[] fl = {"Florida", "fl"}; mappings.put("12",fl);
		String[] ga = {"Georgia", "ga"}; mappings.put("13",ga); 
		String[] hi = {"Hawaii", "hi"}; mappings.put("15",hi);
		String[] id = {"Idaho", "id"}; mappings.put("16",id); 
		String[] il = {"Illinois", "il"}; mappings.put("17",il);
		String[] in = {"Indiana", "in"}; mappings.put("18",in); 
		String[] ia = {"Iowa", "ia"}; mappings.put("19",ia);
		String[] ks = {"Kansas", "ks"}; mappings.put("20",ks); 
		String[] ky = {"Kentucky", "ky"}; mappings.put("21",ky);
		String[] la = {"Louisiana", "la"}; mappings.put("22",la); 
		String[] me = {"Maine", "me"}; mappings.put("23",me);
		String[] md = {"Maryland", "md"}; mappings.put("24",md);
		String[] ma = {"Massachusetts", "ma"}; mappings.put("25",ma);
		String[] mi = {"Michigan", "mi"}; mappings.put("26",mi); 
		String[] mn = {"Minnesota", "mn"}; mappings.put("27",mn);
		String[] ms = {"Mississippi", "ms"}; mappings.put("28",ms);
		String[] mo = {"Missouri", "mo"}; mappings.put("29",mo);
		String[] mt = {"Montana", "mt"}; mappings.put("30",mt);
		String[] ne = {"Nebraska", "ne"}; mappings.put("31",ne);
		String[] nv = {"Nevada", "nv"}; mappings.put("32",nv);
		String[] nh = {"New_Hampshire", "nh"}; mappings.put("33",nh);
		String[] nj = {"New_Jersey", "nj"}; mappings.put("34",nj);
		String[] nm = {"New_Mexico", "nm"}; mappings.put("35",nm);
		String[] ny = {"New_York", "ny"}; mappings.put("36",ny);
		String[] nc = {"North_Carolina", "nc"}; mappings.put("37",nc);
		String[] nd = {"North_Dakota", "nd"}; mappings.put("38",nd);
		String[] oh = {"Ohio", "oh"}; mappings.put("39",oh);
		String[] ok = {"Oklahoma", "ok"}; mappings.put("40",ok);
		String[] or = {"Oregon", "or"}; mappings.put("41",or);
		String[] pa = {"Pennsylvania", "pa"}; mappings.put("42",pa);
		String[] ri = {"Rhode_Island", "ri"}; mappings.put("44",ri);
		String[] sc = {"South_Carolina", "sc"}; mappings.put("45",sc);
		String[] sd = {"South_Dakota", "sd"}; mappings.put("46",sd);
		String[] tn = {"Tennessee", "tn"}; mappings.put("47",tn);
		String[] tx = {"Texas", "tx"}; mappings.put("48",tx);
		String[] ut = {"Utah", "ut"}; mappings.put("49",ut);
		String[] vt = {"Vermont", "vt"}; mappings.put("50",vt);
		String[] va = {"Virginia", "va"}; mappings.put("51",va);
		String[] wa = {"Washington", "wa"}; mappings.put("53",wa);
		String[] wv = {"West_Virginia", "wv"}; mappings.put("54",wv);
		String[] wi = {"Wisconsin", "wi"}; mappings.put("55",wi);
		String[] wy = {"Wyoming", "wy"}; mappings.put("56",wy);
		return mappings;
	}
	
	private static void mergerHelp(){
		System.out.println("Usage:\n" +
				"This program also takes two arguments on the command line.\n" +
				"The first argument is the folder containing the census population data.\n" +
				"The second argument is the folder containing the census state shapefiles.\n" +
				"The folder and file names need to be identical to those downloaded\n" +
				"from the Census website. This will merge all 50 states plus DC.\n" + 
				"If you wish to only merge specific data, use this as a library.");
	}
	
	public static void main(String[] args){
		/**Merges all 50 states, given only two directory strings in the program arguments.*/
		
		if(args == null || args.length<2 || args[0]==null || args[1]==null){
			System.out.println("Copyright (C) 2009 Joshua Justice. Licensed under the GNU GPL.\n");
			mergerHelp();
			return;
		}
		HashMap<String, String[]> mappings = createMappings();
		File popDir = new File(args[0]);
		File shpDir = new File(args[1]);
		
		for(String stateNo: mappings.keySet()){
			System.out.println("Beginning mergers on: " + mappings.get(stateNo)[0]);
			String sf = "tl_2008_" + stateNo + "_tabblock00.dbf";
			File shapeFile = new File(shpDir, sf);
			File popState = new File(popDir, mappings.get(stateNo)[0]);
			File popGeo = new File(popState, mappings.get(stateNo)[1]+"geo.upl");
			File popFile1 = new File(popState, mappings.get(stateNo)[1]+"00001.upl");
			File popFile2 = new File(popState, mappings.get(stateNo)[1]+"00002.upl");
			Merger m_nd = new Merger(shapeFile, popGeo, false);
			Merger m_all = new Merger(shapeFile, popGeo, true);
			m_all.setPopFiles(popFile1, popFile2);
			m_nd.merge();
			m_all.merge();
		}
		/* Sample main method merge code
		Merger mb = new Merger("/path/to/shapefile.dbf",
				"/path/to/stategeo.upl", false);
		mb.merge();
		*/
	}
}