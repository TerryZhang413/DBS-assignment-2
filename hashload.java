package assignment2;

import java.io.*;
import java.util.ArrayList;

public class hashload {
	
	public static void main(String[] args) throws IOException {
		
		hashload hashload=new hashload();
		long startTime=System.currentTimeMillis();
		long endTime;
		int pageSize = 0;
		byte[] pageContent;
		int recordNumber;
		final int recordsAmount=2024631;
		int tableSize=1024 -1;
		double rate=0.996;
		int bucketSize=(int) (( recordsAmount / rate ) / tableSize);
		//int bucketSize=2050; // Amount of records*(1/0.7) /tableSize
		//Hashtable<String, String> hashTable=new Hashtable<String,String>();
		ArrayList<hashTable> hashTableList=new ArrayList<hashTable>();
		
		//creat hash table and add them to the list
		for(int i=0;i<tableSize;i++)
		{
			hashTable hashTable=new hashTable(new ArrayList<hashIndex>());
			hashTableList.add(hashTable);
		}
			
		
		String outputAddress = null;
		FileOutputStream fos;
		BufferedWriter out = null;
		//ObjectOutputStream out = null;
				
		try
			{
				pageSize=Integer.valueOf(args[0]); //get the pagesize in order to direct at the target heap file
				outputAddress="hash"+pageSize;
			}
		catch(Exception e)
			{
				System.err.println("The input as pagesize should be an integer, please check and try again.");
				System.exit(0); //If input is not a number, kill the program
			}
		
		FileInputStream fileInputStream=hashload.readHeapfile(pageSize);
			
		
		if(fileInputStream!=null)
		{
			int pageCount=0;
			while(true)
			{
				pageContent= hashload.readNextPage(fileInputStream,pageSize); //get next page 
				
				if(pageContent != null) //if the page is not empty 
				{
					recordNumber=hashload.getRecordNumber(pageContent); //get how many records in this page
					
					ArrayList<byte[]> recordList=hashload.getRecord(recordNumber, pageContent,pageSize);  //push records in a arraylist
					for(int i=0;i<recordList.size();i++)
					{
						String BN_NAME=hashload.getBN_NAME(recordList.get(i));
						int hashIndex=BN_NAME.hashCode();
						//int hashIndex=hashload.hashCode(BN_NAME,tableSize);
						//System.out.println(BN_NAME+": "+pageCount);
						hashIndex=hashload.hashCodeRestrict(hashIndex, tableSize);
						
						int recordNo=i; //store the locaton of the record in the specific page
						if(hashTableList.get(hashIndex).getHashIndexList().size()<bucketSize)
							hashTableList.get(hashIndex).addRecord(BN_NAME, pageCount, recordNo);
						else
						{
							while(hashTableList.get(hashIndex).getHashIndexList().size()>=bucketSize) //if the bucket is full
							{
								if(hashIndex!=tableSize-1)
								{
									hashIndex++; //store the data in next bucket
								}
								else
								{
									hashIndex=0; //printed to the first hash table
								}
							}
							hashTableList.get(hashIndex).addRecord(BN_NAME, pageCount, recordNo); //put the BN_NAME and pageNo in the specific hashtable
							//hashTable.put(BN_NAME, ""+pageCount);
						}
					}
					pageCount++;
				}
				else
					break;
			}
			
			File folder=new File(outputAddress);

			if(!folder.exists()){
			folder.mkdirs();
			}
			
			for(int i=0;i<tableSize;i++)
			{
				int hashIndex=i;
				File file=new File(outputAddress+"/hashtable"+hashIndex);
				try {
					//fos=new FileOutputStream(file);
					out=new BufferedWriter(new FileWriter(file));
					//out=new ObjectOutputStream(fos);
					//out.writeObject(hashTableList.get(hashIndex).getHashIndexList());
					for(int j=0;j<hashTableList.get(hashIndex).getHashIndexList().size();j++)
					{
						out.write(hashTableList.get(hashIndex).getHashIndexList().get(j).getBN_NAME());
						//System.out.print(hashTableList.get(hashIndex).getHashIndexList().get(j).getBN_NAME());
						out.write(","+hashTableList.get(hashIndex).getHashIndexList().get(j).getPageNo());
						//System.out.print(hashTableList.get(hashIndex).getHashIndexList().get(j).getPageNo());
						out.write(","+hashTableList.get(hashIndex).getHashIndexList().get(j).getRecordNo());
						//System.out.print(hashTableList.get(hashIndex).getHashIndexList().get(j).getRecordNo()+"");
						out.newLine();
						//System.out.println();
						}
				} catch (Exception e) {
					e.printStackTrace();
				}finally {
					out.flush();
				}
				System.out.println("Hash table "+i+" has been built!");
			}
			out.close();
			endTime=System.currentTimeMillis();
			System.out.println("Number of milliseconds is: "+ (endTime-startTime)+ "ms");
		}
		else
		{
			System.err.println("The file is empty, please check and try again.");
		}
		
	}


	public FileInputStream readHeapfile(int pageSize)
	{
		FileInputStream fileInputStream=null;
		File file = new File("heap."+pageSize);			
		try {
			fileInputStream = new FileInputStream(file); //get the stream from heap file
		} catch (FileNotFoundException e) {
			System.err.println("No such heap file, program is closed, please check the pagesize and try again!");
			System.exit(0); //If no such file, kill the program
			}  
			return fileInputStream;
		}
	
	
	public byte[] readNextPage(FileInputStream fileInputStream,int pageSize)
	{
		int pageIndex=0;
		byte[] page = new byte[pageSize];
		try {
			if(fileInputStream.read(page, pageIndex, pageSize)!=-1) //read new 4096 bytes content
				return page;
			else
				return null; //if it is empty, return null
		} catch (IOException e) {
			return null;
		}
	}
	
	public int getRecordNumber(byte[] pageContent)
	{
		byte[] pageSizeByte = new byte[4];		
		System.arraycopy(pageContent,0,pageSizeByte,0,4);
		int recordNumber=byteArrayToInt(pageSizeByte);
		//System.out.println(recordNumber);
		return recordNumber;
	}
	
	//get records in a page
	public ArrayList<byte[]> getRecord(int recordNumber,byte[] pageContent,int pageSize)
	{
		ArrayList<byte[]> recordList = new ArrayList<byte[]>();
		
		int recordLength;
		byte[] startLocation = new byte[2];
		byte[] endLocation = new byte[2];
	    int startRecordLocation;
	    int endRecordLocation = 0;
	    
		//get record by using index
		for(int i=0;i<recordNumber-1;i++)  
		{
			//System.out.println(i+" :");
			System.arraycopy(pageContent,4+i*2,startLocation,0,2);
			System.arraycopy(pageContent,4+(i+1)*2,endLocation,0,2);
			startRecordLocation=byteArrayToShort(startLocation);
			//System.out.println(startRecordLocation);			
			endRecordLocation=byteArrayToShort(endLocation);
			//System.out.println(endRecordLocation);
			recordLength=endRecordLocation-startRecordLocation;
			byte[] record = new byte[recordLength];
			System.arraycopy(pageContent,startRecordLocation,record,0,recordLength);
			recordList.add(record);
		}
		
		byte[] record = new byte[pageSize-endRecordLocation];
		System.arraycopy(pageContent,endRecordLocation,record,0,pageSize-endRecordLocation);   //get last record
		recordList.add(record);
		
		return recordList;
	}
	
	//get BN_NAME in a record for checking
	public String getBN_NAME(byte[] record)
	{
		int startFieldLocation;
		int endFieldLocation;
		int fieldLength;
		byte[] startLocation = new byte[2];
		byte[] endLocation = new byte[2];
		
		for(int i=0;i<9;i++)
		{
			System.arraycopy(record,2*i,startLocation,0,2);
			startFieldLocation=byteArrayToShort(startLocation);
			//System.out.println(startFieldLocation);
		}
		
	
		System.arraycopy(record,2,startLocation,0,2);
		startFieldLocation=byteArrayToShort(startLocation);
		//System.out.println(startFieldLocation);
		
		System.arraycopy(record,4,endLocation,0,2);
		endFieldLocation=byteArrayToShort(endLocation);
		//System.out.println(endFieldLocation);
		
		fieldLength=endFieldLocation-startFieldLocation;
		//System.out.println(fieldLength);
		byte[] field = new byte[fieldLength];
		System.arraycopy(record,startFieldLocation,field,0,fieldLength);
		String fieldContent=new String(field);
		//System.out.println(fieldContent);
		return fieldContent;
	}
	
	public static int byteArrayToInt(byte[] b) {   
		return   b[3] & 0xFF |   
		            (b[2] & 0xFF) << 8 |   
		            (b[1] & 0xFF) << 16 |   
		            (b[0] & 0xFF) << 24;   
		}  
	
	public static int byteArrayToShort(byte[] b) {   
		return   b[1] & 0xFF |   
		            (b[0] & 0xFF) << 8; 
		} 
	
	
	public int hashCodeRestrict(int hashCode,int tableSize)
	{
		hashCode=Math.abs(hashCode)%tableSize;
		return hashCode;
	}
	
	
	public int hashCode(String str,int tableSize)
	{
		char[] charArray=str.toCharArray();
		int hash=(int)charArray[0];
		for(int i=0;i<charArray.length-1;i++)
		{
			hash=hash*31+(int)charArray[i+1];
			hash=hash%tableSize;
		}			
		return hash;
	}
}
