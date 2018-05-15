

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Hashtable;

public class hashquery {

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		long startTime=System.currentTimeMillis();
		long endTime;
		int pageSize=0;  //initial pagesize
		int inputLength=args.length;
		String queryKeyWord="";
		ArrayList<hashIndex> pageRecordNo=new ArrayList<hashIndex>();
		
		
		final int recordsAmount=2024631;
		int tableSize=1024 -1;
		double rate=0.9995;
		int bucketSize=(int) (( recordsAmount / rate ) / tableSize);
		
		
		hashquery hashquery=new hashquery();
						
		if(inputLength<2)  //invalid input 
		{
			System.err.println("invalid input: There should be a pagesize and a keyword!");
			System.exit(0);
		}	
		
		boolean sizeWord=true;  //if the page size is at first position
		try
		{
			pageSize=Integer.valueOf(args[0]);
			if(inputLength==2)
				queryKeyWord=args[1];
			else
			{
				queryKeyWord=args[1];
				for(int i=2;i<inputLength;i++)
				{
					queryKeyWord=queryKeyWord+" "+args[i];
				}
			}
		}
		catch(Exception e)
		{
			sizeWord=false;  //if not, try the other possibility
		}
		
		if(!sizeWord)  //try if the page size is at the last position
		{
			try
			{
				pageSize=Integer.valueOf(args[inputLength-1]);
				if(inputLength==2)
					queryKeyWord=args[0];
				else
				{
					queryKeyWord=args[0];
					for(int i=1;i<inputLength-1;i++)
					{
						queryKeyWord=queryKeyWord+" "+args[i];
					}
				}
			}
			catch(Exception e)
			{
				System.err.println("No pageSize found in input!");  //if not, there is no page size in input. kill the program
				System.exit(0);
			}
		}
		
		System.out.println("Query Keyword is:" + queryKeyWord);	
		int hashIndex=queryKeyWord.hashCode();
		hashIndex=hashquery.hashCodeRestrict(hashIndex, tableSize);  //get hash index by hashCode();
		System.out.println("Hash index of the Keyword is:" + hashIndex);
		
		pageRecordNo=hashquery.getPageRecordNo(pageSize, hashIndex, queryKeyWord , bucketSize,tableSize);
		
		if(pageRecordNo.size()!=0) //if there is on page number return, find it in heap file
		{
			int pageNo=0;
			int recordNo=0;
			for(int i=0;i<pageRecordNo.size();i++)
			{
				pageNo=pageRecordNo.get(i).getPageNo();
				recordNo=pageRecordNo.get(i).getRecordNo();
				hashquery.printInfo(queryKeyWord, pageNo, recordNo, pageSize);
			}
			endTime=System.currentTimeMillis();
			System.out.println("Number of milliseconds is: "+ (endTime-startTime)+ "ms");
		}					
		else //after end or break of loop, check if there is a page number return
		{
			System.out.println("The keyword is not found in hash index, please check and try again");
			endTime=System.currentTimeMillis();
			System.out.println("Number of milliseconds is: "+ (endTime-startTime)+ "ms");
			System.exit(0);
		}

	}
	
	// compute hash code
	public int hashCodeRestrict(int hashCode,int tableSize)
	{
		hashCode=Math.abs(hashCode)%tableSize;
		return hashCode;
	}
	
	// view hash file to get a arraylist containing all matched record's pageNo and recordNo
	@SuppressWarnings("unchecked")
	public ArrayList<hashIndex> getPageRecordNo(int pageSize,int hashIndex,String queryKeyWord, int bucketSize,int tableSize ) throws IOException, ClassNotFoundException
	{
		ArrayList<hashIndex> pageRecordNo= new ArrayList<hashIndex>();
		int countRecord=bucketSize;
		
		while(countRecord>=bucketSize)
		{
			String line = "";
			countRecord=0;
			//Hashtable<String,String> table=new Hashtable<String,String>();
			File file=new File("hash"+pageSize+"/hashtable"+hashIndex);
			//FileInputStream fis=new FileInputStream(file);
			//ObjectInputStream in=new ObjectInputStream(fis);
			BufferedReader in=new BufferedReader(new FileReader(file));
			while((line = in.readLine())!=null)
			{ 
				countRecord++;  //count how many records in this hash table
		      	String[] item = line.split(",");//split the csv file by tab
		      	if(item[0].equals(queryKeyWord))
		      	{
		      		pageRecordNo.add(new hashIndex(item[0],Integer.valueOf(item[1]),Integer.valueOf(item[2])));
		      		System.out.println(item[0]+" is found in hash table No:"+ hashIndex);
		    		//System.out.println(pageRecordNo[0]+"    "+pageRecordNo[1]);
		      	}
			//table=(Hashtable<String, String>) in.readObject();
			//pageNo=table.get(queryKeyWord);
			}			
			in.close();	
			
		//	if(pageRecordNo.size()==0 && countRecord<bucketSize)
		//		System.out.println("Hash table No:"+ hashIndex +" has been viewed as not full and no found.");
		//	else if (pageRecordNo.size()==0 && countRecord>=bucketSize)
		//		System.out.println("Hash table No:"+ hashIndex +" has been viewed as full and no found. Search next hash table!");
			if(countRecord>=bucketSize)
				System.out.println("Hash table No:"+ hashIndex +" has been viewed as full. Search next hash table!");
			else
				System.out.println("Hash table No:"+ hashIndex +" has been viewed as not full. End the query.");
			hashIndex++;
			if(hashIndex==tableSize)
				hashIndex=0; 
		}
		return pageRecordNo;
	}
	
	// print the records
	public boolean printInfo(String queryKeyWord,int pageNumber,int recordNo,int pageSize)
	{
		boolean judgement=false;
		//int recordNumber;
		byte[] pageContent;
		pageContent=readSpecificPage(pageSize,pageNumber);
		
		if(pageContent != null) //if the page is not empty 
		{
			//recordNumber=getRecordNumber(pageContent); //get how many records in this page
			
			//ArrayList<byte[]> recordList=getRecord(recordNumber, pageContent);  //push records in a arraylist

				//System.out.println("number:"+i);
			byte[] record = null;
			record=getRecord(pageContent,recordNo,pageSize);
			//System.out.println("This BN_NAME is founded as "+getBN_NAME(recordList.get(i))); 
			System.out.println("All information is below:");
			System.out.println(getOtherInfo(record));
			System.out.println();
			judgement=true;	
		}
		else
			System.err.println("The page is empty!");
		

		if(judgement==false)
		System.out.println("There is no record matched!");  //error report
		
		return judgement;
	}
	
	//read specific page and return byte[] for whole page
	public byte[] readSpecificPage(int pageSize,int PageNo)
	{
		int pageIndex=0;
		String address="heap."+pageSize;
		File file = new File(address);
		FileInputStream fileInputStream = null;
		try {
			fileInputStream = new FileInputStream(file);  //read heap file
		} catch (FileNotFoundException e1) {
			System.err.println("The heap file does not exist!");
		}
		
		byte[] page = new byte[pageSize];
		try {
			fileInputStream.skip(PageNo*pageSize);
			System.out.println();
			System.out.println("Heap File page No:"+PageNo+" is viewed!");
			if(fileInputStream.read(page,pageIndex, pageSize)!=-1) //read new 4096 bytes content
				return page;
			else
				return null; //if it is empty, return null
		} catch (IOException e) {
			return null;
		}	
	}
		
	//get the specific record
	public byte[] getRecord(byte[] pageContent,int recordNo,int pageSize)
	{
		byte[] recordContet = null;
		int recordLength;
		byte[] recordNumberbyte = new byte[4];
		byte[] startLocation = new byte[2];
		byte[] endLocation = new byte[2];
	    int startRecordLocation;
	    int endRecordLocation = 0;
	    int recordNumber;
		
	    System.arraycopy(pageContent,0,recordNumberbyte,0,4);
	    recordNumber=byteArrayToInt(recordNumberbyte);
	    /*	    System.out.println(byteArrayToInt(recordNumberbyte));
	    for(int i=0;i<byteArrayToInt(recordNumber);i++)
	    {
		    System.arraycopy(pageContent,4+i*2,startLocation,0,2);
			System.arraycopy(pageContent,4+(i+1)*2,endLocation,0,2);
			startRecordLocation=byteArrayToShort(startLocation);
			System.out.println("start: "+startRecordLocation);	
			endRecordLocation=byteArrayToShort(endLocation);
			System.out.println("end: "+endRecordLocation);
	    }*/
	    
	    System.arraycopy(pageContent,4+recordNo*2,startLocation,0,2);
	    if(recordNo!=recordNumber-1)   //judge if it is the last record in the page
	    	{
	    	System.arraycopy(pageContent,4+(recordNo+1)*2,endLocation,0,2);
	    	endRecordLocation=byteArrayToShort(endLocation);
	    	}
	    else 
	    	endRecordLocation=pageSize;
	    
	    
		startRecordLocation=byteArrayToShort(startLocation);
		//System.out.println(startRecordLocation);	
		
		//System.out.println(endRecordLocation);
		recordLength=endRecordLocation-startRecordLocation;
		recordContet=new byte[recordLength];
		
		System.arraycopy(pageContent,startRecordLocation,recordContet,0,recordLength);
		
		return recordContet;
	}
		
	//get other info in the record
	public String getOtherInfo(byte[] record)
	{
		int startFieldLocation=18;
		int endFieldLocation = 0;
		int fieldLength;
		byte[] startLocation = new byte[2];
		byte[] endLocation = new byte[2];
		String info="";
		
		for(int i=0;i<8;i++)
		{
			//System.out.println("loop: "+i*2);
			System.arraycopy(record,i*2,startLocation,0,2);
			startFieldLocation=byteArrayToShort(startLocation);	
			//System.out.println("start "+startFieldLocation);
			
			System.arraycopy(record,(i+1)*2,endLocation,0,2);
			endFieldLocation=byteArrayToShort(endLocation);
			//System.out.println("end " +endFieldLocation);
			
			fieldLength=endFieldLocation-startFieldLocation;
			
			//System.out.println("field endloaction is "+endFieldLocation);

			byte[] field = new byte[fieldLength];
			System.arraycopy(record,startFieldLocation,field,0,fieldLength);
			info+= new String(field)+" ";
		}
		
		byte[] LongContent=new byte[8];
		System.arraycopy(record,endFieldLocation,LongContent,0,8);
		
		
		return info+byteArrayToLong(LongContent);
	}
	
	
	//convert byte[] to int
	public static int byteArrayToInt(byte[] b) {   
		return   b[3] & 0xFF |   
		            (b[2] & 0xFF) << 8 |   
		            (b[1] & 0xFF) << 16 |   
		            (b[0] & 0xFF) << 24;   
		}  
	
	//convert byte[] to short
	public static int byteArrayToShort(byte[] b) {   
		return   b[1] & 0xFF |   
		            (b[0] & 0xFF) << 8; 
		} 
	
	
	//convert byte[] to long
    public long byteArrayToLong(byte[] b) { 
        long result = 0; 
        for (int i = 0; i < 8; i++) { 
            result <<= 8; 
            result |= (b[i] & 0xff); 
        } 
        return result; 
    } 
}
