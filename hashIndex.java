import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;


public class hashIndex {

	public static void main(String[] args) {
		
		long startTime=System.currentTimeMillis();
		long endTime;		
		//int tableSize=1024;
		//int[] hashCount=new int[tableSize];
		//int max=0;
		//int min=99999999;
		int range;
		int rangeMin=2000;
		
		
		for(int tableSize=200;tableSize<2000;tableSize++)
		{
			int recordAmount=0;
			int max=0;
			int min=99999999;
			int[] hashCount=new int[tableSize];
			for(int i=0;i<tableSize;i++)
			{
				hashCount[i]=0;
			}
	
			String fileAddress=args[2];	          //file address
			
			BufferedReader reader = null;
			PrintStream p=null;
			FileOutputStream fs=null;
			
			try {
					reader = new BufferedReader(new FileReader(fileAddress));
					fs = new FileOutputStream(new File("D:\\hashIndex.txt"));
					p = new PrintStream(fs);
					reader.readLine(); //ignore first line(title)
					String line = "";
				
					while((line = reader.readLine())!=null)
					{ 
				      	String[] item = line.split("\t");//split the csv file by tab
				      	
				      	if (item.length<9) //if columns is not 9,delete this useless line 
						{continue;}
				      	recordAmount++;
				      	int hashIndex=hashCode(item[1],tableSize);
				      	hashCount[hashIndex]++;
				      	
						//p.println(item[1]+" "+hashIndex+ "The block's amount is: "+hashCount[hashIndex]);
					}
				} catch (Exception e) {
			e.printStackTrace();
		}
			
			
			for(int i=0;i<tableSize;i++)
			{
				p.println("block "+i+" has "+hashCount[i]+ " buckets!");
				if (hashCount[i]>max)
					max=hashCount[i];
				if (hashCount[i]<min)
					min=hashCount[i];
			}
			
			range=max-min;
			if(range<rangeMin)
				{
				rangeMin=range;
				double bucketAmount=recordAmount/tableSize;
				double pro=rangeMin/bucketAmount*100;
				System.out.print("rangeMin: "+rangeMin);
				System.out.print("   Bucket Size: "+bucketAmount);
				System.out.print("   Proportion: "+pro+"%");
				System.out.println("   Block Size: "+tableSize);
				
				
				}
			//System.out.println("Block size:");
			//System.out.println("Min:"+min);
			//System.out.println("Max:"+max);
			
			//endTime=System.currentTimeMillis();
			//System.out.println("Number of milliseconds is: "+ (endTime-startTime)+ "ms");
		}
}

	
	public static int hashCode(String str,int tableSize)
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
