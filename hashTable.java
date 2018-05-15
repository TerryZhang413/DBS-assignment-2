

import java.util.ArrayList;

public class hashTable {

	private ArrayList<hashIndex> hashIndexList=new ArrayList<hashIndex>();
	
	public hashTable(ArrayList<hashIndex> hashIndexList)
	{
		this.setHashIndexList(hashIndexList);
	}


	public void addRecord(String BN_NAME,int pageNo,int recordNo)
	{
		hashIndex hashIndex=new hashIndex(BN_NAME,pageNo,recordNo);
		hashIndexList.add(hashIndex);
	}

	public ArrayList<hashIndex> getHashIndexList() {
		return hashIndexList;
	}


	public void setHashIndexList(ArrayList<hashIndex> hashIndexList) {
		this.hashIndexList = hashIndexList;
	}

}

