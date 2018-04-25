package assignment2;

import java.util.Hashtable;

public class hashTable {

	private String hashIndex;
	private String BN_NAME;
	private Hashtable<String, String> hashTable=new Hashtable<String,String>();
	
	
	public hashTable(Hashtable<String, String> hashTable)
	{
		this.setHashTable(hashTable);
	}

	public String getHashIndex() {
		return hashIndex;
	}

	public void setHashIndex(String hashIndex) {
		this.hashIndex = hashIndex;
	}

	public String getBN_NAME() {
		return BN_NAME;
	}

	public void setBN_NAME(String bN_NAME) {
		BN_NAME = bN_NAME;
	}

	public Hashtable<String, String> getHashTable() {
		return hashTable;
	}

	public void setHashTable(Hashtable<String, String> hashTable) {
		this.hashTable = hashTable;
	}
	
	
}

