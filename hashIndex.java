public class hashIndex {

	private String BN_NAME;
	private int pageNo;
	private int recordNo;
	private int repeatRecord;
	
	public hashIndex(String BN_NAME,int pageNo,int recordNo)
	{
		this.BN_NAME=BN_NAME;
		this.pageNo=pageNo;
		this.recordNo=recordNo;
	}
	
	public hashIndex(String BN_NAME,int pageNo,int recordNo,int repeatRecord)
	{
		this.BN_NAME=BN_NAME;
		this.pageNo=pageNo;
		this.recordNo=recordNo;
		this.repeatRecord=repeatRecord;
	}
	
	public String getBN_NAME() {
		return BN_NAME;
	}

	public void setBN_NAME(String bN_NAME) {
		BN_NAME = bN_NAME;
	}

	public int getPageNo() {
		return pageNo;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	public int getRecordNo() {
		return recordNo;
	}

	public void setRecordNo(int recordNo) {
		this.recordNo = recordNo;
	}
	
	public int getRepeatRecord() {
		return repeatRecord;
	}

	public void setRepeatRecord(int repeatRecord) {
		this.repeatRecord = repeatRecord;
	}

}
