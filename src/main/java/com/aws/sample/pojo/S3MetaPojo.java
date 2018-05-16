package com.aws.sample.pojo;

import java.util.Date;

public class S3MetaPojo {

	public String fileName;
	public long size;
	public Date lastModified;
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long l) {
		this.size = l;
	}
	public Date getLastModified() {
		return lastModified;
	}
	public void setLastModified(Date lastModified) {
		this.lastModified = lastModified;
	}
	

	
	
}
