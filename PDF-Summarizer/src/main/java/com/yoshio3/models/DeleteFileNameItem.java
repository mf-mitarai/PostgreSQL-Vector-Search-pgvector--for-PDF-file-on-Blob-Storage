package com.yoshio3.models;

public class DeleteFileNameItem {
	private int fileId;
	private String fileName;
	public DeleteFileNameItem(int fileId, String fileName) {
		this.fileId = fileId;
		this.fileName = fileName;
	}
	public int getFileId() {
		return fileId;
	}
	public void setFileId(int fileId) {
		this.fileId = fileId;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
}
