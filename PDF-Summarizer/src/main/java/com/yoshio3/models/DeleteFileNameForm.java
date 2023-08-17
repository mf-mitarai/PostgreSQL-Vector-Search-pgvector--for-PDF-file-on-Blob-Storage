package com.yoshio3.models;

import java.util.List;

public class DeleteFileNameForm {
	private boolean allDelete;
	private List<String> deleteFileNames;
	public boolean isAllDelete() {
		return allDelete;
	}
	public void setAllDelete(boolean allDelete) {
		this.allDelete = allDelete;
	}
	public List<String> getDeleteFileNames() {
		return deleteFileNames;
	}
	public void setDeleteFileNames(List<String> deleteFileNames) {
		this.deleteFileNames = deleteFileNames;
	}
}
