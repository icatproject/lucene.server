package org.icatproject.lucene.exceptions;

@SuppressWarnings("serial")
public class LuceneException extends Exception {

	private int httpStatusCode;
	private String message;

	public LuceneException(int httpStatusCode, String message) {
		this.httpStatusCode = httpStatusCode;
		this.message = message;
	}

	public String getShortMessage() {
		return message;
	}

	public int getHttpStatusCode() {
		return httpStatusCode;
	}

	public String getMessage() {
		return "(" + httpStatusCode + ") : " + message;
	}

}
