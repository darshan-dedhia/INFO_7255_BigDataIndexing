package edu.info7225.darshandedhia.bigdataindexing.Exceptions;

public class InternalServerErrorException extends Exception{
	
	public InternalServerErrorException() {
		super();
	}
	
	public InternalServerErrorException(String message) {
		super(message);
	}

	public InternalServerErrorException(String message, Throwable cause) {
		super(message, cause);
	}

	public InternalServerErrorException(Throwable cause) {
		super(cause);
	}
	
}
