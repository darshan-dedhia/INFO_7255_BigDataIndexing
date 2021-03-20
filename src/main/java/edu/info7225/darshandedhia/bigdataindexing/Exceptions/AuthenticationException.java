package edu.info7225.darshandedhia.bigdataindexing.Exceptions;

public class AuthenticationException extends Exception{
	public AuthenticationException() {
		super();
	}

	public AuthenticationException(String message) {
		super(message);
	}

	public AuthenticationException(String message, Throwable cause) {
		super(message, cause);
	}

	public AuthenticationException(Throwable cause) {
		super(cause);
	}
}
