package com.yoshio3.logging;

import java.util.logging.Level;

import org.slf4j.LoggerFactory;

import com.microsoft.azure.functions.ExecutionContext;

public final class BDLogger {

	private final org.slf4j.Logger logbackLogger;
	private final java.util.logging.Logger julLogger;

	BDLogger(final ExecutionContext context, Class<?> clazz) {
		julLogger = context.getLogger();
		logbackLogger = LoggerFactory.getLogger(clazz);
	}

    public void severe(String msg, Throwable th) {
		logbackLogger.info(msg, th);
		julLogger.log(Level.SEVERE, msg, th);
    }

	public void info(String msg) {
		logbackLogger.info(msg);
		julLogger.info(msg);
    }

	public void fine(String msg) {
		logbackLogger.debug(msg);
		julLogger.fine(msg);
    }

}
