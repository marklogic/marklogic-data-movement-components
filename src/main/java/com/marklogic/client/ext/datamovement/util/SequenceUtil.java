package com.marklogic.client.ext.datamovement.util;

public abstract class SequenceUtil {

	public static String arrayToSequence(String... values) {
		final String delimiter = "\", \"";
		StringBuilder sb = new StringBuilder("(\"");
		for (int i = 0; i < values.length; i++) {
			if (i > 0) {
				sb.append(delimiter);
			}
			String value = values[i];
			if (value != null && value.contains("\"")) {
				throw new IllegalArgumentException("Cannot add value with double quotes to sequence: " + value);
			}
			sb.append(value);
		}
		sb.append("\")");
		return sb.toString();
	}

	public static String arrayToSequenceSingleQuoted(String... values) {
		StringBuilder sb = new StringBuilder("('");
		if (values != null) {
			for (int i = 0; i < values.length; i++) {
				if (i > 0) {
					sb.append("', '");
				}
				sb.append(values[i]);
			}
		}
		sb.append("')");
		return sb.toString();
	}
}
