package com.qfree.obotest.rest;

public class ReST {

	/**
	 * Extracts and returns  the ReST API version from the HTTP "Accept" header.
	 * 
	 * This method expects ReST API version to be embedded in the HTTP "Accept"
	 * header as a media type parameter with name "v". The accept header must
	 * thereofre be of the form:
	 * 
	 *     ...;v=version...
	 * 
	 * Here "v" is the parameter name and "version" is the version that will be 
	 * returned.
	 * 
	 * @param httpAcceptHeader
	 * @return
	 */
	public static String extractAPIVersion(String httpAcceptHeader) {
		String apiVersion = "";

		for (String token : httpAcceptHeader.split(";")) {
			String[] potentialKeyValuePair = token.split("=");
			if (potentialKeyValuePair.length == 2) {
				String key = potentialKeyValuePair[0].trim();
				if (key.equals("v")) {
					apiVersion = potentialKeyValuePair[1].trim();
					break;
				}
			}
		}
		return apiVersion;
	}
}
