package com.vinay.app;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to parse a generic SQL statement, extract conditions from the WHERE clause, 
 * and to create SQL query in a safer, parameterized way that prevents SQL injection. 
 * 
 * Handles varying numbers of columns and values in the WHERE clause.
 * 
 * Uses regex to extract the parameters and placeholders for the parameterized query, 
 * then uses a PreparedStatement to set the parameters.
 * 
 */
public class SQLParser {

	/**
	 * Helper class holds the parameterized query string and the list of parameters
	 * for setting up the PreparedStatement.
	 */
	public static class ParsedQuery {
		
		private final String parameterizedQuery;
		private final List<Object> parameters;

		public ParsedQuery(String parameterizedQuery, List<Object> parameters) {
			this.parameterizedQuery = parameterizedQuery;
			this.parameters = parameters;
		}

		public String getParameterizedQuery() {
			return parameterizedQuery;
		}

		public List<Object> getParameters() {
			return parameters;
		}
	}

	/**
	 * 
	 * Uses regex to find values in the WHERE clause. 
	 * It replaces them with ? place holders, creating a parameterized query. 
	 * It also extracts the values into a list called parameters.
	 * 
	 * @param query
	 * @return
	 */

	public static ParsedQuery parseSQLQuery(String query) {
		
		String parameterizedQuery = query;
		List<Object> parameters = new ArrayList<>();

		// Regex pattern to match different types of conditions
		
		Pattern pattern = Pattern.compile("\\b(\\w+)\\s+IN\\s*\\(([^)]+)\\)|" + // column IN (values) 1,2
				"\\b(\\w+)\\s*=\\s*('[^']*'|\\d+(?:\\.\\d+)?)|" + 				// column = value 3,4
				"\\b(\\w+)\\s*<\\s*('[^']*'|\\d+(?:\\.\\d+)?)|" + 				// column < value 5,6
				"\\b(\\w+)\\s*>\\s*('[^']*'|\\d+(?:\\.\\d+)?)|" + 				// column > value 7,8
				"\\b(\\w+)\\s*>=\\s*('[^']*'|\\d+(?:\\.\\d+)?)|" + 				// column >= value 9,10
				"\\b(\\w+)\\s*<=\\s*('[^']*'|\\d+(?:\\.\\d+)?)|" + 				// column <= value 11,12
				"\\b(\\w+)\\s*!=\\s*('[^']*'|\\d+(?:\\.\\d+)?)|" + 				// column = value 13,14
				"\\b(\\w+)\\s+BETWEEN\\s+('[^']*'|\\d+)\\s+AND\\s+('[^']*'|\\d+)" 	// column BETWEEN value1 AND value2
																					// 15, 16, 17
		);
		Matcher matcher = pattern.matcher(query);
		//
		// Process each condition, replacing with placeholders and adding values to
		// parameters list
		//
		while (matcher.find()) {
			
			if (matcher.group(1) != null) {
				
				// Handle "column IN (values)"
				
				String[] values = matcher.group(2).split(",");
				StringBuilder placeholders = new StringBuilder();
				for (String val : values) {
					val = val.trim();
					placeholders.append("?, ");
					parameters.add(parseValue(val));
				}
				placeholders.setLength(placeholders.length() - 2); // Remove last comma and space
				parameterizedQuery = parameterizedQuery.replaceFirst(Pattern.quote(matcher.group(2)), placeholders.toString());
				
			} else if (matcher.group(3) != null) {
				
				// Handle "column = value"
				
				String value = matcher.group(4).trim();
				parameterizedQuery = parameterizedQuery.replaceFirst(Pattern.quote(value), "?");
				parameters.add(parseValue(value));
				
			} else if (matcher.group(5) != null) {
				
				// Handle "column < value"
				
				String value = matcher.group(6).trim();
				parameterizedQuery = parameterizedQuery.replaceFirst(Pattern.quote(value), "?");
				parameters.add(parseValue(value));
				
			} else if (matcher.group(7) != null) {
				
				// Handle "column > value"
				
				String value = matcher.group(8).trim();
				parameterizedQuery = parameterizedQuery.replaceFirst(Pattern.quote(value), "?");
				parameters.add(parseValue(value));
				
			} else if (matcher.group(9) != null) {
				
				// Handle "column >= value"
				
				String value = matcher.group(10).trim();
				parameterizedQuery = parameterizedQuery.replaceFirst(Pattern.quote(value), "?");
				parameters.add(parseValue(value));
				
			} else if (matcher.group(11) != null) {
				
				// Handle "column <= value"
				String value = matcher.group(12).trim();
				parameterizedQuery = parameterizedQuery.replaceFirst(Pattern.quote(value), "?");
				parameters.add(parseValue(value));
				
			} else if (matcher.group(13) != null) {
				
				// Handle "column != value"
				
				String value = matcher.group(14).trim();
				parameterizedQuery = parameterizedQuery.replaceFirst(Pattern.quote(value), "?");
				parameters.add(parseValue(value));
				
			} else if (matcher.group(15) != null) {
				
				// Handle "column BETWEEN value1 AND value2"
				
				String value1 = matcher.group(16).trim();
				String value2 = matcher.group(17).trim();
				parameterizedQuery = parameterizedQuery.replaceFirst(Pattern.quote(value1 + " AND " + value2), "? AND ?");
				parameters.add(parseValue(value1));
				parameters.add(parseValue(value2));
			}
		}

		return new ParsedQuery(parameterizedQuery, parameters);
	}

	/**
	 * Parses a string value, handling single-quoted strings and numbers.
	 * 
	 * @param value
	 * @return
	 */
	private static Object parseValue(String value) {
		
		if (value.startsWith("'") && value.endsWith("'")) {
			
			return value.substring(1, value.length() - 1); // It's a string, strip quotes
			
		} else if (value.matches("-?\\d+(\\.\\d+)?")) { // Numeric values
			
			if (value.contains(".")) {
				return Double.parseDouble(value);
			} else {
				return Integer.parseInt(value);
			}
			
		} else {
			return value; // In case of unsupported types
		}
	}

	/**
	 * 
	 * Initializes a PreparedStatement with the parameterized query and then
	 * iterates through parameters to set each parameter in the PreparedStatement.
	 * 
	 * @param connection
	 * @param parsedQuery
	 * @return
	 * @throws SQLException
	 */
	public static PreparedStatement createPreparedStatement(Connection connection, ParsedQuery parsedQuery)
			throws SQLException {
		PreparedStatement preparedStatement = connection.prepareStatement(parsedQuery.getParameterizedQuery());

		// Set the extracted parameters into the PreparedStatement
		
		List<Object> parameters = parsedQuery.getParameters();
		for (int i = 0; i < parameters.size(); i++) {
			if (parameters.get(i) instanceof String) {
				preparedStatement.setString(i + 1, (String) parameters.get(i));
			} else if (parameters.get(i) instanceof Integer) {
				preparedStatement.setInt(i + 1, (Integer) parameters.get(i));
			} else if (parameters.get(i) instanceof Double) {
				preparedStatement.setDouble(i + 1, (Double) parameters.get(i));
			}
		}
		return preparedStatement;
	}

	/**
	 * 
	 * Returns a PreparedStatement
	 * 
	 * @param connection
	 * @param parsedQuery
	 * @return
	 * @throws SQLException
	 */
    public PreparedStatement prepareStatement(Connection connection, ParsedQuery parsedQuery) throws SQLException {
    	
        PreparedStatement preparedStmt = connection.prepareStatement(parsedQuery.getParameterizedQuery());
        
        for (int i = 0; i < parsedQuery.parameters.size(); i++) {
            preparedStmt.setObject(i + 1, parsedQuery.parameters.get(i));
        }
        return preparedStmt;
    }
}
