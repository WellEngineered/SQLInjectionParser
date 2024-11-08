package com.vinay.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

public class SQLParserTest {

    @Test
    public void testParseSQLQuery_SimpleEquals_Integer() {
        String sqlQuery = "SELECT * FROM orders WHERE order_id=5";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE order_id=?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_SimpleEquals_Double() {
        String sqlQuery = "SELECT * FROM orders WHERE order_id=5.0";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE order_id=?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5.0), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_SimpleEquals_String() {
        String sqlQuery = "SELECT * FROM orders WHERE order_id='abc'";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE order_id=?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList("abc"), parsedQuery.getParameters());
    }

    @Test
    public void testParseSQLQuery_SimpleNotEquals_Integer() {
        String sqlQuery = "SELECT * FROM orders WHERE order_id != 5";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE order_id != ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_SimpleNotEquals_Double() {
        String sqlQuery = "SELECT * FROM orders WHERE order_id != 5.0";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE order_id != ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5.0), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_SimpleNotEquals_String() {
        String sqlQuery = "SELECT * FROM orders WHERE order_id != 'abc'";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE order_id != ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList("abc"), parsedQuery.getParameters());
    }
    @Test
    public void testParseSQLQuery_INClause_String() {
        String sqlQuery = "SELECT * FROM orders WHERE status IN ('complete', 'incomplete')";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE status IN (?, ?)", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList("complete", "incomplete"), parsedQuery.getParameters());
    }

    @Test
    public void testParseSQLQuery_INClause_Integer() {
        String sqlQuery = "SELECT * FROM orders WHERE status IN (212,3434)";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE status IN (?, ?)", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(212, 3434), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_INClause_Double() {
        String sqlQuery = "SELECT * FROM orders WHERE status IN (212.45,3434.6754)";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE status IN (?, ?)", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(212.45,3434.6754), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_INClause_Mixed() {
        String sqlQuery = "SELECT * FROM orders WHERE status IN (212.45,3434,'zzz')";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE status IN (?, ?, ?)", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(212.45,3434,"zzz"), parsedQuery.getParameters());
    }


    @Test
    public void testParseSQLQuery_GreaterThan_Integer() {
        String sqlQuery = "SELECT * FROM orders WHERE zzz > 5";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE zzz > ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5), parsedQuery.getParameters());
    }
    

    @Test
    public void testParseSQLQuery_GreaterThan_Double() {
        String sqlQuery = "SELECT * FROM orders WHERE zzz > 5.2";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE zzz > ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5.2), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_GreaterThan_Date() {
        String sqlQuery = "SELECT * FROM orders WHERE req_time > '12/01/2022 08:00:00'";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE req_time > ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList("12/01/2022 08:00:00"), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_GreaterThanEquals_Integer() {
        String sqlQuery = "SELECT * FROM orders WHERE zzz >= 5";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE zzz >= ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5), parsedQuery.getParameters());
    }
    

    @Test
    public void testParseSQLQuery_GreaterThanEquals_Double() {
        String sqlQuery = "SELECT * FROM orders WHERE zzz >= 5.2";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE zzz >= ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5.2), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_GreaterThanEquals_Date() {
        String sqlQuery = "SELECT * FROM orders WHERE req_time >= '12/01/2022 08:00:00'";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE req_time >= ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList("12/01/2022 08:00:00"), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_LessThan_Integer() {
        String sqlQuery = "SELECT * FROM orders WHERE req_count < 5";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE req_count < ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5), parsedQuery.getParameters());
    }

    @Test
    public void testParseSQLQuery_LessThan_Double() {
        String sqlQuery = "SELECT * FROM orders WHERE req_count < 5.2";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE req_count < ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5.2), parsedQuery.getParameters());
    }

    @Test
    public void testParseSQLQuery_LessThanEquals_Integer() {
        String sqlQuery = "SELECT * FROM orders WHERE req_status <= 5";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE req_status <= ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_LessThanEquals_Double() {
        String sqlQuery = "SELECT * FROM orders WHERE req_status <= 5.5";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE req_status <= ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(5.5), parsedQuery.getParameters());
    }
    
   @Test
    public void testParseSQLQuery_Between() {
        String sqlQuery = "SELECT * FROM orders WHERE req_date BETWEEN '11/01/2022 08:00:00' AND '10/01/2022 08:00:00'";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE req_date BETWEEN ? AND ?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList("11/01/2022 08:00:00", "10/01/2022 08:00:00"), parsedQuery.getParameters());
    }

    @Test
    public void testParseSQLQuery_ComplexQuery() {
        String sqlQuery = "SELECT * FROM orders WHERE order_id=5 AND user_name='abc' AND status IN ('complete', 'incomplete') " +
                "AND order_number IN (5, 7) AND req_time >= '12/01/2022 08:00:00' " +
                "AND req_date BETWEEN '11/01/2022 08:00:00' AND '10/01/2022 08:00:00' AND req_status <= 5 AND req_count < 5";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM orders WHERE order_id=? AND user_name=? AND status IN (?, ?) " +
                "AND order_number IN (?, ?) AND req_time >= ? AND req_date BETWEEN ? AND ? AND req_status <= ? AND req_count < ?", parsedQuery.getParameterizedQuery());
        List<Object> expectedParams = Arrays.asList(5, "abc", "complete", "incomplete", 5, 7, "12/01/2022 08:00:00",
                "11/01/2022 08:00:00", "10/01/2022 08:00:00", 5, 5);
        assertEquals(expectedParams, parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_ComplexQuery2() {
        String sqlQuery = "SELECT *\n"
        		+ "FROM   order\n"
        		+ "WHERE  order_id = 5\n"
        		+ "       AND user_name = 'abc'\n"
        		+ "       AND status IN ( 'complete', 'incomplete' )\n"
        		+ "       AND order_number IN ( 5, 7 )\n"
        		+ "       AND req_time >= '12/01/2022 08:00:00'\n"
        		+ "       AND req_time2 <= '10/01/2022 08:00:00'\n"
        		+ "       AND req_date BETWEEN '11/01/2022 08:00:00' AND '10/01/2022 08:00:00'\n"
        		+ "       AND req_status <= 5\n"
        		+ "       AND req_count < 5\n"
        		+ "       AND temp < 3.2\n"
        		+ "       AND age <= 4.2\n"
        		+ "       AND req_status2 >= 9\n"
        		+ "       AND req_count2 > 10\n"
        		+ "       AND temp2 > 11.2\n"
        		+ "       AND age2 >= 12.2\n"
        		+ "       AND bit != 8 ";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("SELECT * FROM order WHERE order_id = ? AND user_name = ? AND status IN (?, ?) AND order_number IN (?, ?) AND req_time >= ? AND req_time2 <= ? AND req_date BETWEEN ? AND ? AND req_status <= ? AND req_count < ? AND temp < ? AND age <= ? AND req_status2 >= ? AND req_count2 > ? AND temp2 > ? AND age2 >= ? AND bit != ?".replaceAll("\\s+", " ").trim(), parsedQuery.getParameterizedQuery().replaceAll("\\s+", " ").trim());
        
        List<Object> expectedParams = Arrays.asList(5, "abc", "complete", "incomplete", 5, 7, "12/01/2022 08:00:00","10/01/2022 08:00:00",
                "11/01/2022 08:00:00", "10/01/2022 08:00:00", 5, 5, 3.2, 4.2, 9, 10 ,11.2, 12.2, 8);
       assertEquals(expectedParams, parsedQuery.getParameters());
    }
    
    
    @Test
    public void testParseSQLQuery_ComplexQuery_Join() {
        String sqlQuery = "SELECT OI.ORDER_ID,\n"
        		+ "       C.CUSTOMER_ID,\n"
        		+ "       CONCAT(C.CUSTOMER_FNAME, \" \", C.CUSTOMER_LNAME) AS 'CUSTOMER_FULL_NAME',\n"
        		+ "       SUM(OI.PRODUCT_QUANTITY) AS 'TOTAL_QUANTITY'\n"
        		+ " FROM online_customer C\n"
        		+ "    INNER JOIN ORDER_HEADER OH\n"
        		+ "        ON C.CUSTOMER_ID = OH.CUSTOMER_ID\n"
        		+ "    INNER JOIN order_items OI\n"
        		+ "        ON OH.ORDER_ID = OI.ORDER_ID\n"
        		+ " WHERE OH.ORDER_ID > 10060\n"
        		+ "      AND OH.ORDER_STATUS = 'Shipped'\n"
        		+ " GROUP BY OI.ORDER_ID\n"
        		+ " HAVING TOTAL_QUANTITY > 15";
        
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);
        
        String expectedQuery = "SELECT OI.ORDER_ID,\n"
        		+ "       C.CUSTOMER_ID,\n"
        		+ "       CONCAT(C.CUSTOMER_FNAME, \" \", C.CUSTOMER_LNAME) AS 'CUSTOMER_FULL_NAME',\n"
        		+ "       SUM(OI.PRODUCT_QUANTITY) AS 'TOTAL_QUANTITY'\n"
        		+ " FROM online_customer C\n"
        		+ "    INNER JOIN ORDER_HEADER OH\n"
        		+ "        ON C.CUSTOMER_ID = OH.CUSTOMER_ID\n"
        		+ "    INNER JOIN order_items OI\n"
        		+ "        ON OH.ORDER_ID = OI.ORDER_ID\n"
        		+ " WHERE OH.ORDER_ID > ?\n"
        		+ "      AND OH.ORDER_STATUS = ?\n"
        		+ " GROUP BY OI.ORDER_ID\n"
        		+ " HAVING TOTAL_QUANTITY > ?";

        assertEquals(expectedQuery, parsedQuery.getParameterizedQuery());
        
        List<Object> expectedParams = Arrays.asList(10060, "Shipped", 15);
        
       assertEquals(expectedParams, parsedQuery.getParameters());
    }
    
    
    @Test
    public void testParseSQLQuery_Update() {
        String sqlQuery = "UPDATE EMPLOYEE SET PHONENO='4657' WHERE EMPNO='000010'";
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);

        assertEquals("UPDATE EMPLOYEE SET PHONENO=? WHERE EMPNO=?", parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList("4657", "000010" ), parsedQuery.getParameters());
    }
    
    @Test
    public void testParseSQLQuery_Update_Complex() {
        String sqlQuery = "UPDATE o \n"
        		+ "SET total_orders = 7 \n"
        		+ "FROM orders o \n"
        		+ "INNER JOIN order_details od \n"
        		+ "    ON o.order_id = od.order_id \n"
        		+ "WHERE customer_name = 'Jack' ";
        
        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery(sqlQuery);
        
        String expectedQuery = "UPDATE o \n"
        		+ "SET total_orders = ? \n"
        		+ "FROM orders o \n"
        		+ "INNER JOIN order_details od \n"
        		+ "    ON o.order_id = od.order_id \n"
        		+ "WHERE customer_name = ? ";

        assertEquals(expectedQuery, parsedQuery.getParameterizedQuery());
        assertEquals(Arrays.asList(7, "Jack" ), parsedQuery.getParameters());
    }
    
    @Test
    public void testPrepareStatement() throws SQLException {
    	
        Connection mockConnection = mock(Connection.class);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);

        SQLParser.ParsedQuery parsedQuery = SQLParser.parseSQLQuery("SELECT * FROM orders WHERE order_id = 5 AND user_name = 'abc'");
        
        SQLParser sqlParser = new SQLParser();
        
        PreparedStatement stmt = sqlParser.prepareStatement(mockConnection, parsedQuery);

        verify(mockPreparedStatement, times(1)).setObject(1, 5);
        verify(mockPreparedStatement, times(1)).setObject(2, "abc");
        assertEquals(mockPreparedStatement, stmt);
    }
    
}
