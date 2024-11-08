# SQLInjectionParser

The repository contains a class to parse a generic SQL statement, extract conditions from the WHERE clause, 
and to create SQL query in a safer, parameterized way that prevents SQL injection. 
Handles varying numbers of columns and values in the WHERE clause.
Uses regex to extract the parameters and placeholders for the parameterized query, 
then uses a PreparedStatement to set the parameters.
It is accompanied by a unit tests to check that the main application works as expected. 