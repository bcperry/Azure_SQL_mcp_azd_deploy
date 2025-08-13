import os
from pydantic import Field
from SqlDB import SqlDatabase
from mcp.server.fastmcp import FastMCP
import dotenv

# Load environment variables from .env file (for local development)
dotenv.load_dotenv(".env")

# Get connection string from environment
connection_string = os.environ.get("AZURE_SQL_CONNECTIONSTRING")
print(f"Connected using: {connection_string}")
if not connection_string:
    raise ValueError("AZURE_SQL_CONNECTIONSTRING environment variable is required")


# app = FastAPI()
mcp = FastMCP("AzureSQL")

# from sqlite_db import SqliteDatabase
db = SqlDatabase(connection_string)


@mcp.tool()
async def list_tables() -> str:
    """List all tables in the SQL database"""

    query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
    results = db._execute_query(query)
    return str(results)

@mcp.tool()
async def describe_table(table_name: str = Field(description="Name of the table to describe")) -> str:
    """Get the schema information for a specific table"""
    if table_name is None:
        raise ValueError("Missing table_name argument")
    results = db._execute_query(
        f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE \
        FROM INFORMATION_SCHEMA.COLUMNS \
        WHERE TABLE_NAME = '{table_name}';"
            )
    return str(results)

# @mcp.tool()
# async def create_table(query: str = Field(description="CREATE TABLE SQL statement")) -> str:
#     """Create a table in the SQL database"""
#     if not query.strip().upper().startswith("CREATE TABLE"):
#         raise ValueError("Only CREATE TABLE statements are allowed")
#     db._execute_query(query)
#     return f"Table created successfully."

@mcp.tool()
async def write_query(query: str = Field(description="SQL query to execute")) -> str:
    """Execute an INSERT or UPDATE query on existing tables only"""
    
    # Normalize the query for checking
    normalized_query = query.strip().upper()
    
    # Only allow INSERT and UPDATE statements
    allowed_operations = ["INSERT", "UPDATE"]
    if not any(normalized_query.startswith(op) for op in allowed_operations):
        raise ValueError("Only INSERT and UPDATE queries are allowed for write_query")
    
    # Block dangerous keywords that could modify schema or users
    dangerous_keywords = [
        "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "MERGE", 
        "EXEC", "EXECUTE", "CALL", "GRANT", "REVOKE", "COMMIT", 
        "ROLLBACK", "SAVEPOINT", "USER", "LOGIN", "ROLE", "PERMISSION",
        "SCHEMA", "DATABASE", "INDEX", "TRIGGER", "PROCEDURE", "FUNCTION"
    ]
    
    for keyword in dangerous_keywords:
        if keyword in normalized_query:
            raise ValueError(f"Keyword '{keyword}' is not allowed - only data modifications on existing tables permitted")
    
    # Execute the query
    results = db._execute_query(query)
    return str(results)

@mcp.tool()
async def read_query(query: str = Field(description="SELECT SQL query to execute")) -> str:
    """Execute a SELECT query on the SQL database"""
    
    # Normalize the query for checking
    normalized_query = query.strip().upper()
    
    # Only allow SELECT statements
    if not normalized_query.startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed for read_query")
    
    # Block dangerous keywords that could modify data or schema
    dangerous_keywords = [
        "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", 
        "TRUNCATE", "MERGE", "EXEC", "EXECUTE", "CALL", "GRANT", 
        "REVOKE", "COMMIT", "ROLLBACK", "SAVEPOINT"
    ]
    
    for keyword in dangerous_keywords:
        if keyword in normalized_query:
            raise ValueError(f"Keyword '{keyword}' is not allowed in read-only queries")
    
    # Execute the query
    results = db._execute_query(query)
    return str(results)


if __name__ == "__main__":
    mcp.run(transport="streamable-http")