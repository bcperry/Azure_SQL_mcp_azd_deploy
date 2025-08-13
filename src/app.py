import os
from pydantic import Field
from SqlDB import SqlDatabase
from mcp.server.fastmcp import FastMCP
import dotenv

dotenv.load_dotenv(".env")

connection_string = os.environ["AZURE_SQL_CONNECTIONSTRING"]


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

@mcp.tool()
async def create_table(query: str = Field(description="CREATE TABLE SQL statement")) -> str:
    """Create a table in the SQL database"""
    if not query.strip().upper().startswith("CREATE TABLE"):
        raise ValueError("Only CREATE TABLE statements are allowed")
    db._execute_query(query)
    return f"Table created successfully."

@mcp.tool()
async def write_query(query: str = Field(description="SQL query to execute")) -> str:
    """Execute an INSERT, UPDATE, or DELETE query on the SQL database"""
    if query.strip().upper().startswith("SELECT"):
        raise ValueError("SELECT queries are not allowed for write_query")
    results = db._execute_query(query)
    return str(results)

@mcp.tool()
async def read_query(query: str = Field(description="SELECT SQL query to execute")) -> str:
    """Execute a SELECT query on the SQL database"""
    if not query.strip().upper().startswith("SELECT"):
        raise ValueError("Only SELECT queries are allowed for read_query")
    results = db._execute_query(query)
    return str(results)


if __name__ == "__main__":
    mcp.run(transport="streamable-http")