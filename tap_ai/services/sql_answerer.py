# tap_ai/services/sql_answerer.py
"""
Text-to-SQL Engine - Enhanced with User Context
Generates SQL queries with user-specific filtering (batch, grade)
FIXED: Proper table name escaping for tables with spaces
"""

import json
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.infra.sql_catalog import load_schema


def _llm(model: str = "gpt-4o", temperature: float = 0.0) -> ChatOpenAI:
    """Initializes the Language Model client."""
    api_key = get_config("openai_api_key")
    return ChatOpenAI(model_name=model, openai_api_key=api_key, temperature=temperature, max_tokens=2000)


def _build_schema_context(schema: Dict[str, Any], user_profile: Optional[Dict] = None) -> str:
    """
    Builds a compact schema representation with user context hints.
    
    Args:
        schema: Database schema dictionary
        user_profile: User profile for context-aware schema building
    
    Returns:
        Formatted schema string for LLM prompt
    """
    tables = schema.get("tables", {})
    links = schema.get("allowed_joins", [])
    
    lines = ["DATABASE SCHEMA:\n"]
    
    # Add user context hints if available
    if user_profile:
        lines.append("USER CONTEXT:")
        if user_profile.get('batch'):
            lines.append(f"  - User's Batch: {user_profile['batch']}")
        if user_profile.get('grade'):
            lines.append(f"  - User's Grade: {user_profile['grade']}")
        if user_profile.get('current_enrollment'):
            enrollment = user_profile['current_enrollment']
            if enrollment.get('course'):
                lines.append(f"  - User's Course: {enrollment['course']}")
        lines.append("")
        lines.append("IMPORTANT: When filtering by batch or grade, use the user's context values above.")
        lines.append("")
    
    # Add tables
    for table_name, table_info in tables.items():
        doctype = table_info.get("doctype", table_name.replace("tab", "", 1))
        cols = table_info.get("columns", [])
        pk = table_info.get("pk", "name")
        display = table_info.get("display_field")
        
        lines.append(f"Table: {table_name}")
        lines.append(f"  DocType: {doctype}")
        lines.append(f"  Primary Key: {pk}")
        if display:
            lines.append(f"  Display Field: {display}")
        lines.append(f"  Columns: {', '.join(cols[:30])}")  # Limit to 30 columns
        lines.append("")
    
    # Add relationships
    if links:
        lines.append("RELATIONSHIPS (Foreign Keys):")
        for link in links[:50]:  # Limit to 50 relationships
            lines.append(
                f"  {link['left_table']}.{link['left_key']} -> "
                f"{link['right_table']}.{link['right_key']}"
            )
    
    return "\n".join(lines)


SQL_GENERATION_PROMPT = """You are an expert SQL query generator for a Frappe/ERPNext database.

Given:
1. A database schema with tables, columns, and relationships
2. A natural language question
3. Optional user context (batch, grade, course)
4. Optional conversation history

Generate a VALID MariaDB SQL query that answers the question.

CRITICAL RULES:
1. Use ONLY tables and columns that exist in the schema
2. Table names MUST start with "tab" (e.g., "tabStudent", "tabVideoClass")
3. **IMPORTANT**: If a table name contains spaces, wrap it in backticks in the entire query
   - Example: `tabCourse Verticals` NOT tabCourse Verticals
   - Example: `tabGrade Course Level Mapping` NOT tabGrade Course Level Mapping
4. Column names must be exact matches from the schema
5. Use proper JOINs based on the relationships provided
6. If user context is provided (batch, grade), ADD appropriate WHERE clauses
7. Return ONLY valid SQL - no explanations, no markdown
8. Use LIMIT to prevent huge result sets (max 100 rows)
9. For Student queries with batch context, filter by batch automatically
10. For content queries with grade context, filter by grade automatically

TABLE NAME EXAMPLES:
✅ CORRECT: `tabCourse Verticals` (with backticks)
❌ WRONG: tabCourse Verticals (without backticks - will cause syntax error)

✅ CORRECT: `tabGrade Course Level Mapping` (with backticks)
❌ WRONG: tabGrade Course Level Mapping (without backticks - will cause syntax error)

FILTERING GUIDELINES:
- If user has a batch and query involves Student data: WHERE batch = 'user_batch'
- If user has a grade and query involves content: WHERE grade = 'user_grade'
- If user is in a specific course and query involves course content: WHERE course = 'user_course'

Return ONLY the SQL query, nothing else.
"""


def _generate_sql_query(
    query: str,
    user_profile: Optional[Dict] = None,
    content_details: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Generates a SQL query from a natural language question with user context.
    
    Args:
        query: Natural language question
        user_profile: User profile for context-aware filtering
        content_details: Content details for content-specific queries
    
    Returns:
        Dictionary with 'sql' and 'explanation' keys
    """
    llm = _llm()
    schema = load_schema()
    schema_str = _build_schema_context(schema, user_profile)
    
    # Build context-aware prompt
    user_prompt_parts = [f"QUESTION: {query}"]
    
    # Add user context
    if user_profile:
        user_context_parts = []
        if user_profile.get('batch'):
            user_context_parts.append(f"User Batch: {user_profile['batch']}")
        if user_profile.get('grade'):
            user_context_parts.append(f"User Grade: {user_profile['grade']}")
        if user_profile.get('current_enrollment'):
            enrollment = user_profile['current_enrollment']
            if enrollment.get('course'):
                user_context_parts.append(f"User Course: {enrollment['course']}")
        
        if user_context_parts:
            user_prompt_parts.append(f"\nUSER CONTEXT: {', '.join(user_context_parts)}")
            user_prompt_parts.append("IMPORTANT: Use the user context to filter results appropriately.")
            user_prompt_parts.append("REMEMBER: Wrap table names with spaces in backticks (`)!")
    
    # Add content context
    if content_details:
        content_context = f"Content: {content_details.get('title')} (Type: {content_details.get('type')})"
        user_prompt_parts.append(f"\nCONTENT CONTEXT: {content_context}")
    
    user_prompt = "\n".join(user_prompt_parts)
    
    try:
        messages = [
            ("system", SQL_GENERATION_PROMPT),
            ("system", schema_str),
            ("user", user_prompt),
        ]
        
        resp = llm.invoke(messages)
        sql_text = getattr(resp, "content", "").strip()
        
        # Clean up markdown if present
        if sql_text.startswith("```sql"):
            sql_text = sql_text[6:]
        if sql_text.startswith("```"):
            sql_text = sql_text[3:]
        if sql_text.endswith("```"):
            sql_text = sql_text[:-3]
        
        sql_text = sql_text.strip()
        
        print(f"> Generated SQL (with user context):\n{sql_text}")
        
        return {"sql": sql_text, "explanation": "SQL generated with user context"}
        
    except Exception as e:
        frappe.log_error(f"SQL generation failed: {e}")
        return {"sql": None, "explanation": str(e)}


def _execute_sql(sql_query: str) -> List[Dict[str, Any]]:
    """
    Executes a SQL query and returns results.
    
    Args:
        sql_query: SQL query string
    
    Returns:
        List of result dictionaries
    """
    try:
        return frappe.db.sql(sql_query, as_dict=True)
    except Exception as e:
        frappe.log_error(f"SQL execution failed for query: {sql_query}", f"Error: {e}")
        return []


def _synthesize_answer(
    query: str,
    sql_query: str,
    results: List[Dict[str, Any]],
    user_profile: Optional[Dict] = None,
    chat_history: Optional[List[Dict[str, str]]] = None
) -> str:
    """
    Synthesizes a natural language answer from SQL results with user context.
    
    Args:
        query: Original user question
        sql_query: SQL query that was executed
        results: Query results
        user_profile: User profile for personalized responses
        chat_history: Conversation history
    
    Returns:
        Natural language answer
    """
    llm = _llm(model="gpt-4o-mini", temperature=0.2)
    
    chat_history = chat_history or []
    
    system_prompt = (
        "You are a helpful assistant. The user asked a question, a SQL query was run, and here are the results. "
        "Based on the conversation history, user context, and the data, formulate a friendly, natural language answer."
    )
    
    # Build user context string
    user_context_str = ""
    if user_profile:
        context_parts = [f"User: {user_profile.get('name', 'Student')}"]
        if user_profile.get('grade'):
            context_parts.append(f"Grade {user_profile['grade']}")
        if user_profile.get('batch'):
            context_parts.append(f"Batch {user_profile['batch']}")
        user_context_str = " | ".join(context_parts)
    
    # Build history string
    history_str = ""
    if chat_history:
        history_str = "\n".join([f"{turn['role'].title()}: {turn['content']}" for turn in chat_history])
    
    user_prompt_parts = []
    
    if history_str:
        user_prompt_parts.append(f"CONVERSATION HISTORY:\n---\n{history_str}\n---\n")
    
    if user_context_str:
        user_prompt_parts.append(f"USER CONTEXT: {user_context_str}\n")
    
    user_prompt_parts.append(f"QUESTION: {query}\n")
    user_prompt_parts.append(f"SQL QUERY THAT WAS RUN: {sql_query}\n")
    user_prompt_parts.append(f"DATA RESULTS:\n{json.dumps(results, indent=2, default=str)}\n")
    user_prompt_parts.append("Please provide a final, user-friendly answer that's personalized for this user.")
    
    user_prompt = "\n".join(user_prompt_parts)
    
    try:
        resp = llm.invoke([("system", system_prompt), ("user", user_prompt)])
        return (getattr(resp, "content", None) or "Could not synthesize an answer.").strip()
    except Exception as e:
        frappe.log_error(f"SQL answer synthesis failed: {e}")
        return "There was an error while formatting the answer."


# --- Main Function (ENHANCED) ---

def answer_from_sql(
    query: str,
    user_profile: Optional[Dict[str, Any]] = None,
    content_details: Optional[Dict[str, Any]] = None,
    chat_history: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]:
    """
    Main Text-to-SQL entry point with user context awareness.
    
    Args:
        query: Natural language question
        user_profile: User profile from DynamicConfig.get_user_profile()
            Contains: name, batch, grade, enrollments, etc.
        content_details: Content details for content-specific queries
        chat_history: Conversation history
    
    Returns:
        Dictionary with answer, SQL query, and success flag
    """
    chat_history = chat_history or []
    
    print("> Starting Text-to-SQL process...")
    if user_profile:
        print(f"> User Context: {user_profile.get('name')} | Grade {user_profile.get('grade')} | Batch {user_profile.get('batch')}")
    
    generation_result = _generate_sql_query(query, user_profile, content_details)
    sql_query = generation_result.get("sql")

    if not sql_query:
        return {
            "question": query,
            "answer": "I could not generate a valid SQL query.",
            "sql_query": None,
            "success": False
        }
    
    print(f"\n> Generated SQL Query:\n{sql_query}")
    
    try:
        results = _execute_sql(sql_query)
    except Exception as e:
        return {
            "question": query,
            "answer": f"The query failed to execute. Error: {e}",
            "sql_query": sql_query,
            "success": False
        }

    if not results:
        return {
            "question": query,
            "answer": "The query ran successfully but returned no results.",
            "sql_query": sql_query,
            "results": [],
            "success": True
        }

    print(f"> SQL returned {len(results)} row(s).")
    
    final_answer = _synthesize_answer(query, sql_query, results, user_profile, chat_history)
    
    return {
        "question": query,
        "answer": final_answer,
        "sql_query": sql_query,
        "results": results,
        "success": True,
        "user_context": {
            "batch": user_profile.get('batch') if user_profile else None,
            "grade": user_profile.get('grade') if user_profile else None
        }
    }
