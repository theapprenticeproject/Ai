# tap_ai/services/sql_answerer.py
"""
Text-to-SQL Engine - Enhanced with Optional User Context
Generates SQL queries with optional grade/batch filtering for personalized results
"""

import json
import re
import time
from typing import Any, Dict, List, Optional

import frappe
from langchain_core.prompts import ChatPromptTemplate
from loguru import logger

from tap_ai.infra.config import get_config
from tap_ai.infra.llm_client import llm_invoke_cached
from tap_ai.infra.sql_catalog import load_schema
from tap_ai.models import UserProfile
from tap_ai.utils.prompt_bank import get_system_message_for_context


def _sanitize_prompt_value(value: Any) -> str:
    """Strip newlines and control characters from user-profile values before prompt injection."""
    if not value:
        return ""
    return re.sub(r"[\r\n\t\x00-\x1f]", " ", str(value).strip())[:100]


# --- Schema Building ---

def _build_enriched_schema_prompt(user_profile: Optional[UserProfile] = None) -> str:
    """Builds an enriched schema prompt with optional user context hints."""
    schema = load_schema()
    tables = schema.get("tables", {})
    allowed_joins = schema.get("allowed_joins", [])
    guardrails = schema.get("guardrails", [])
    
    # Prepend persona system prompt to bias SQL assistant behavior
    try:
        persona = get_system_message_for_context(user_profile=user_profile)
        prompt_parts = [f"SYSTEM_PERSONA:\n{persona}\n\n", "DATABASE SCHEMA:\n"]
    except Exception:
        prompt_parts = ["DATABASE SCHEMA:\n"]
    
    col_limit = int(get_config("schema_max_columns") or 20)
    join_limit = int(get_config("schema_max_joins") or 20)

    # Add tables
    for table_name, table_info in tables.items():
        doctype = table_info.get("doctype")
        columns = table_info.get("columns", [])
        description = table_info.get("description", "")

        if len(columns) > col_limit:
            logger.warning(f"Schema truncated: {table_name} has {len(columns)} columns, showing {col_limit}")

        prompt_parts.append(f"\nTable: {table_name} (DocType: {doctype})")
        prompt_parts.append(f"Description: {description}")
        prompt_parts.append(f"Columns: {', '.join(columns[:col_limit])}")

        # Add display field if available
        if table_info.get("display_field"):
            prompt_parts.append(f"Display Field: {table_info['display_field']}")

    # Add allowed joins
    if allowed_joins:
        if len(allowed_joins) > join_limit:
            logger.warning(f"Schema truncated: {len(allowed_joins)} joins, showing {join_limit}")
        prompt_parts.append("\n\nALLOWED JOINS:")
        for join in allowed_joins[:join_limit]:
            prompt_parts.append(
                f"- {join['left_table']}.{join['left_key']} → "
                f"{join['right_table']}.{join['right_key']}"
            )
    
    # Add guardrails
    if guardrails:
        prompt_parts.append("\n\nGUARDRAILS:")
        for rule in guardrails:
            prompt_parts.append(f"- {rule}")

        # Add user context hints if available
    if user_profile:
        if user_profile.type:
            prompt_parts.append(f"- User Type: {_sanitize_prompt_value(user_profile.type)}")

        if user_profile.grade:
            grade = _sanitize_prompt_value(user_profile.grade)
            prompt_parts.append(f"- Grade: {grade}")
            prompt_parts.append(f"  IMPORTANT: Filter results by grade = '{grade}' when querying student content")

        if user_profile.batch:
            batch = _sanitize_prompt_value(user_profile.batch)
            prompt_parts.append(f"- Batch: {batch}")
            prompt_parts.append(f"  Consider filtering by batch when relevant")

        if user_profile.current_enrollment and user_profile.current_enrollment.course:
            course = _sanitize_prompt_value(user_profile.current_enrollment.course)
            prompt_parts.append(f"- Current Course: {course}")
            prompt_parts.append(f"  Prioritize content from this course")
    else:
        prompt_parts.append("\n\nUSER CONTEXT:")
        prompt_parts.append("- Anonymous query (no user-specific filtering)")
        prompt_parts.append("- Return general content without grade/batch filters")
    
    return "\n".join(prompt_parts)


# --- SQL Generation ---

SQL_GENERATION_PROMPT = """You are an expert SQL query generator for an educational platform database.

Your task is to convert natural language questions into valid PostgreSQL SQL queries based on the provided schema.

RULES:
1. Return ONLY valid SQL - no explanations, no markdown, no code fences
2. Use ONLY tables and joins from the schema
3. Always quote table and column names with double quotes (PostgreSQL syntax — NOT backticks)
4. Always use table aliases for clarity
5. Primary key is always 'name'
6. Always include LIMIT clause (default: 20)
7. Use proper WHERE conditions for filtering
8. Apply user context filters (grade, batch) when provided and relevant
9. For SELECT *, limit to essential columns when possible
10. Handle NULL values appropriately
11. Use ILIKE '%term%' for case-insensitive text search

RESPONSE FORMAT:
Return ONLY the SQL query, nothing else.

Example good queries:
- SELECT v.name, v.video_name, v.difficulty_tier FROM "tabVideoClass" v WHERE v.difficulty_tier = 'Basic' LIMIT 10
- SELECT s.name, s.name1, s.grade FROM "tabStudent" s WHERE s.grade = '8' LIMIT 20
- SELECT a.name, a.assignment_name, a.subject FROM "tabAssignment" a WHERE a.difficulty_tier = 'Intermediate' LIMIT 15
"""


_SQL_GEN_PROMPT = ChatPromptTemplate.from_messages([
    ("system", SQL_GENERATION_PROMPT),
    ("system", "{persona}"),
    ("human", "{user_prompt}"),
])

_SQL_SYNTHESIS_PROMPT = ChatPromptTemplate.from_messages([
    ("system", "{system_prompt}"),
    ("human", "{user_prompt}"),
])


def _generate_sql_query(
    query: str,
    schema_prompt: str,
    user_profile: Optional[UserProfile] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
) -> str:
    """Uses LLM to generate SQL from natural language query."""
    try:
        persona = get_system_message_for_context(user_profile=user_profile)
    except Exception:
        persona = ""

    # Build user prompt with context hints  
    user_prompt_parts = [  
        f"QUESTION: {query}",  
        "",  
        schema_prompt  
    ]  
      
    # Add conversation history if available  
    if chat_history:  
        history_text = "\n".join([  
            f"{msg.get('role', 'unknown').title()}: {msg.get('content', '')}"  
            for msg in chat_history[-5:]  # Last 5 messages for context  
        ])  
        user_prompt_parts.append(f"\nCONVERSATION HISTORY:\n{history_text}")  
        user_prompt_parts.append("\nConsider the conversation context when generating the SQL query.")  
      
    # Add specific instructions for user context
    if user_profile:
        if user_profile.grade:
            grade = _sanitize_prompt_value(user_profile.grade)
            user_prompt_parts.append(
                f"\nIMPORTANT: User is in Grade {grade}. "
                f"Filter by grade = '{grade}' when querying student content like videos, quizzes, assignments."
            )

        if user_profile.batch:
            batch = _sanitize_prompt_value(user_profile.batch)
            user_prompt_parts.append(
                f"User's batch is {batch}. Consider filtering by batch when relevant."
            )
    else:  
        user_prompt_parts.append(  
            "\nNote: This is an anonymous query. Return general content without user-specific filters."  
        )  
      
    user_prompt = "\n".join(user_prompt_parts)  
      
    try:
        messages = _SQL_GEN_PROMPT.format_messages(persona=persona or "", user_prompt=user_prompt)
        # Drop the persona system message when it's empty
        messages = [m for m in messages if m.content.strip()]

        sql = llm_invoke_cached(
            [(m.type, m.content) for m in messages],
            model=get_config("primary_llm_model") or "gpt-4o-mini",
            temperature=0.0,
            max_tokens=800,
        ).strip()
          
        # Clean up the SQL
        sql = sql.replace("```sql", "").replace("```", "").strip()

        # Safety net: replace any backtick-quoted identifiers with double quotes.
        # The LLM sometimes reverts to MySQL/MariaDB syntax despite the prompt.
        sql = re.sub(r"`([^`]+)`", r'"\1"', sql)

        # Add LIMIT if missing
        if "LIMIT" not in sql.upper():
            sql += " LIMIT 20"  
          
        logger.debug(f"Generated SQL: {sql[:200]}...")
        return sql  
          
    except Exception as e:  
        frappe.log_error(f"SQL generation failed: {e}")  
        raise Exception(f"Failed to generate SQL query: {str(e)}")  

# --- SQL Execution ---

def _execute_sql(sql: str) -> List[Dict[str, Any]]:
    """
    Executes SQL query safely and returns results.

    Args:
        sql: SQL query string

    Returns:
        List of result dictionaries
    """
    try:
        # Use remote database instead of local frappe.db
        from tap_ai.utils.remote_db import execute_remote_query
        results = execute_remote_query(sql)
        logger.debug(f"SQL returned {len(results)} rows")
        return results

    except Exception as e:
        error_msg = str(e)
        frappe.log_error(f"SQL execution failed: {error_msg}\nSQL: {sql}")

        # Return user-friendly error
        if "doesn't exist" in error_msg:
            raise Exception("The query referenced tables that don't exist in the database.")
        elif "syntax" in error_msg.lower():
            raise Exception("The generated SQL query had a syntax error.")
        else:
            raise Exception(f"Database error: {error_msg}")


# --- Answer Synthesis ---

def _synthesize_answer_from_results(
    query: str,
    sql: str,
    results: List[Dict[str, Any]],
    user_profile: Optional[UserProfile] = None,
) -> str:
    """Uses LLM to synthesize a natural language answer from SQL results."""
    if user_profile and user_profile.name:
        system_prompt = f"You are a helpful educational assistant.\n\nThe user is {user_profile.name}.\n"
        if user_profile.type:
            system_prompt += f"User type: {user_profile.type.title()}. "
        if user_profile.grade:
            system_prompt += f"They are in Grade {user_profile.grade}. "
        system_prompt += "\nConvert the SQL query results into a clear, friendly answer.\nAddress the user by name when appropriate.\n"
    else:
        system_prompt = "You are a helpful educational assistant.\n\nConvert the SQL query results into a clear, friendly answer.\n"
    
    system_prompt += """
RULES:
1. Answer the user's question directly
2. Present data in a clear, organized format
3. Be concise but complete
4. Use bullet points or numbering for lists
5. Include relevant details from the results
6. If no results, say so politely
7. Be encouraging and helpful
"""
    
    # Format results for LLM
    if not results:
        results_text = "No results found."
    else:
        # Limit to first 20 results for LLM context
        limited_results = results[:20]
        results_text = json.dumps(limited_results, indent=2, default=str)
    
    user_prompt = f"""QUESTION: {query}

SQL QUERY: {sql}

RESULTS ({len(results)} total):
{results_text}

Provide a helpful answer based on these results."""
    
    try:
        messages = _SQL_SYNTHESIS_PROMPT.format_messages(
            system_prompt=system_prompt, user_prompt=user_prompt
        )
        answer = llm_invoke_cached(
            [(m.type, m.content) for m in messages],
            model=get_config("primary_llm_model") or "gpt-4o-mini",
            temperature=0.2,
            max_tokens=800,
        ).strip()
        if not answer:
            answer = "I couldn't generate an answer."
        
        if user_profile and user_profile.name and not answer.startswith("Hi"):
            answer = f"Hi {user_profile.name}! {answer}"
        
        return answer
        
    except Exception as e:
        frappe.log_error(f"Answer synthesis failed: {e}")
        return "I found some results but couldn't format them properly. Please try rephrasing your question."


# --- Main Function ---

def answer_from_sql(
    query: str,
    user_profile: Optional[UserProfile] = None,
    content_details: Optional[Any] = None,
    chat_history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """Main Text-to-SQL entry point with optional user context."""
    start_time = time.time()

    logger.info("Starting Text-to-SQL process")
    if user_profile:
        logger.debug(f"User context: {user_profile.name} | grade={user_profile.grade} | batch={user_profile.batch}")

    try:
        schema_prompt = _build_enriched_schema_prompt(user_profile)
        sql_query = _generate_sql_query(query, schema_prompt, user_profile, chat_history)
        results = _execute_sql(sql_query)
        answer = _synthesize_answer_from_results(
            query=query,
            sql=sql_query,
            results=results,
            user_profile=user_profile
        )

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"Text-to-SQL completed in {elapsed}s")
        
        return {
            "question": query,
            "answer": answer,
            "sql_query": sql_query,
            "results_count": len(results),
            "results": results[:10],  # Return first 10 for API response
            "execution_time": elapsed,
            "user_context": "personalized" if user_profile else "general"
        }
        
    except Exception as e:
        error_msg = str(e)
        frappe.log_error(f"Text-to-SQL failed: {error_msg}\nQuery: {query}")
        
        elapsed = round(time.time() - start_time, 2)
        
        return {
            "question": query,
            "answer": f"I encountered an error while processing your query: {error_msg}",
            "sql_query": None,
            "results_count": 0,
            "error": error_msg,
            "execution_time": elapsed
        }