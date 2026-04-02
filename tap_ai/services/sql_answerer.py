# tap_ai/services/sql_answerer.py
"""
Text-to-SQL Engine - Enhanced with Optional User Context
Generates SQL queries with optional grade/batch filtering for personalized results
"""

import json
import time
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.infra.sql_catalog import load_schema


# --- LLM Initialization ---

def _llm(model: str = "gpt-4o-mini", temperature: float = 0.0) -> ChatOpenAI:  
    from tap_ai.infra.llm_client import LLMClient  
    return LLMClient.get_client(  
        model=model,  
        temperature=temperature,  
        max_tokens=800  
    )


# --- Schema Building ---

def _build_enriched_schema_prompt(user_profile: Optional[Dict] = None) -> str:
    """
    Builds an enriched schema prompt with optional user context hints.
    
    Args:
        user_profile: Optional user profile (can be None)
    
    Returns:
        String containing schema description for LLM
    """
    schema = load_schema()
    tables = schema.get("tables", {})
    allowed_joins = schema.get("allowed_joins", [])
    guardrails = schema.get("guardrails", [])
    
    prompt_parts = ["DATABASE SCHEMA:\n"]
    
    # Add tables
    for table_name, table_info in tables.items():
        doctype = table_info.get("doctype")
        columns = table_info.get("columns", [])
        description = table_info.get("description", "")
        
        prompt_parts.append(f"\nTable: {table_name} (DocType: {doctype})")
        prompt_parts.append(f"Description: {description}")
        prompt_parts.append(f"Columns: {', '.join(columns[:20])}")  # Limit for brevity
        
        # Add display field if available
        if table_info.get("display_field"):
            prompt_parts.append(f"Display Field: {table_info['display_field']}")
    
    # Add allowed joins
    if allowed_joins:
        prompt_parts.append("\n\nALLOWED JOINS:")
        for join in allowed_joins[:20]:  # Limit for brevity
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
        prompt_parts.append("\n\nUSER CONTEXT:")
        
        if user_profile.get('type'):
            prompt_parts.append(f"- User Type: {user_profile['type']}")
        
        if user_profile.get('grade'):
            prompt_parts.append(f"- Grade: {user_profile['grade']}")
            prompt_parts.append(f"  IMPORTANT: Filter results by grade = '{user_profile['grade']}' when querying student content")
        
        if user_profile.get('batch'):
            prompt_parts.append(f"- Batch: {user_profile['batch']}")
            prompt_parts.append(f"  Consider filtering by batch when relevant")
        
        if user_profile.get('current_enrollment'):
            enrollment = user_profile['current_enrollment']
            if enrollment.get('course'):
                prompt_parts.append(f"- Current Course: {enrollment['course']}")
                prompt_parts.append(f"  Prioritize content from this course")
    else:
        prompt_parts.append("\n\nUSER CONTEXT:")
        prompt_parts.append("- Anonymous query (no user-specific filtering)")
        prompt_parts.append("- Return general content without grade/batch filters")
    
    return "\n".join(prompt_parts)


# --- SQL Generation ---

SQL_GENERATION_PROMPT = """You are an expert SQL query generator for an educational platform database.

Your task is to convert natural language questions into valid MariaDB SQL queries based on the provided schema.

RULES:
1. Return ONLY valid SQL - no explanations, no markdown, no backticks
2. Use ONLY tables and joins from the schema
3. Always use table aliases for clarity
4. Primary key is always 'name'
5. Always include LIMIT clause (default: 20)
6. Use proper WHERE conditions for filtering
7. Apply user context filters (grade, batch) when provided and relevant
8. For SELECT *, limit to essential columns when possible
9. Handle NULL values appropriately
10. Use LIKE '%term%' for text search

RESPONSE FORMAT:
Return ONLY the SQL query, nothing else.

Example good queries:
- SELECT v.name, v.video_name, v.difficulty_tier FROM `tabVideoClass` v WHERE v.difficulty_tier = 'Basic' LIMIT 10
- SELECT s.name, s.name1, s.grade FROM `tabStudent` s WHERE s.grade = '8' LIMIT 20
- SELECT a.name, a.assignment_name, a.subject FROM `tabAssignment` a WHERE a.difficulty_tier = 'Intermediate' LIMIT 15
"""


def _generate_sql_query(  
    query: str,  
    schema_prompt: str,  
    user_profile: Optional[Dict] = None,  
    chat_history: Optional[List[Dict[str, str]]] = None  # Add chat_history parameter  
) -> str:  
    """  
    Uses LLM to generate SQL from natural language query.  
      
    Args:  
        query: User's natural language question  
        schema_prompt: Enriched schema description  
        user_profile: Optional user profile for context  
        chat_history: Optional conversation history for context  
      
    Returns:  
        Generated SQL query string  
    """  
    llm = _llm()  
      
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
        if user_profile.get('grade'):  
            user_prompt_parts.append(  
                f"\nIMPORTANT: User is in Grade {user_profile['grade']}. "  
                f"Filter by grade = '{user_profile['grade']}' when querying student content like videos, quizzes, assignments."  
            )  
          
        if user_profile.get('batch'):  
            user_prompt_parts.append(  
                f"User's batch is {user_profile['batch']}. Consider filtering by batch when relevant."  
            )  
    else:  
        user_prompt_parts.append(  
            "\nNote: This is an anonymous query. Return general content without user-specific filters."  
        )  
      
    user_prompt = "\n".join(user_prompt_parts)  
      
    try:  
        resp = llm.invoke([  
            ("system", SQL_GENERATION_PROMPT),  
            ("user", user_prompt)  
        ])  
        sql = getattr(resp, "content", "").strip()  
          
        # Clean up the SQL  
        sql = sql.replace("```sql", "").replace("```", "").strip()  
          
        # Add LIMIT if missing  
        if "LIMIT" not in sql.upper():  
            sql += " LIMIT 20"  
          
        print(f"> Generated SQL: {sql[:200]}...")  
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
        # Execute as dict for easier processing
        results = frappe.db.sql(sql, as_dict=True)
        print(f"> SQL returned {len(results)} rows")
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
    user_profile: Optional[Dict] = None
) -> str:
    """
    Uses LLM to synthesize a natural language answer from SQL results.
    
    Args:
        query: Original user question
        sql: SQL query that was executed
        results: Query results
        user_profile: Optional user profile for personalization
    
    Returns:
        Natural language answer
    """
    llm = _llm(temperature=0.2)
    
    # Build system prompt with optional personalization
    if user_profile and user_profile.get('name'):
        system_prompt = f"""You are a helpful educational assistant.

The user is {user_profile.get('name', 'a learner')}.
"""
        if user_profile.get('type'):
            system_prompt += f"User type: {user_profile['type'].title()}. "
        
        if user_profile.get('grade'):
            system_prompt += f"They are in Grade {user_profile['grade']}. "
        
        system_prompt += """
Convert the SQL query results into a clear, friendly answer.
Address the user by name when appropriate.
"""
    else:
        system_prompt = """You are a helpful educational assistant.

Convert the SQL query results into a clear, friendly answer.
"""
    
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
        resp = llm.invoke([
            ("system", system_prompt),
            ("user", user_prompt)
        ])
        answer = getattr(resp, "content", "I couldn't generate an answer.").strip()
        
        # Add personalized greeting if user profile available
        if user_profile and user_profile.get('name') and not answer.startswith("Hi"):
            answer = f"Hi {user_profile['name']}! {answer}"
        
        return answer
        
    except Exception as e:
        frappe.log_error(f"Answer synthesis failed: {e}")
        return "I found some results but couldn't format them properly. Please try rephrasing your question."


# --- Main Function ---

def answer_from_sql(  
    query: str,  
    user_profile: Optional[Dict[str, Any]] = None,  
    content_details: Optional[Dict[str, Any]] = None,  
    chat_history: Optional[List[Dict[str, str]]] = None  
) -> Dict[str, Any]: 
    """
    Main Text-to-SQL entry point with optional user context.
    
    Args:
        query: Natural language question
        user_profile: Optional user profile from DynamicConfig.get_user_profile()
            Contains: name, batch, grade, enrollments, current_enrollment
            Can be None for anonymous queries
        content_details: Optional content details (not typically used for SQL)
        chat_history: Optional conversation history (not typically used for SQL)
    
    Returns:
        Dictionary with answer, SQL query, results, and metadata
    """
    start_time = time.time()
    
    print("> Starting Text-to-SQL process...")
    if user_profile:
        user_name = user_profile.get('name', 'Unknown')
        user_grade = user_profile.get('grade', 'N/A')
        user_batch = user_profile.get('batch', 'N/A')
        print(f"> User Context: {user_name} | Grade {user_grade} | Batch {user_batch}")
    else:
        print("> No user context - generating general SQL query")
    
    try:
        # Step 1: Build enriched schema prompt
        print("> Building schema prompt...")
        schema_prompt = _build_enriched_schema_prompt(user_profile)
        
        # Step 2: Generate SQL query - now pass chat_history  
        print("> Generating SQL query...")  
        sql_query = _generate_sql_query(query, schema_prompt, user_profile, chat_history)  
        
        # Step 3: Execute SQL
        print("> Executing SQL query...")
        results = _execute_sql(sql_query)
        
        # Step 4: Synthesize natural language answer
        print("> Synthesizing answer...")
        answer = _synthesize_answer_from_results(
            query=query,
            sql=sql_query,
            results=results,
            user_profile=user_profile
        )
        
        elapsed = round(time.time() - start_time, 2)
        print(f"> Text-to-SQL process completed in {elapsed}s")
        
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