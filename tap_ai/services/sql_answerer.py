# tap_ai/services/sql_answerer.py
# Implements the Text-to-SQL approach with a rich, dynamic schema and chat history.

import json
import logging
from typing import Dict, Any, List, Optional

import frappe
from langchain_openai import ChatOpenAI

from tap_ai.infra.config import get_config
from tap_ai.infra.sql_catalog import load_schema

logger = logging.getLogger(__name__)

# --- LLM and Schema Helpers ---

def _llm(model: str = "gpt-4o-mini") -> Optional[ChatOpenAI]:
    """Initializes the Language Model client."""
    api_key = get_config("openai_api_key")
    if not api_key:
        logger.error("OpenAI API key missing.")
        return None
    return ChatOpenAI(model_name=model, openai_api_key=api_key, temperature=0.0, max_tokens=1024)

def _schema_summary_for_sql() -> str:
    """
    Creates a rich, text-based summary of the DB schema, including filterable
    fields, their specific options, and explicit join information to guide the LLM.
    """
    schema = load_schema()
    summary_parts = []
    
    FILTERABLE_TYPES = {"Select", "Link"}

    summary_parts.append("TABLES (with filterable fields and options):")
    for tname, tinfo in schema.get("tables", {}).items():
        doctype = tinfo.get("doctype") or tname.replace("tab", "", 1)
        
        try:
            meta = frappe.get_meta(doctype)
            field_details = []
            for field in meta.fields:
                # Include filterable fields like Select and Link
                if field.fieldtype in FILTERABLE_TYPES:
                    if field.fieldtype == "Select" and field.options:
                        options = [opt.strip() for opt in field.options.split('\n') if opt.strip()]
                        field_details.append(f"{field.fieldname} (Select, Options: {options})")
                    elif field.fieldtype == "Link" and field.options:
                        field_details.append(f"{field.fieldname} (Link to {field.options})")
                # Also include key data fields for context
                elif field.fieldtype in {"Data", "Small Text", "Text", "Currency", "Int", "Float"}:
                    field_details.append(f"{field.fieldname} ({field.fieldtype})")
            
            if field_details:
                summary_parts.append(f"- {tname}:")
                for detail in field_details:
                    summary_parts.append(f"  - {detail}")

        except frappe.DoesNotExistError:
            # Fallback for schemas without detailed meta
            cols = ", ".join(tinfo.get("columns", []))
            summary_parts.append(f"- {tname}: Columns are [{cols}]")

    # ---Add the explicit join information ---
    summary_parts.append("\nJOINS (how tables connect):")
    for join in schema.get("allowed_joins", []):
        why = join.get('why', f"{join['left_table']}.{join['left_key']} -> {join['right_table']}.{join['right_key']}")
        summary_parts.append(f"- {why}")

    return "\n".join(summary_parts)


# --- Core Text-to-SQL Logic ---

SQL_GEN_PROMPT = """You are an expert SQL query generator. Your task is to convert a natural language question into a precise and safe SQL query for a MariaDB database.

Given:
- A user's question.
- A schema summary describing tables, their filterable fields, the exact options for those fields, and how they join.

Return ONLY a JSON object with this structure:
{
  "sql": "SELECT ... FROM ... WHERE ... LIMIT 20;",
  "reason": "A short explanation of the generated query (<= 30 words)."
}

Rules:
- The SQL query MUST be valid for MariaDB.
- The query MUST be a `SELECT` statement. Do NOT generate `UPDATE`, `DELETE`, or `INSERT` queries.
- ALWAYS include a `LIMIT` clause (e.g., `LIMIT 20`).
- Use ONLY the tables, columns, and joins provided in the schema.
- When filtering, the value in the `WHERE` clause MUST exactly match one of the `Options` provided in the schema (e.g., `WHERE difficulty_tier = 'Basic'`). Do not guess or approximate values.
- If the question cannot be answered, return `{"sql": null, "reason": "The question cannot be answered with the available data."}`
"""

def _generate_sql_query(query: str) -> Dict[str, Any]:
    """Uses an LLM to generate a SQL query from a natural language question."""
    llm = _llm("gpt-4o")
    if not llm: return {"sql": None, "reason": "LLM not available."}

    schema_summary = _schema_summary_for_sql()
    user_prompt = (f"QUESTION:\n{query}\n\nDATABASE SCHEMA:\n{schema_summary}\n\nGenerate the SQL query.")
    try:
        resp = llm.invoke([("system", SQL_GEN_PROMPT), ("user", user_prompt)])
        content = getattr(resp, "content", "")
        if content.startswith("```json"):
            content = content[7:-3].strip()
        
        data = json.loads(content)
        sql = data.get("sql")
        
        # Basic validation
        if sql and "SELECT" in sql.upper() and "LIMIT" in sql.upper():
            print(f"LLM Reason for SQL: {data.get('reason')}")
            return data
        else:
            print(f"LLM Reason (Query Failed Validation): {data.get('reason')}")
            return {"sql": None, "reason": data.get('reason')}
    except Exception as e:
        logger.error(f"SQL generation LLM failed: {e}")
        return {"sql": None, "reason": f"LLM error: {e}"}

def _execute_sql(sql_query: str) -> List[Dict[str, Any]]:
    """Executes a given SQL query and returns the results."""
    try:
        return frappe.db.sql(sql_query, as_dict=True)
    except Exception as e:
        frappe.log_error(f"SQL execution failed for query: {sql_query}", f"Error: {e}")
        # Return an empty list on failure to prevent crashes
        return []

def _synthesize_answer(
    query: str,
    sql_query: str,
    results: List[Dict[str, Any]],
    chat_history: List[Dict[str, str]]
) -> str:
    """Asks the LLM to turn the SQL results into a natural language answer, using history for context."""
    llm = _llm()

    system_prompt = (
        "You are a helpful assistant. The user asked a question, a SQL query was run, and here are the results. "
        "Based on the conversation history and the data, formulate a friendly, natural language answer to the user's final question."
    )
    
    history_str = "\n".join([f"{turn['role'].title()}: {turn['content']}" for turn in chat_history])
    
    user_prompt_with_context = (
        f"CONVERSATION HISTORY:\n---\n{history_str}\n---\n\n"
        f"FINAL QUESTION: {query}\n\n"
        f"SQL QUERY THAT WAS RUN: {sql_query}\n\n"
        f"DATA RESULTS:\n{json.dumps(results, indent=2, default=str)}\n\n"
        "Please provide a final, user-friendly answer."
    )

    try:
        resp = llm.invoke([("system", system_prompt), ("user", user_prompt_with_context)])
        return (getattr(resp, "content", None) or "Could not synthesize an answer.").strip()
    except Exception as e:
        frappe.log_error(f"SQL answer synthesis failed: {e}")
        return "There was an error while formatting the answer."

# --- Main Function ---
def answer_from_sql(
    query: str,
    chat_history: Optional[List[Dict[str, str]]] = None
) -> Dict[str, Any]:
    """
    Main Text-to-SQL entry point, now aware of conversation history.
    """
    chat_history = chat_history or []
    print("> Starting Text-to-SQL process...")
    
    generation_result = _generate_sql_query(query)
    sql_query = generation_result.get("sql")

    if not sql_query:
        # Explicitly signal failure if no valid SQL was generated.
        return {"question": query, "answer": "I could not generate a valid SQL query.", "sql_query": None, "success": False}
    print(f"\n> Generated SQL Query:\n{sql_query}")
    
    try:
        results = _execute_sql(sql_query)
    except Exception as e:
        # On a database error, return a specific failure flag for the router.
        return {"question": query, "answer": f"The query failed to execute. Error: {e}", "sql_query": sql_query, "success": False}

    if not results:
        # This is a "soft failure" - the query ran but found nothing.
        return {"question": query, "answer": "The query ran successfully but returned no results.", "sql_query": sql_query, "success": True}
    final_answer = _synthesize_answer(query, sql_query, results, chat_history)

    return {
        "question": query,
        "answer": final_answer,
        "sql_query": sql_query,
        "raw_results": results,
        "success": True
    }

# --- Bench CLI Helper ---
def cli(query: str):
    """
    Bench command to test the Text-to-SQL pipeline.
    Example:
    bench execute tap_ai.services.sql_answerer.cli --kwargs "{'query': 'list the names of all course videos and their links having basic difficulty'}"
    """
    result = answer_from_sql(query)
    
    print("\n--- FINAL ANSWER ---")
    final_json = json.dumps(result, indent=2, default=str)
    print(final_json)
    
    return result

