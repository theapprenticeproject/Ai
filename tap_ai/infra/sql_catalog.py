# tap_ai/infra/sql_catalog.py  
  
import json 
import frappe 
import os  
import time  
from functools import lru_cache  
  
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schema", "tap_ai_schema.json")  
  
# Global cache with timestamp  
_schema_cache = {  
    'data': None,  
    'timestamp': 0,  
    'ttl': 300  # 5 minutes  
}  
  
def load_schema(force_refresh: bool = False):  
    """  
    Load schema with in-memory caching.  
      
    Args:  
        force_refresh: If True, bypass cache and reload from disk  
    """  
    current_time = time.time()  
      
    # Check if cache is valid  
    if (not force_refresh and   
        _schema_cache['data'] is not None and   
        current_time - _schema_cache['timestamp'] < _schema_cache['ttl']):  
        return _schema_cache['data']  
      
    # Load from disk  
    try:  
        with open(SCHEMA_PATH, "r") as f:  
            _schema_cache['data'] = json.load(f)  
            _schema_cache['timestamp'] = current_time  
            return _schema_cache['data']  
    except FileNotFoundError:  
        frappe.log_error(f"Schema file not found: {SCHEMA_PATH}")  
        return {}  
    except json.JSONDecodeError as e:  
        frappe.log_error(f"Invalid JSON in schema file: {e}")  
        return {}  
  
def clear_schema_cache():  
    """Clear the schema cache"""  
    _schema_cache['data'] = None  
    _schema_cache['timestamp'] = 0  
  
@lru_cache(maxsize=1)  
def get_schema_version():  
    """Get schema file modification time for cache invalidation"""  
    try:  
        return os.path.getmtime(SCHEMA_PATH)  
    except OSError:  
        return 0