# tap_ai/utils/dynamic_config.py
"""
Dynamic Configuration Helper - Updated for TAP LMS Child Tables
Makes TAP AI resilient to TAP LMS schema changes
Properly handles Student enrollment child table
"""

import frappe
import json
import time
from typing import Dict, Any, Optional, List, Tuple
import json
from tap_ai.services.router import process_query  # Import the actual AI processor function to use in the API endpoint

class DynamicConfig:
    """
    Central configuration handler that decouples TAP AI from TAP LMS with TTL-based cache invalidation
    NOW PROPERLY HANDLES CHILD TABLES (like Student.enrollment)
    """
    
    _instance = None  
    _config_cache = {  
        'data': None,  
        'timestamp': 0,  
        'ttl': 300  # 5 minutes default TTL  
    }  
    
    def __new__(cls):  
        if cls._instance is None:  
            cls._instance = super().__new__(cls)  
        return cls._instance  
    
    @classmethod  
    def get_config(cls, force_refresh: bool = False) -> Dict[str, Any]:  
        """  
        Get cached configuration with TTL-based invalidation.  
          
        Args:  
            force_refresh: If True, bypass cache and reload from database  
        """  
        current_time = time.time()  
          
        # Check if cache is valid  
        if (not force_refresh and   
            cls._config_cache['data'] is not None and   
            current_time - cls._config_cache['timestamp'] < cls._config_cache['ttl']):  
            return cls._config_cache['data']  
          
        # Load from database  
        try:  
            config_doc = frappe.get_single("AI Integration Config")  
            cls._config_cache['data'] = {  
                'user_type_config': json.loads(config_doc.user_type_config or '{}'),  
                'doctype_mappings': json.loads(config_doc.doctype_mappings or '{}'),  
                'context_resolution_rules': json.loads(config_doc.context_resolution_rules or '{}'),  
                'response_templates': json.loads(config_doc.response_templates or '{}'),  
                'fallback_behavior': json.loads(config_doc.fallback_behavior or '{}'),  
                'enabled': config_doc.enabled,  
                'cache_ttl': config_doc.cache_ttl or 300,  
                'enable_logging': config_doc.enable_logging  
            }  
            cls._config_cache['timestamp'] = current_time  
            cls._config_cache['ttl'] = cls._config_cache['data']['cache_ttl']  
              
        except Exception as e:  
            frappe.log_error(f"Failed to load DynamicConfig: {e}")  
            # Return cached data even if expired, or empty dict if no cache  
            return cls._config_cache['data'] or {}  
          
        return cls._config_cache['data'] 
    
    @classmethod  
    def clear_cache(cls):  
        """Clear cached config"""  
        cls._config_cache['data'] = None  
        cls._config_cache['timestamp'] = 0  
      
    @classmethod  
    def set_cache_ttl(cls, ttl: int):  
        """Set custom cache TTL in seconds"""  
        cls._config_cache['ttl'] = ttl  
    
    # ========== User Type Configuration Methods ==========
    
    @classmethod
    def get_user_type_config(cls, user_type: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific user type"""
        config = cls.get_config()
        return config['user_type_config'].get(user_type)
    
    @classmethod
    def get_profile_doctype(cls, user_type: str) -> Optional[str]:
        """Get the DocType name for a user type's profile"""
        user_config = cls.get_user_type_config(user_type)
        return user_config.get('profile_doctype') if user_config else None
    
    @classmethod
    def get_user_profile(cls, user_type: str, identifier_value: str, batch_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Fetch user profile dynamically based on user_type
        NOW PROPERLY HANDLES ENROLLMENT CHILD TABLE!
        
        Args:
            user_type: 'student' or 'teacher'
            identifier_value: Value to search (e.g., glific_id)
            batch_id: Optional specific batch ID (for students with multiple enrollments)
            
        Returns:
            User profile data as dict with enrollment info
            
        Example:
            >>> # Get student with current enrollment
            >>> profile = DynamicConfig.get_user_profile('student', '12345')
            >>> print(profile['name'])
            >>> print(profile['current_enrollment']['batch'])  # From child table!
            >>> print(profile['enrollments'])  # All enrollments
            
            >>> # Get specific enrollment
            >>> profile = DynamicConfig.get_user_profile('student', '12345', 'BT-2025-G8-A')
            >>> print(profile['current_enrollment']['batch'])  # Specific batch
        """
        user_config = cls.get_user_type_config(user_type)
        if not user_config:
            return None
        
        profile_doctype = user_config.get('profile_doctype')
        identifier_field = user_config.get('identifier_field', 'glific_id')
        
        if not profile_doctype:
            return None
        
        try:
            # Query the user profile
            user = frappe.get_doc(
                profile_doctype,
                {identifier_field: identifier_value}
            )
            
            if not user:
                return None
            
            # Build base profile
            profile = {
                'id': user.name,
                'name': getattr(user, user_config.get('name_field', 'name'), None),
                'phone': getattr(user, user_config.get('phone_field', 'phone'), None),
                'grade': getattr(user, user_config.get('grade_field', 'grade'), None),
                'glific_id': getattr(user, identifier_field, None),
                '_raw': user  # Keep reference to full doc
            }
            
            # Handle enrollment child table for students
            enrollment_config = user_config.get('enrollment_config')
            if enrollment_config:
                enrollments = cls._get_enrollments(user, enrollment_config, batch_id)
                profile['enrollments'] = enrollments
                profile['current_enrollment'] = enrollments[0] if enrollments else None
                # For backward compatibility, add batch at top level
                profile['batch'] = enrollments[0]['batch'] if enrollments else None
            else:
                # Simple batch field (for teachers, etc.)
                batch_field = user_config.get('batch_field')
                if batch_field:
                    profile['batch'] = getattr(user, batch_field, None)
                
            return profile
                
        except Exception as e:
            frappe.log_error(f"Error fetching {user_type} profile: {str(e)}")
            return None
    
    @classmethod
    def _get_enrollments(cls, student_doc, enrollment_config: Dict, specific_batch_id: Optional[str] = None) -> List[Dict]:
        """
        Extract enrollments from student's enrollment child table
        
        The Student DocType in TAP LMS has an 'enrollment' child table with:
        - batch (Link to Batch)
        - course (Link to Course Level)
        - grade (Select)
        - school (Link to School)
        - date_joining (Date)
        
        Args:
            student_doc: Student document
            enrollment_config: Configuration for enrollment child table
            specific_batch_id: If provided, return only this enrollment
            
        Returns:
            List of enrollment dicts, sorted by date_joining (most recent first)
        """
        child_table_name = enrollment_config.get('child_table', 'enrollment')
        
        if not hasattr(student_doc, child_table_name):
            return []
        
        child_table = getattr(student_doc, child_table_name)
        if not child_table:
            return []
        
        # Field mappings from config
        batch_field = enrollment_config.get('batch_field', 'batch')
        course_field = enrollment_config.get('course_field', 'course')
        grade_field = enrollment_config.get('grade_field', 'grade')
        school_field = enrollment_config.get('school_field', 'school')
        date_field = enrollment_config.get('date_joining_field', 'date_joining')
        
        enrollments = []
        for enrollment_row in child_table:
            batch_value = getattr(enrollment_row, batch_field, None)
            
            # If specific batch requested, filter to only that one
            if specific_batch_id and batch_value != specific_batch_id:
                continue
            
            enrollment_data = {
                'batch': batch_value,
                'course': getattr(enrollment_row, course_field, None),
                'grade': getattr(enrollment_row, grade_field, None),
                'school': getattr(enrollment_row, school_field, None),
                'date_joining': getattr(enrollment_row, date_field, None),
                '_raw': enrollment_row
            }
            enrollments.append(enrollment_data)
        
        # Sort by date_joining, most recent first (current enrollment first)
        enrollments.sort(
            key=lambda x: x.get('date_joining') or '1900-01-01',
            reverse=True
        )
        
        return enrollments
    
    # ========== DocType Mapping Methods ==========
    
    @classmethod
    def get_doctype_mapping(cls, logical_entity: str) -> Optional[Dict[str, Any]]:
        """Get DocType mapping for a logical entity"""
        config = cls.get_config()
        return config['doctype_mappings'].get(logical_entity)
    
    @classmethod
    def get_actual_field_name(cls, logical_entity: str, logical_field: str) -> Optional[str]:
        """Convert logical field name to actual DocType field name"""
        mapping = cls.get_doctype_mapping(logical_entity)
        if not mapping:
            return None
        return mapping.get('fields', {}).get(logical_field)
    
    @classmethod
    def get_actual_doctype_name(cls, logical_entity: str) -> Optional[str]:
        """Get actual DocType name from logical entity"""
        mapping = cls.get_doctype_mapping(logical_entity)
        return mapping.get('doctype') if mapping else None
    
    # ========== Validation Methods ==========
    
    @classmethod
    def validate_request(cls, user_type: str, request_data: Dict) -> Tuple[bool, Optional[str]]:
        """Validate incoming request against configured schema"""
        config = cls.get_config()
        user_config = config['user_type_config'].get(user_type)
        
        if not user_config:
            return False, f"Unknown user_type: {user_type}"
        
        # Check required fields
        required = user_config.get('required_fields', [])
        missing = [f for f in required if f not in request_data]
        if missing:
            return False, f"Missing required fields: {', '.join(missing)}"
        
        # Validate context schema if present
        context = request_data.get('context', {})
        context_schema = user_config.get('context_schema', {})
        
        for field, rules in context_schema.items():
            if isinstance(rules, dict):
                # Check if required
                if rules.get('required') and field not in context:
                    return False, f"Missing required context field: {field}"
                
                # Check allowed values
                value = context.get(field)
                allowed = rules.get('allowed_values', [])
                if value and allowed and value not in allowed:
                    return False, f"Invalid value for {field}: {value}. Allowed: {allowed}"
        
        return True, None
    
    @classmethod
    def validate_content_type(cls, user_type: str, content_type: str) -> bool:
        """Check if content_type is valid for user_type"""
        user_config = cls.get_user_type_config(user_type)
        if not user_config:
            return False
        
        context_schema = user_config.get('context_schema', {})
        content_type_config = context_schema.get('content_type', {})
        
        if isinstance(content_type_config, dict):
            allowed = content_type_config.get('allowed_values', [])
            return content_type in allowed
        
        return False
    
    # ========== Query Helper Methods ==========
    
    @classmethod
    def build_query_filters(cls, logical_entity: str, filters: Dict[str, Any]) -> Dict[str, Any]:
        """Convert logical filters to actual DocType filters"""
        mapping = cls.get_doctype_mapping(logical_entity)
        if not mapping:
            return filters
        
        actual_filters = {}
        field_map = mapping.get('fields', {})
        
        for logical_field, value in filters.items():
            actual_field = field_map.get(logical_field, logical_field)
            actual_filters[actual_field] = value
        
        return actual_filters
    
    @classmethod
    def get_search_fields(cls, logical_entity: str) -> List[str]:
        """Get fields to search for a logical entity"""
        mapping = cls.get_doctype_mapping(logical_entity)
        if not mapping:
            return ['name']
        
        return mapping.get('search_fields', [mapping.get('fields', {}).get('title', 'name')])


# ========== Convenience Helper Functions ==========

def get_video_details(video_id: str) -> Optional[Dict[str, Any]]:
    """Get video details dynamically"""
    config = DynamicConfig.get_doctype_mapping('video')
    if not config:
        return None
    
    doctype = config['doctype']
    field_map = config['fields']
    
    try:
        video = frappe.get_doc(doctype, video_id)
        
        return {
            'id': getattr(video, field_map.get('id', 'name')),
            'title': getattr(video, field_map.get('title', 'title')),
            'duration': getattr(video, field_map.get('duration', 'duration'), None),
            'difficulty': getattr(video, field_map.get('difficulty', 'difficulty'), None),
            'description': getattr(video, field_map.get('description', 'description'), None)
        }
    except Exception as e:
        frappe.log_error(f"Error fetching video {video_id}: {str(e)}")
        return None


def get_content_details(content_type: str, content_id: str) -> Optional[Dict[str, Any]]:
    """Generic function to get any content type details"""
    if content_type == 'video':
        return get_video_details(content_id)
    
    mapping = DynamicConfig.get_doctype_mapping(content_type)
    if not mapping:
        return None
    
    try:
        doc = frappe.get_doc(mapping['doctype'], content_id)
        field_map = mapping['fields']
        
        result = {}
        for logical_field, actual_field in field_map.items():
            result[logical_field] = getattr(doc, actual_field, None)
        
        return result
    except Exception as e:
        frappe.log_error(f"Error fetching {content_type} {content_id}: {str(e)}")
        return None


def search_content(logical_entity: str, search_term: str, limit: int = 10) -> List[Dict]:
    """Search for content dynamically"""
    config = DynamicConfig.get_doctype_mapping(logical_entity)
    if not config:
        return []
    
    doctype = config['doctype']
    search_fields = config.get('search_fields', ['name'])
    field_map = config['fields']
    
    filters = []
    for field in search_fields:
        filters.append([doctype, field, 'like', f'%{search_term}%'])
    
    try:
        results = frappe.get_all(
            doctype,
            or_filters=filters,
            fields=[field_map.get('id', 'name'), field_map.get('title', 'name')],
            limit=limit
        )
        
        return [{
            'id': r.get(field_map.get('id', 'name')),
            'title': r.get(field_map.get('title', 'name'))
        } for r in results]
    except Exception as e:
        frappe.log_error(f"Error searching {logical_entity}: {str(e)}")
        return []

# Hook to clear cache when config is updated  
def on_config_update(doc, method):  
    """Clear cache when AI Integration Config is updated"""  
    DynamicConfig.clear_cache()  
  
# Register the hook  
if frappe.db:  
    frappe.whitelist()(on_config_update)
    
# ========== API Integration Example ==========

from tap_ai.services.router import _get_history_from_cache, _save_history_to_cache, _append_history_to_db, get_session_transcript, list_sessions_for_user

def get_or_create_session_id(user_id: str) -> str:
    """Get or create an active session ID for a user"""
    key = f"active_session_{user_id}"
    session_id = frappe.cache().get(key)
    if not session_id:
        import uuid
        session_id = uuid.uuid4().hex[:16]
        frappe.cache().set(key, session_id, expire_in_seconds=3600)  # 1 hour
    return session_id


  
@frappe.whitelist()  
def query_endpoint(**kwargs):  
    """  
    Dynamic API endpoint that adapts to LMS changes  
    NOW HANDLES ENROLLMENT CHILD TABLE!  
    """  
    # Extract params  
    user_type = kwargs.get('user_type')  
    glific_id = kwargs.get('glific_id')  
    context = kwargs.get('context', {})  
    batch_id = context.get('batch_id')  # Optional specific batch  
    query = kwargs.get('query', '')  # Get the actual query  
      
    if not query:  
        return {'success': False, 'error': 'Query parameter is required'}  
      
    # Validate request  
    is_valid, error = DynamicConfig.validate_request(user_type, kwargs)  
    if not is_valid:  
        return {'success': False, 'error': error}  
      
    # Get user profile dynamically (with enrollment handling!)  
    user_profile = DynamicConfig.get_user_profile(user_type, glific_id, batch_id)  
    if not user_profile:  
        return {'success': False, 'error': f'{user_type.title()} not found'}  
      
    # Get content details if provided  
    content_details = None  
    content_type = context.get('content_type')  
    content_id = context.get('content_id')  
      
    if content_type and content_id:  
        if not DynamicConfig.validate_content_type(user_type, content_type):  
            return {'success': False, 'error': f'Invalid content_type: {content_type}'}  
          
        content_details = get_content_details(content_type, content_id)  
      
    # Get or create session ID for conversation grouping  
    session_id = kwargs.get('session_id') or get_or_create_session_id(user_profile['name'])  
      
    # Get chat history for context using session-aware cache  
    history = _get_history_from_cache(user_profile['name'], session_id=session_id)  
      
    # Process query with TAP AI - ACTUAL IMPLEMENTATION  
    try:  
        result = process_query(  
            query=query,  
            user_profile=user_profile,  
            content_details=content_details,  
            chat_history=history  
        )  
          
        # Update and persist chat history  
        history.append({"role": "user", "content": query})  
        history.append({"role": "assistant", "content": result.get("answer", "")})  
        _save_history_to_cache(user_profile['name'], history, session_id=session_id)  
        _append_history_to_db(  
            user_profile['name'],  
            [{"role": "user", "content": query}, {"role": "assistant", "content": result.get("answer", "")}],  
            session_id=session_id,  
            metadata={"source": "api"}  
        )  
          
        return {  
            'success': True,  
            'answer': result.get('answer', 'No answer generated'),  
            'session_id': session_id,  
            'user_profile': {  
                'name': user_profile['name'],  
                'batch': user_profile['batch'],  # From current enrollment  
                'enrollments': user_profile.get('enrollments', [])  # All enrollments  
            },  
            'content_details': content_details,  
            'metadata': {  
                'primary_engine': result.get('primary_engine'),  
                'routed_doctypes': result.get('routed_doctypes', []),  
                'fallback_used': result.get('fallback_used', False)  
            }  
        }  
          
    except Exception as e:  
        frappe.log_error(f"Dynamic config query failed: {str(e)}")  
        return {  
            'success': False,   
            'error': f'AI processing failed: {str(e)}'  
        }


@frappe.whitelist()
def get_transcript(session_id: str, user_id: Optional[str] = None, limit: Optional[int] = None):
    """Get full transcript for a session"""
    try:
        transcript = get_session_transcript(session_id, user_id=user_id, limit=limit)
        return {'success': True, 'transcript': transcript}
    except Exception as e:
        frappe.log_error(f"Transcript retrieval failed: {str(e)}")
        return {'success': False, 'error': str(e)}


@frappe.whitelist()
def list_user_sessions(user_id: str, limit: int = 20):
    """List all sessions for a user"""
    try:
        sessions = list_sessions_for_user(user_id, limit=limit)
        return {'success': True, 'sessions': sessions}
    except Exception as e:
        frappe.log_error(f"Session listing failed: {str(e)}")
        return {'success': False, 'error': str(e)}