# tap_ai/api/query.py
"""
TAP AI Query API - Dynamic Configuration Version
Handles all student and teacher queries using dynamic field mapping
"""

import frappe
from frappe import _
from tap_ai.utils.dynamic_config import DynamicConfig, get_content_details, get_user_profile

@frappe.whitelist(allow_guest=False)
def query(**kwargs):
    """
    Main TAP AI query endpoint with dynamic configuration
    
    Args:
        q (str): User query text
        user_type (str): 'student' or 'teacher'
        phone (str): Phone number
        name (str): User name
        glific_id (str): Glific contact ID
        context (dict): Context information
            For students:
                - content_type: 'video', 'quiz', 'assignment', 'general'
                - content_id: ID of the content
                - batch_id: Optional specific batch ID
            For teachers:
                - query_type: 'activity', 'student', 'content'
                - batch_id: Batch ID
                - target_id: Optional target student/content ID
    
    Returns:
        dict: Response with answer and metadata
    """
    try:
        # ============================================
        # 1. Extract and Validate Parameters
        # ============================================
        query_text = kwargs.get('q')
        user_type = kwargs.get('user_type')
        glific_id = kwargs.get('glific_id')
        phone = kwargs.get('phone')
        name = kwargs.get('name')
        context = kwargs.get('context', {})
        
        if not query_text:
            return {
                'success': False,
                'error': 'Query text (q) is required'
            }
        
        # Validate request using dynamic config
        is_valid, error = DynamicConfig.validate_request(user_type, kwargs)
        if not is_valid:
            return {
                'success': False,
                'error': error
            }
        
        # ============================================
        # 2. Get User Profile (Dynamic)
        # ============================================
        batch_id = context.get('batch_id')  # Optional specific batch
        
        user_profile = DynamicConfig.get_user_profile(
            user_type,
            glific_id,
            batch_id  # For students with multiple enrollments
        )
        
        if not user_profile:
            return {
                'success': False,
                'error': f'{user_type.title()} with glific_id {glific_id} not found'
            }
        
        # ============================================
        # 3. Get Content Details (Dynamic)
        # ============================================
        content_details = None
        content_type = context.get('content_type')
        content_id = context.get('content_id')
        
        if content_type and content_id:
            # Validate content type is allowed for this user
            if not DynamicConfig.validate_content_type(user_type, content_type):
                return {
                    'success': False,
                    'error': f'Invalid content_type: {content_type} for {user_type}'
                }
            
            # Fetch content details dynamically
            content_details = get_content_details(content_type, content_id)
            
            if not content_details:
                frappe.log_error(
                    f"Content not found: {content_type}/{content_id}",
                    "TAP AI Query API"
                )
                # Don't fail - continue without content details
        
        # ============================================
        # 4. Build Context for AI
        # ============================================
        ai_context = {
            'user': {
                'type': user_type,
                'name': user_profile['name'],
                'phone': user_profile['phone'],
                'batch': user_profile['batch'],
                'grade': user_profile.get('grade')
            },
            'content': content_details,
            'query_text': query_text
        }
        
        # Add enrollment info for students
        if user_type == 'student':
            ai_context['user']['enrollments'] = user_profile.get('enrollments', [])
            ai_context['user']['current_enrollment'] = user_profile.get('current_enrollment')
        
        # ============================================
        # 5. Process Query with TAP AI
        # ============================================
        # Import your AI processing logic
        from tap_ai.services.router import process_query
        
        answer = process_query(
            query=query_text,
            user_profile=user_profile,
            content_details=content_details,
            context=ai_context
        )
        
        # ============================================
        # 6. Return Response
        # ============================================
        return {
            'success': True,
            'answer': answer,
            'user': {
                'name': user_profile['name'],
                'type': user_type,
                'batch': user_profile['batch']
            },
            'content': {
                'type': content_type,
                'title': content_details.get('title') if content_details else None
            } if content_details else None
        }
        
    except Exception as e:
        frappe.log_error(
            f"TAP AI Query Error: {str(e)}\nRequest: {kwargs}",
            "TAP AI Query API Error"
        )
        return {
            'success': False,
            'error': 'An error occurred processing your query. Please try again.'
        }


@frappe.whitelist(allow_guest=False)
def get_user_details(**kwargs):
    """
    Get user profile details
    
    Args:
        user_type (str): 'student' or 'teacher'
        glific_id (str): Glific contact ID
        batch_id (str): Optional specific batch ID
    
    Returns:
        dict: User profile with enrollment information
    """
    try:
        user_type = kwargs.get('user_type')
        glific_id = kwargs.get('glific_id')
        batch_id = kwargs.get('batch_id')
        
        if not user_type or not glific_id:
            return {
                'success': False,
                'error': 'user_type and glific_id are required'
            }
        
        profile = DynamicConfig.get_user_profile(user_type, glific_id, batch_id)
        
        if not profile:
            return {
                'success': False,
                'error': f'{user_type.title()} not found'
            }
        
        # Remove internal _raw field
        profile.pop('_raw', None)
        
        return {
            'success': True,
            'profile': profile
        }
        
    except Exception as e:
        frappe.log_error(f"Get User Details Error: {str(e)}", "TAP AI API Error")
        return {
            'success': False,
            'error': 'An error occurred fetching user details'
        }


@frappe.whitelist(allow_guest=False)
def get_content(**kwargs):
    """
    Get content details
    
    Args:
        content_type (str): 'video', 'quiz', 'assignment'
        content_id (str): Content ID
    
    Returns:
        dict: Content details
    """
    try:
        content_type = kwargs.get('content_type')
        content_id = kwargs.get('content_id')
        
        if not content_type or not content_id:
            return {
                'success': False,
                'error': 'content_type and content_id are required'
            }
        
        content = get_content_details(content_type, content_id)
        
        if not content:
            return {
                'success': False,
                'error': f'{content_type.title()} not found'
            }
        
        return {
            'success': True,
            'content': content
        }
        
    except Exception as e:
        frappe.log_error(f"Get Content Error: {str(e)}", "TAP AI API Error")
        return {
            'success': False,
            'error': 'An error occurred fetching content details'
        }


@frappe.whitelist(allow_guest=False)
def search_content(**kwargs):
    """
    Search for content
    
    Args:
        content_type (str): 'video', 'quiz', 'assignment'
        search_term (str): Search query
        limit (int): Max results (default 10)
    
    Returns:
        dict: Search results
    """
    try:
        from tap_ai.utils.dynamic_config import search_content as search_fn
        
        content_type = kwargs.get('content_type')
        search_term = kwargs.get('search_term')
        limit = int(kwargs.get('limit', 10))
        
        if not content_type or not search_term:
            return {
                'success': False,
                'error': 'content_type and search_term are required'
            }
        
        results = search_fn(content_type, search_term, limit)
        
        return {
            'success': True,
            'results': results,
            'count': len(results)
        }
        
    except Exception as e:
        frappe.log_error(f"Search Content Error: {str(e)}", "TAP AI API Error")
        return {
            'success': False,
            'error': 'An error occurred searching content'
        }


@frappe.whitelist(allow_guest=False)
def clear_config_cache():
    """
    Clear dynamic configuration cache
    Useful after updating AI Integration Config
    
    Returns:
        dict: Success message
    """
    try:
        # Check permissions
        if not frappe.has_permission("AI Integration Config", "write"):
            return {
                'success': False,
                'error': 'Permission denied'
            }
        
        DynamicConfig.clear_cache()
        
        return {
            'success': True,
            'message': 'Configuration cache cleared successfully'
        }
        
    except Exception as e:
        frappe.log_error(f"Clear Cache Error: {str(e)}", "TAP AI API Error")
        return {
            'success': False,
            'error': 'An error occurred clearing cache'
        }
