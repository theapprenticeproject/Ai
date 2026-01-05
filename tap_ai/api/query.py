# tap_ai/api/query.py
"""
TAP AI Query API - Dynamic Configuration Version with Optional User Context
Handles queries with graceful degradation for user context
"""

import frappe
from frappe import _
from tap_ai.utils.dynamic_config import DynamicConfig, get_content_details

@frappe.whitelist(allow_guest=True)
def query(**kwargs):
    """
    Main TAP AI query endpoint with optional user context
    
    Args:
        q (str): User query text (REQUIRED)
        user_type (str): 'student' or 'teacher' (OPTIONAL)
        phone (str): Phone number (OPTIONAL)
        name (str): User name (OPTIONAL)
        glific_id (str): Glific contact ID (OPTIONAL)
        batch_id (str): Batch ID (OPTIONAL)
        context (dict): Context information with content_type/content_id (OPTIONAL)
    
    Returns:
        dict: Response with answer, metadata, and context level
    """
    try:
        # ============================================
        # 1. Extract Parameters
        # ============================================
        query_text = kwargs.get('q')
        user_type = kwargs.get('user_type')
        glific_id = kwargs.get('glific_id')
        phone = kwargs.get('phone')
        name = kwargs.get('name')
        batch_id = kwargs.get('batch_id')
        context = kwargs.get('context', {})
        
        # Validate required parameter
        if not query_text:
            return {
                'success': False,
                'error': 'Query text (q) is required'
            }
        
        # ============================================
        # 2. Get User Profile (OPTIONAL - Graceful Degradation)
        # ============================================
        user_profile = None
        user_context_level = "none"
        
        if user_type and glific_id:
            # Try to get full user profile from database
            try:
                user_profile = DynamicConfig.get_user_profile(
                    user_type,
                    glific_id,
                    batch_id
                )
                
                if user_profile:
                    # Full context - user found in database
                    user_context_level = "full"
                    print(f"> Full user context: {user_profile.get('name')} | Grade {user_profile.get('grade')} | Batch {user_profile.get('batch')}")
                else:
                    # User not found in DB - create minimal profile from provided data
                    user_context_level = "partial"
                    user_profile = {
                        'type': user_type,
                        'name': name or 'there',
                        'phone': phone,
                        'glific_id': glific_id,
                        'grade': None,
                        'batch': None,
                        'enrollments': [],
                        'current_enrollment': None
                    }
                    print(f"> User not found in DB, using partial context: {user_type} - {name}")
                    
            except Exception as e:
                # Error fetching user - continue with partial context
                frappe.log_error(
                    f"User profile fetch error: {str(e)}\nUser: {user_type}/{glific_id}",
                    "TAP AI Query API - User Fetch"
                )
                user_context_level = "partial"
                user_profile = {
                    'type': user_type,
                    'name': name or 'there',
                    'phone': phone,
                    'glific_id': glific_id,
                    'grade': None,
                    'batch': None
                }
                print(f"> Error fetching user, using partial context: {user_type}")
        
        elif user_type:
            # Only user_type provided (no glific_id)
            user_context_level = "partial"
            user_profile = {
                'type': user_type,
                'name': name or 'there',
                'phone': phone
            }
            print(f"> Partial user context: {user_type} only")
        
        else:
            # No user context at all - anonymous query
            user_context_level = "none"
            user_profile = None
            print("> Anonymous query - no user context")
        
        # ============================================
        # 3. Get Content Details (OPTIONAL)
        # ============================================
        content_details = None
        content_type = context.get('content_type')
        content_id = context.get('content_id')
        
        if content_type and content_id:
            # Validate content type only if we have user_type
            if user_type:
                if not DynamicConfig.validate_content_type(user_type, content_type):
                    return {
                        'success': False,
                        'error': f'Invalid content_type: {content_type} for {user_type}'
                    }
            
            # Fetch content details
            try:
                content_details = get_content_details(content_type, content_id)
                if content_details:
                    print(f"> Content context: {content_type}/{content_id}")
                else:
                    print(f"> Content not found: {content_type}/{content_id}")
                    frappe.log_error(
                        f"Content not found: {content_type}/{content_id}",
                        "TAP AI Query API - Content Fetch"
                    )
            except Exception as e:
                frappe.log_error(
                    f"Content fetch error: {str(e)}\nContent: {content_type}/{content_id}",
                    "TAP AI Query API - Content Fetch"
                )
        
        # ============================================
        # 4. Process Query with TAP AI Router
        # ============================================
        from tap_ai.services.router import process_query
        
        # Process query with optional user profile and content
        answer_data = process_query(
            query=query_text,
            user_profile=user_profile,  # Can be None or partial
            content_details=content_details,
            chat_history=[]  # TODO: Implement conversation history management
        )
        
        # ============================================
        # 5. Build Response
        # ============================================
        response = {
            'success': True,
            'answer': answer_data.get('answer', 'I could not generate an answer.'),
            'tool_used': answer_data.get('tool_used'),
            'user_context_level': user_context_level
        }
        
        # Add SQL query if available
        if answer_data.get('sql_query'):
            response['sql_query'] = answer_data['sql_query']
        
        # Add search info if available
        if answer_data.get('search_info'):
            response['search_info'] = answer_data['search_info']
        
        # Add user profile info to response (sanitized)
        if user_profile and user_context_level in ["partial", "full"]:
            response['user_profile'] = {
                'name': user_profile.get('name'),
                'type': user_profile.get('type'),
                'batch': user_profile.get('batch'),
                'grade': user_profile.get('grade')
            }
            
            # Add enrollment info for students with full context
            if user_context_level == "full" and user_profile.get('type') == 'student':
                current_enrollment = user_profile.get('current_enrollment')
                if current_enrollment:
                    response['user_profile']['current_course'] = current_enrollment.get('course')
        
        # Add content details if available
        if content_details:
            response['content_details'] = {
                'type': content_type,
                'id': content_id,
                'title': content_details.get('title'),
                'url': content_details.get('url')
            }
        
        # Add helpful note for partial context
        if user_context_level == "partial" and glific_id:
            response['note'] = "User not found in database. Providing general response with basic personalization."
        
        return response
        
    except Exception as e:
        # Log full error for debugging
        frappe.log_error(
            f"TAP AI Query Error: {str(e)}\nRequest: {kwargs}",
            "TAP AI Query API Error"
        )
        
        # Return user-friendly error
        return {
            'success': False,
            'error': 'An error occurred processing your query. Please try again.',
            'details': str(e) if frappe.conf.get('developer_mode') else None
        }


@frappe.whitelist(allow_guest=False)
def get_user_details(**kwargs):
    """
    Get user profile details (requires authentication)
    
    Args:
        user_type (str): 'student' or 'teacher'
        glific_id (str): Glific contact ID
        batch_id (str): Batch ID (optional)
    
    Returns:
        dict: User profile or error
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
    Get content details (requires authentication)
    
    Args:
        content_type (str): Type of content (video, quiz, assignment, etc.)
        content_id (str): Content ID
    
    Returns:
        dict: Content details or error
    """
    try:
        content_type = kwargs.get('content_type')
        content_id = kwargs.get('content_id')
        
        if not content_type or not content_id:
            return {
                'success': False,
                'error': 'content_type and content_id are required'
            }
        
        details = get_content_details(content_type, content_id)
        
        if not details:
            return {
                'success': False,
                'error': f'{content_type.title()} not found'
            }
        
        return {
            'success': True,
            'content': details
        }
        
    except Exception as e:
        frappe.log_error(f"Get Content Error: {str(e)}", "TAP AI API Error")
        return {
            'success': False,
            'error': 'An error occurred fetching content details'
        }
