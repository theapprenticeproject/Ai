import frappe
from frappe import _
from frappe.utils import cstr

@frappe.whitelist(allow_guest=False)
def get_video_urls(course_level=None, week_no=None, language=None, video_source=None):
    """
    API to get video URLs with filters - Returns individual video objects (RECOMMENDED)
    
    Args:
        course_level: Course Level name
        week_no: Week number
        language: Language for translations
        video_source: Type of video source (youtube/plio/file)
    
    Returns:
        Individual video objects with flattened structure
    """
    
    try:
        # Base query to get video data
        base_query = """
        SELECT
            vc.name as video_id,
            vc.video_name,
            vc.video_youtube_url,
            vc.video_plio_url,
            vc.video_file,
            vc.duration,
            vc.description,
            vc.difficulty_tier,
            vc.estimated_duration,
            lu.unit_name,
            lu.order as unit_order,
            cl.name as course_level_id,
            cl.name1 as course_level_name,
            lul.week_no,
            cv.name1 as vertical_name
        FROM 
            `tabVideoClass` vc
        INNER JOIN 
            `tabUnitContentItem` uci ON uci.content = vc.name 
            AND uci.content_type = 'VideoClass'
        INNER JOIN 
            `tabLearningUnit` lu ON lu.name = uci.parent
        INNER JOIN 
            `tabLearningUnitList` lul ON lul.learning_unit = lu.name
        INNER JOIN 
            `tabCourse Level` cl ON cl.name = lul.parent
        LEFT JOIN 
            `tabCourse Verticals` cv ON cv.name = vc.course_vertical
        WHERE 1=1
        """
        
        params = []
        
        # Add filters
        if course_level:
            base_query += " AND cl.name = %s"
            params.append(course_level)
            
        if week_no:
            base_query += " AND lul.week_no = %s"
            params.append(int(week_no))
        
        # Order the results
        base_query += " ORDER BY lul.week_no, lu.order, vc.name"
        
        # Execute query
        base_data = frappe.db.sql(base_query, params, as_dict=True)
        
        if not base_data:
            return {
                "status": "success",
                "message": "No videos found",
                "count": 0
            }
        
        # Get translations if language is specified
        translations = {}
        if language and language.lower() != 'default':
            video_ids = [row.video_id for row in base_data]
            if video_ids:
                video_ids_placeholders = ','.join(['%s'] * len(video_ids))
                translation_query = f"""
                SELECT 
                    parent as video_id,
                    language,
                    translated_name,
                    translated_description,
                    video_youtube_url as translated_youtube_url,
                    video_plio_url as translated_plio_url,
                    video_file as translated_video_file
                FROM `tabVideoTranslation`
                WHERE parent IN ({video_ids_placeholders})
                AND language = %s
                """
                
                translation_params = video_ids + [language]
                translation_data = frappe.db.sql(translation_query, translation_params, as_dict=True)
                
                for trans in translation_data:
                    translations[trans.video_id] = trans
        
        # Process results
        result = []
        for row in base_data:
            translation = translations.get(row.video_id, {})
            
            video_data = {
                "status": "success",
                "video_id": row.video_id,
                "video_name": translation.get("translated_name") or row.video_name,
                "description": translation.get("translated_description") or row.description,
                "unit_name": row.unit_name,
                "unit_order": row.unit_order,
                "course_level": row.course_level_name,
                "course_level_id": row.course_level_id,
                "week_no": row.week_no,
                "vertical": row.vertical_name,
                "difficulty_tier": row.difficulty_tier,
                "estimated_duration": row.estimated_duration,
                "duration": row.duration,
                "language": language if translation else "default"
            }
            
            # Add video URLs as direct properties
            has_video = False
            
            # Use translated URLs if available
            if translation:
                if translation.get("translated_youtube_url"):
                    video_data["youtube"] = translation["translated_youtube_url"]
                    has_video = True
                if translation.get("translated_plio_url"):
                    video_data["plio"] = translation["translated_plio_url"]
                    has_video = True
                if translation.get("translated_video_file"):
                    video_data["file"] = get_file_url(translation["translated_video_file"])
                    has_video = True
            
            # Fallback to original URLs
            if not has_video or not translation:
                if row.video_youtube_url:
                    video_data["youtube"] = row.video_youtube_url
                    has_video = True
                if row.video_plio_url:
                    video_data["plio"] = row.video_plio_url
                    has_video = True
                if row.video_file:
                    video_data["file"] = get_file_url(row.video_file)
                    has_video = True
            
            # Filter by video source if specified
            if video_source:
                source_key = video_source.lower()
                if source_key not in video_data:
                    continue  # Skip if requested source not available
                
                # Keep only the requested source
                sources_to_remove = []
                if source_key != "youtube" and "youtube" in video_data:
                    sources_to_remove.append("youtube")
                if source_key != "plio" and "plio" in video_data:
                    sources_to_remove.append("plio")
                if source_key != "file" and "file" in video_data:
                    sources_to_remove.append("file")
                
                for source in sources_to_remove:
                    del video_data[source]
            
            # Add to result if has video URLs
            if has_video:
                result.append(video_data)
        
        # Add count to each video
        total_count = len(result)
        for video in result:
            video["count"] = total_count
        
        # Return single object if only one video, array if multiple
        if total_count == 1:
            return result[0]
        else:
            return result
        
    except Exception as e:
        frappe.log_error(f"Error in get_video_urls API: {str(e)}", "Video URLs API Error")
        return {
            "status": "error",
            "message": str(e)
        }

@frappe.whitelist(allow_guest=False)
def get_video_urls_aggregated(course_level=None, week_no=None, language=None, video_source=None):
    """
    API to get video URLs aggregated by week - Returns comma-separated values
    
    Args:
        course_level: Course Level name
        week_no: Week number
        language: Language for translations
        video_source: Type of video source (youtube/plio/file)
    
    Returns:
        Aggregated video data per week with comma-separated URLs
    """
    
    try:
        # Base query to get video data
        base_query = """
        SELECT
            vc.name as video_id,
            vc.video_name,
            vc.video_youtube_url,
            vc.video_plio_url,
            vc.video_file,
            vc.duration,
            vc.description,
            vc.difficulty_tier,
            vc.estimated_duration,
            lu.unit_name,
            lu.order as unit_order,
            cl.name as course_level_id,
            cl.name1 as course_level_name,
            lul.week_no,
            cv.name1 as vertical_name
        FROM 
            `tabVideoClass` vc
        INNER JOIN 
            `tabUnitContentItem` uci ON uci.content = vc.name 
            AND uci.content_type = 'VideoClass'
        INNER JOIN 
            `tabLearningUnit` lu ON lu.name = uci.parent
        INNER JOIN 
            `tabLearningUnitList` lul ON lul.learning_unit = lu.name
        INNER JOIN 
            `tabCourse Level` cl ON cl.name = lul.parent
        LEFT JOIN 
            `tabCourse Verticals` cv ON cv.name = vc.course_vertical
        WHERE 1=1
        """
        
        params = []
        
        # Add filters
        if course_level:
            base_query += " AND cl.name = %s"
            params.append(course_level)
            
        if week_no:
            base_query += " AND lul.week_no = %s"
            params.append(int(week_no))
        
        # Order the results
        base_query += " ORDER BY lul.week_no, lu.order, vc.name"
        
        # Execute query
        base_data = frappe.db.sql(base_query, params, as_dict=True)
        
        if not base_data:
            return {
                "status": "success",
                "message": "No videos found",
                "count": 0
            }
        
        # Get translations if language is specified
        translations = {}
        if language and language.lower() != 'default':
            video_ids = [row.video_id for row in base_data]
            if video_ids:
                video_ids_placeholders = ','.join(['%s'] * len(video_ids))
                translation_query = f"""
                SELECT 
                    parent as video_id,
                    language,
                    translated_name,
                    translated_description,
                    video_youtube_url as translated_youtube_url,
                    video_plio_url as translated_plio_url,
                    video_file as translated_video_file
                FROM `tabVideoTranslation`
                WHERE parent IN ({video_ids_placeholders})
                AND language = %s
                """
                
                translation_params = video_ids + [language]
                translation_data = frappe.db.sql(translation_query, translation_params, as_dict=True)
                
                for trans in translation_data:
                    translations[trans.video_id] = trans
        
        # Group videos by week
        weeks_data = {}
        
        for row in base_data:
            translation = translations.get(row.video_id, {})
            week_key = row.week_no
            
            if week_key not in weeks_data:
                weeks_data[week_key] = {
                    "videos": [],
                    "course_level": row.course_level_name,
                    "course_level_id": row.course_level_id,
                    "vertical": row.vertical_name,
                    "week_no": row.week_no
                }
            
            # Prepare video data
            video_info = {
                "video_id": row.video_id,
                "video_name": translation.get("translated_name") or row.video_name,
                "description": translation.get("translated_description") or row.description,
                "unit_name": row.unit_name,
                "unit_order": row.unit_order,
                "difficulty_tier": row.difficulty_tier,
                "estimated_duration": row.estimated_duration,
                "duration": row.duration,
                "youtube": None,
                "plio": None,
                "file": None
            }
            
            # Add URLs
            if translation:
                if translation.get("translated_youtube_url"):
                    video_info["youtube"] = translation["translated_youtube_url"]
                if translation.get("translated_plio_url"):
                    video_info["plio"] = translation["translated_plio_url"]
                if translation.get("translated_video_file"):
                    video_info["file"] = get_file_url(translation["translated_video_file"])
            
            # Fallback to original URLs
            if not video_info["youtube"] and row.video_youtube_url:
                video_info["youtube"] = row.video_youtube_url
            if not video_info["plio"] and row.video_plio_url:
                video_info["plio"] = row.video_plio_url
            if not video_info["file"] and row.video_file:
                video_info["file"] = get_file_url(row.video_file)
            
            weeks_data[week_key]["videos"].append(video_info)
        
        # Create aggregated response
        result = []
        
        for week_no_key, week_data in weeks_data.items():
            videos = week_data["videos"]
            
            # Filter by video source if specified
            if video_source:
                source_key = video_source.lower()
                videos = [v for v in videos if v.get(source_key)]
                
                if not videos:
                    continue
            
            if not videos:
                continue
                
            # Aggregate data
            aggregated = {
                "status": "success",
                "video_id": f"week-{week_no_key}-videos",
                "video_name": ", ".join([v["video_name"] for v in videos if v["video_name"]]),
                "description": " | ".join([v["description"] for v in videos if v["description"]]),
                "unit_name": ", ".join([v["unit_name"] for v in videos if v["unit_name"]]),
                "course_level": week_data["course_level"],
                "course_level_id": week_data["course_level_id"],
                "week_no": week_no_key,
                "vertical": week_data["vertical"],
                "difficulty_tier": ", ".join(list(set([v["difficulty_tier"] for v in videos if v["difficulty_tier"]]))),
                "estimated_duration": ", ".join([v["estimated_duration"] for v in videos if v["estimated_duration"]]),
                "duration": ", ".join([v["duration"] for v in videos if v["duration"]]),
                "language": language if language else "default",
                "count": len(videos)
            }
            
            # Aggregate URLs
            youtube_urls = [v["youtube"] for v in videos if v["youtube"]]
            plio_urls = [v["plio"] for v in videos if v["plio"]]
            file_urls = [v["file"] for v in videos if v["file"]]
            
            if youtube_urls:
                aggregated["youtube"] = ",".join(youtube_urls)
            if plio_urls:
                aggregated["plio"] = ",".join(plio_urls)
            if file_urls:
                aggregated["file"] = ",".join(file_urls)
            
            # Filter by video source if specified
            if video_source:
                source_key = video_source.lower()
                if source_key not in aggregated:
                    continue
                
                # Keep only the requested source
                sources_to_remove = []
                if source_key != "youtube" and "youtube" in aggregated:
                    sources_to_remove.append("youtube")
                if source_key != "plio" and "plio" in aggregated:
                    sources_to_remove.append("plio")
                if source_key != "file" and "file" in aggregated:
                    sources_to_remove.append("file")
                
                for source in sources_to_remove:
                    del aggregated[source]
            
            result.append(aggregated)
        
        # Return single object if only one week, array if multiple weeks
        if len(result) == 1:
            return result[0]
        else:
            return result
        
    except Exception as e:
        frappe.log_error(f"Error in get_video_urls_aggregated API: {str(e)}", "Aggregated Video URLs API Error")
        return {
            "status": "error",
            "message": str(e)
        }

def get_file_url(file_path):
    """Convert file path to full URL"""
    if not file_path:
        return None
    
    if file_path.startswith('/files/'):
        return frappe.utils.get_url() + file_path
    elif file_path.startswith('http'):
        return file_path
    else:
        return frappe.utils.get_url() + '/files/' + file_path

@frappe.whitelist(allow_guest=False)
def get_available_filters():
    """
    API to get available filter options
    """
    try:
        # Get available course levels
        course_levels = frappe.db.sql("""
            SELECT name, name1 as display_name 
            FROM `tabCourse Level` 
            ORDER BY name1
        """, as_dict=True)
        
        # Get available weeks
        weeks = frappe.db.sql("""
            SELECT DISTINCT week_no 
            FROM `tabLearningUnitList` 
            WHERE week_no IS NOT NULL 
            ORDER BY week_no
        """, as_dict=True)
        
        # Get available languages
        languages = frappe.db.sql("""
            SELECT DISTINCT language 
            FROM `tabVideoTranslation` 
            WHERE language IS NOT NULL
            ORDER BY language
        """, as_dict=True)
        
        # Get course verticals
        verticals = frappe.db.sql("""
            SELECT name, name1 as display_name 
            FROM `tabCourse Verticals` 
            ORDER BY name1
        """, as_dict=True)
        
        return {
            "status": "success",
            "course_levels": course_levels,
            "weeks": [w.week_no for w in weeks],
            "languages": [l.language for l in languages],
            "video_sources": ["youtube", "plio", "file"],
            "verticals": verticals
        }
        
    except Exception as e:
        frappe.log_error(f"Error in get_available_filters API: {str(e)}", "Filters API Error")
        return {
            "status": "error",
            "message": str(e)
        }

@frappe.whitelist(allow_guest=False)
def get_video_statistics():
    """
    API to get video statistics
    """
    try:
        # Basic video counts
        video_stats = frappe.db.sql("""
            SELECT 
                COUNT(*) as total_videos,
                COUNT(CASE WHEN video_youtube_url IS NOT NULL AND video_youtube_url != '' THEN 1 END) as youtube_videos,
                COUNT(CASE WHEN video_plio_url IS NOT NULL AND video_plio_url != '' THEN 1 END) as plio_videos,
                COUNT(CASE WHEN video_file IS NOT NULL AND video_file != '' THEN 1 END) as file_videos
            FROM `tabVideoClass`
        """, as_dict=True)
        
        # Course and unit counts
        course_stats = frappe.db.sql("""
            SELECT 
                COUNT(DISTINCT cl.name) as total_courses,
                COUNT(DISTINCT lul.week_no) as total_weeks,
                COUNT(DISTINCT cv.name) as total_verticals
            FROM `tabCourse Level` cl
            LEFT JOIN `tabLearningUnitList` lul ON lul.parent = cl.name
            LEFT JOIN `tabCourse Verticals` cv ON cv.name = cl.vertical
        """, as_dict=True)
        
        # Language counts
        language_stats = frappe.db.sql("""
            SELECT COUNT(DISTINCT language) as available_languages
            FROM `tabVideoTranslation`
            WHERE language IS NOT NULL
        """, as_dict=True)
        
        # Combine all statistics
        combined_stats = {}
        if video_stats:
            combined_stats.update(video_stats[0])
        if course_stats:
            combined_stats.update(course_stats[0])
        if language_stats:
            combined_stats.update(language_stats[0])
        
        return {
            "status": "success",
            "statistics": combined_stats
        }
        
    except Exception as e:
        frappe.log_error(f"Error in get_video_statistics API: {str(e)}", "Statistics API Error")
        return {
            "status": "error",
            "message": str(e)
        }

@frappe.whitelist(allow_guest=False)
def test_connection():
    """
    Simple test endpoint to verify API is working
    """
    try:
        # Test basic database connection
        result = frappe.db.sql("SELECT COUNT(*) as video_count FROM `tabVideoClass`", as_dict=True)
        
        return {
            "status": "success",
            "message": "API is working correctly",
            "video_count": result[0].video_count if result else 0,
            "endpoints": [
                "get_video_urls - Individual video objects (recommended)",
                "get_video_urls_aggregated - Comma-separated aggregated format",
                "get_available_filters - Available filter options",
                "get_video_statistics - Video statistics",
                "test_connection - Connection test"
            ]
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"API test failed: {str(e)}"
        }
