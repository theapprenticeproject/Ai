import frappe
import requests
import json
from datetime import datetime, timedelta, timezone
from dateutil.parser import isoparse

def get_glific_settings():
    return frappe.get_single("Glific Settings")

def get_glific_auth_headers():
    settings = get_glific_settings()
    current_time = datetime.now(timezone.utc)
    
    # Convert token_expiry_time to datetime if it's a string
    if settings.token_expiry_time:
        if isinstance(settings.token_expiry_time, str):
            settings.token_expiry_time = isoparse(settings.token_expiry_time)
        elif settings.token_expiry_time.tzinfo is None:
            settings.token_expiry_time = settings.token_expiry_time.replace(tzinfo=timezone.utc)
    
    if not settings.access_token or not settings.token_expiry_time or \
       current_time >= settings.token_expiry_time:
        # Token is expired or not set, get a new one
        url = f"{settings.api_url}/api/v1/session"
        payload = {
            "user": {
                "phone": settings.phone_number,
                "password": settings.password
            }
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()["data"]
            
            # Parse the token_expiry_time string to a timezone-aware datetime object
            token_expiry_time = isoparse(data["token_expiry_time"])
            
            # Update the Glific Settings directly in the database
            frappe.db.set_value("Glific Settings", settings.name, {
                "access_token": data["access_token"],
                "renewal_token": data["renewal_token"],
                "token_expiry_time": token_expiry_time
            }, update_modified=False)
            
            frappe.db.commit()
            
            return {
                "authorization": data["access_token"],
                "Content-Type": "application/json"
            }
        else:
            frappe.throw("Failed to authenticate with Glific API")
    else:
        return {
            "authorization": settings.access_token,
            "Content-Type": "application/json"
        }

def create_contact(name, phone, school_name, model_name, language_id, batch_id):
    settings = get_glific_settings()
    url = f"{settings.api_url}/api"
    headers = get_glific_auth_headers()

    # Prepare the fields dictionary
    fields = {
        "school": {
            "value": school_name,
            "type": "string",
            "inserted_at": datetime.now(timezone.utc).isoformat()
        },
        "model": {
            "value": model_name,
            "type": "string",
            "inserted_at": datetime.now(timezone.utc).isoformat()
        },
        "buddy_name": {
            "value": name,
            "type": "string",
            "inserted_at": datetime.now(timezone.utc).isoformat()
        },
        "batch_id": {
            "value": batch_id,
            "type": "string",
            "inserted_at": datetime.now(timezone.utc).isoformat()
        }
    }

    payload = {
        "query": "mutation createContact($input:ContactInput!) { createContact(input: $input) { contact { id name phone } errors { key message } } }",
        "variables": {
            "input": {
                "name": name,
                "phone": phone,
                "fields": json.dumps(fields),
                "languageId": int(language_id)
            }
        }
    }

    frappe.logger().info(f"Attempting to create Glific contact. Name: {name}, Phone: {phone}, School: {school_name}, Model: {model_name}, Language ID: {language_id}, Batch ID: {batch_id}")
    frappe.logger().info(f"Glific API URL: {url}")
    frappe.logger().info(f"Glific API Headers: {headers}")
    frappe.logger().info(f"Glific API Payload: {payload}")

    try:
        response = requests.post(url, json=payload, headers=headers)
        frappe.logger().info(f"Glific API response status: {response.status_code}")
        frappe.logger().info(f"Glific API response content: {response.text}")

        if response.status_code == 200:
            data = response.json()
            if "errors" in data:
                frappe.logger().error(f"Error creating Glific contact: {data['errors']}")
                return None
            if "data" in data and "createContact" in data["data"] and "contact" in data["data"]["createContact"]:
                contact = data["data"]["createContact"]["contact"]
                frappe.logger().info(f"Glific contact created successfully: {contact}")
                return contact
            else:
                frappe.logger().error(f"Unexpected response structure: {data}")
                return None
        else:
            frappe.logger().error(f"Failed to create Glific contact. Status code: {response.status_code}")
            return None
    except Exception as e:
        frappe.logger().error(f"Exception occurred while creating Glific contact: {str(e)}", exc_info=True)
        return None

def update_contact_fields(contact_id, fields_to_update):
    settings = get_glific_settings()
    url = f"{settings.api_url}/api"
    headers = get_glific_auth_headers()
    
    # First, fetch the current contact to get existing fields
    fetch_payload = {
        "query": """
        query contact($id: ID!) {
          contact(id: $id) {
            contact {
              id
              name
              phone
              fields
              language {
                label
              }
            }
          }
        }
        """,
        "variables": {
            "id": contact_id
        }
    }
    
    try:
        # Get current contact data
        fetch_response = requests.post(url, json=fetch_payload, headers=headers)
        fetch_response.raise_for_status()
        fetch_data = fetch_response.json()
        
        if "errors" in fetch_data:
            frappe.logger().error(f"Glific API Error in fetching contact: {fetch_data['errors']}")
            return False
        
        contact_data = fetch_data.get("data", {}).get("contact", {}).get("contact")
        if not contact_data:
            frappe.logger().error(f"Failed to fetch contact with ID: {contact_id}")
            return False
        
        # Parse existing fields
        existing_fields = {}
        if contact_data.get("fields"):
            try:
                existing_fields = json.loads(contact_data.get("fields", "{}"))
            except json.JSONDecodeError:
                frappe.logger().error(f"Failed to parse fields JSON for contact {contact_id}")
                existing_fields = {}
        
        # Log the existing fields for debugging
        frappe.logger().info(f"Existing fields for contact {contact_id}: {existing_fields}")
        
        # Update fields - keep existing fields and update only those provided
        updated_fields = existing_fields.copy()
        for key, value in fields_to_update.items():
            updated_fields[key] = {
                "value": value,
                "type": "string",
                "inserted_at": datetime.now(timezone.utc).isoformat()
            }
        
        # Use the updateContact mutation instead of updateContactFields
        update_payload = {
            "query": """
            mutation updateContact($id: ID!, $input:ContactInput!) {
              updateContact(id: $id, input: $input) {
                contact {
                  id
                  name
                  fields
                }
                errors {
                  key
                  message
                }
              }
            }
            """,
            "variables": {
                "id": contact_id,
                "input": {
                    "name": contact_data.get("name", ""),
                    "fields": json.dumps(updated_fields)
                }
            }
        }
        
        frappe.logger().info(f"Attempting to update Glific contact. ID: {contact_id}")
        frappe.logger().info(f"Glific API URL: {url}")
        frappe.logger().info(f"Glific API Headers: {headers}")
        frappe.logger().info(f"Glific API Payload: {update_payload}")
        
        update_response = requests.post(url, json=update_payload, headers=headers)
        frappe.logger().info(f"Glific API response status: {update_response.status_code}")
        frappe.logger().info(f"Glific API response content: {update_response.text}")
        
        update_response.raise_for_status()
        update_data = update_response.json()
        
        if "errors" in update_data:
            frappe.logger().error(f"Glific API Error in updating contact: {update_data['errors']}")
            return False
        
        contact = update_data.get("data", {}).get("updateContact", {}).get("contact")
        if contact:
            frappe.logger().info(f"Contact updated successfully: {contact}")
            return True
        else:
            frappe.logger().error(f"Failed to update contact. Response: {update_data}")
            return False
            
    except requests.exceptions.RequestException as e:
        frappe.logger().error(f"Error calling Glific API: {str(e)}")
        return False
    except Exception as e:
        frappe.logger().error(f"Unexpected error in update_contact_fields: {str(e)}")
        return False

def get_contact_by_phone(phone):
    settings = get_glific_settings()
    url = f"{settings.api_url}/api"
    headers = get_glific_auth_headers()
    payload = {
        "query": """
        query contactByPhone($phone: String!) {
          contactByPhone(phone: $phone) {
            contact {
              id
              name
              optinTime
              optoutTime
              phone
              bspStatus
              status
              lastMessageAt
              fields
              settings
            }
          }
        }
        """,
        "variables": {
            "phone": phone
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if "errors" in data:
            frappe.logger().error(f"Glific API Error in getting contact by phone: {data['errors']}")
            return None
        
        contact = data.get("data", {}).get("contactByPhone", {}).get("contact")
        if contact:
            return contact
        else:
            frappe.logger().error(f"Contact not found for phone: {phone}")
            return None
    except requests.exceptions.RequestException as e:
        frappe.logger().error(f"Error calling Glific API to get contact by phone: {str(e)}")
        return None

def optin_contact(phone, name):
    settings = get_glific_settings()
    url = f"{settings.api_url}/api"
    headers = get_glific_auth_headers()
    payload = {
        "query": """
        mutation optinContact($phone: String!, $name: String) {
          optinContact(phone: $phone, name: $name) {
            contact {
              id
              phone
              name
              lastMessageAt
              optinTime
              bspStatus
            }
            errors {
              key
              message
            }
          }
        }
        """,
        "variables": {
            "phone": phone,
            "name": name
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if "errors" in data:
            frappe.logger().error(f"Glific API Error in opting in contact: {data['errors']}")
            return False
        
        contact = data.get("data", {}).get("optinContact", {}).get("contact")
        if contact:
            frappe.logger().info(f"Contact opted in successfully: {contact}")
            return True
        else:
            frappe.logger().error(f"Failed to opt in contact. Response: {data}")
            return False
    except requests.exceptions.RequestException as e:
        frappe.logger().error(f"Error calling Glific API to opt in contact: {str(e)}")
        return False

def create_contact_old(name, phone):
    settings = get_glific_settings()
    url = f"{settings.api_url}/api"
    headers = get_glific_auth_headers()
    payload = {
        "query": "mutation createContact($input:ContactInput!) { createContact(input: $input) { contact { id name phone } errors { key message } } }",
        "variables": {
            "input": {
                "name": name,
                "phone": phone
            }
        }
    }

    frappe.logger().info(f"Attempting to create Glific contact. Name: {name}, Phone: {phone}")
    frappe.logger().info(f"Glific API URL: {url}")
    frappe.logger().info(f"Glific API Headers: {headers}")
    frappe.logger().info(f"Glific API Payload: {payload}")

    try:
        response = requests.post(url, json=payload, headers=headers)
        frappe.logger().info(f"Glific API response status: {response.status_code}")
        frappe.logger().info(f"Glific API response content: {response.text}")

        if response.status_code == 200:
            data = response.json()
            if "errors" in data:
                frappe.logger().error(f"Error creating Glific contact: {data['errors']}")
                return None
            if "data" in data and "createContact" in data["data"] and "contact" in data["data"]["createContact"]:
                contact = data["data"]["createContact"]["contact"]
                frappe.logger().info(f"Glific contact created successfully: {contact}")
                return contact
            else:
                frappe.logger().error(f"Unexpected response structure: {data}")
                return None
        else:
            frappe.logger().error(f"Failed to create Glific contact. Status code: {response.status_code}")
            return None
    except Exception as e:
        frappe.logger().error(f"Exception occurred while creating Glific contact: {str(e)}", exc_info=True)
        return None

def start_contact_flow(flow_id, contact_id, default_results):
    settings = get_glific_settings()
    url = f"{settings.api_url}/api"
    headers = get_glific_auth_headers()
    payload = {
        "query": """
        mutation startContactFlow($flowId: ID!, $contactId: ID!, $defaultResults: Json!) {
            startContactFlow(flowId: $flowId, contactId: $contactId, defaultResults: $defaultResults) {
                success
                errors {
                    key
                    message
                }
            }
        }
        """,
        "variables": {
            "flowId": flow_id,
            "contactId": contact_id,
            "defaultResults": json.dumps(default_results)
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if "errors" in data:
            frappe.logger().error(f"Glific API Error in starting flow: {data['errors']}")
            return False
        
        success = data.get("data", {}).get("startContactFlow", {}).get("success")
        if success:
            return True
        else:
            frappe.logger().error(f"Failed to start Glific flow. Response: {data}")
            return False
    except requests.exceptions.RequestException as e:
        frappe.logger().error(f"Error calling Glific API to start flow: {str(e)}")
        return False

def update_student_glific_ids(batch_size=100):
    def format_phone(phone):
        phone = phone.strip().replace(' ', '')
        if len(phone) == 10:
            return f"91{phone}"
        elif len(phone) == 12 and phone.startswith('91'):
            return phone
        else:
            return None

    students = frappe.get_all(
        "Student",
        filters={"glific_id": ["in", ["", None]]},
        fields=["name", "phone"],
        limit=batch_size
    )

    for student in students:
        formatted_phone = format_phone(student.phone)
        if not formatted_phone:
            frappe.logger().warning(f"Invalid phone number for student {student.name}: {student.phone}")
            continue

        glific_contact = get_contact_by_phone(formatted_phone)
        if glific_contact and 'id' in glific_contact:
            frappe.db.set_value("Student", student.name, "glific_id", glific_contact['id'])
            frappe.logger().info(f"Updated Glific ID for student {student.name}: {glific_contact['id']}")
        else:
            frappe.logger().warning(f"No Glific contact found for student {student.name} with phone {formatted_phone}")

    frappe.db.commit()
    return len(students)



def check_glific_group_exists(group_label):
    """Check if a group with the given label already exists in Glific"""
    settings = get_glific_settings()
    url = f"{settings.api_url}/api"
    headers = get_glific_auth_headers()

    payload = {
        "query": """
        query groups($filter: GroupFilter, $opts: Opts) {
          groups(filter: $filter, opts: $opts) {
            id
            label
          }
        }
        """,
        "variables": {
            "filter": {
                "label": group_label
            },
            "opts": {}
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            frappe.logger().error(f"Glific API Error in checking group: {data['errors']}")
            return None

        groups = data.get("data", {}).get("groups", [])
        if groups:
            return groups[0]  # Return the first matching group
        return None
    except Exception as e:
        frappe.logger().error(f"Error checking Glific group: {str(e)}")
        return None

def create_glific_group(label, description=""):
    """Create a new group in Glific"""
    settings = get_glific_settings()
    url = f"{settings.api_url}/api"
    headers = get_glific_auth_headers()

    payload = {
        "query": """
        mutation createGroup($input: GroupInput!) {
          createGroup(input: $input) {
            group {
              id
              label
              description
            }
            errors {
              key
              message
            }
          }
        }
        """,
        "variables": {
            "input": {
                "label": label,
                "description": description
            }
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            frappe.logger().error(f"Glific API Error in creating group: {data['errors']}")
            return None

        if "data" in data and "createGroup" in data["data"]:
            if "errors" in data["data"]["createGroup"] and data["data"]["createGroup"]["errors"]:
                errors = data["data"]["createGroup"]["errors"]
                frappe.logger().error(f"Glific API Error in creating group: {errors}")
                return None

            if "group" in data["data"]["createGroup"]:
                return data["data"]["createGroup"]["group"]

        frappe.logger().error(f"Unexpected response structure: {data}")
        return None
    except Exception as e:
        frappe.logger().error(f"Error creating Glific group: {str(e)}")
        return None

def create_or_get_glific_group_for_batch(set_id):
    """Create a Glific group for a backend onboarding batch or get existing one"""
    # Get the batch document
    set = frappe.get_doc("Backend Student Onboarding", set_id)

    # Check if we already have a mapping for this batch
    existing_mapping = frappe.get_all("GlificContactGroup",
                                   filters={"backend_onboarding_set": set_id},
                                   fields=["name", "group_id", "label"])

    if existing_mapping:
        return existing_mapping[0]

    # Derive group label from batch name
    group_label = f"Set: {set.set_name}"

    # Check if this group already exists in Glific
    existing_group = check_glific_group_exists(group_label)

    if existing_group:
        # Group exists, create mapping
        glific_group = frappe.new_doc("GlificContactGroup")
        glific_group.group_id = existing_group["id"]
        glific_group.label = existing_group["label"]
        glific_group.description = f"Auto-created for backend onboarding batch {set.set_name}"
        glific_group.backend_onboarding_set = set_id
        glific_group.insert()
        return {
            "group_id": existing_group["id"],
            "label": existing_group["label"]
        }

    # Group doesn't exist, create it in Glific
    new_group = create_glific_group(group_label, f"Students from batch {set.set_name}")

    if new_group:
        # Create mapping
        glific_group = frappe.new_doc("GlificContactGroup")
        glific_group.group_id = new_group["id"]
        glific_group.label = new_group["label"]
        glific_group.description = f"Auto-created for backend onboarding batch {set.set_name}"
        glific_group.backend_onboarding_set = set_id
        glific_group.insert()
        return {
            "group_id": new_group["id"],
            "label": new_group["label"]
        }

    # Failed to create group
    return None

def add_contact_to_group(contact_id, group_id):
    """Add a single contact to a single group"""
    if not contact_id or not group_id:
        return False

    settings = get_glific_settings()
    url = f"{settings.api_url}/api"
    headers = get_glific_auth_headers()

    payload = {
        "query": """
        mutation updateGroupContacts($input: GroupContactsInput!) {
          updateGroupContacts(input: $input) {
            groupContacts {
              id
            }
            numberDeleted
          }
        }
        """,
        "variables": {
            "input": {
                "groupId": group_id,
                "addContactIds": [contact_id],
                "deleteContactIds": []
            }
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            frappe.logger().error(f"Glific API Error adding contact to group: {data['errors']}")
            return False

        if "data" in data and "updateGroupContacts" in data["data"]:
            if "errors" in data["data"]["updateGroupContacts"] and data["data"]["updateGroupContacts"]["errors"]:
                errors = data["data"]["updateGroupContacts"]["errors"]
                frappe.logger().error(f"Glific API Error adding contact to group: {errors}")
                return False

            return True

        return False
    except Exception as e:
        frappe.logger().error(f"Error adding contact to group: {str(e)}")
        return False



def add_student_to_glific_for_onboarding(student_name, phone, school_name, batch_id, group_id, language_id=None, course_level_name=None, course_vertical_name=None, grade=None):
    """
    Function dedicated to backend onboarding that:
    1. Formats the phone number correctly
    2. Checks if contact already exists in Glific
    3. Creates contact if needed or just adds to group if exists
    4. ADDED: Opts in the contact for WhatsApp messaging
    
    Args:
        student_name: Name of the student
        phone: Phone number
        school_name: Name of the school
        batch_name: Name of the batch
        group_id: Glific group ID to add contact to
        language_id: Glific language ID from TAP Language
        course_level_name: Course level name for Glific
        course_vertical_name: Course vertical name for Glific
        grade: Student grade for Glific
        
    Returns:
        Contact information if successful, None otherwise
    """
    settings = get_glific_settings()

    # Format phone number
    phone = phone.strip().replace(' ', '')
    if len(phone) == 10:
        phone = f"91{phone}"
    elif len(phone) == 12 and phone.startswith('91'):
        pass  # Phone is already properly formatted
    else:
        frappe.logger().warning(f"Invalid phone number format: {phone}")
        return None

    # Check if contact already exists
    existing_contact = get_contact_by_phone(phone)

    if existing_contact and 'id' in existing_contact:
        frappe.logger().info(f"Contact already exists in Glific. Using existing contact: {existing_contact['id']}")
        
        # ADDED: Check if contact is opted in and opt-in if needed
        bsp_status = existing_contact.get('bspStatus', 'NONE')
        if bsp_status not in ['SESSION', 'SESSION_AND_HSM']:
            frappe.logger().info(f"Existing contact not opted in. Attempting opt-in...")
            try:
                optin_result = optin_contact(phone, student_name)
                if optin_result:
                    frappe.logger().info(f"Successfully opted in contact: {phone}")
                else:
                    frappe.logger().warning(f"Failed to opt-in contact: {phone}")
            except Exception as e:
                frappe.logger().warning(f"Error during opt-in: {str(e)}")
                # Continue even if opt-in fails

        # Add to group
        if group_id:
            add_contact_to_group(existing_contact['id'], group_id)

        # Optionally update fields to ensure they're current
        fields_to_update = {
            "buddy_name": student_name,
            "batch_id": batch_id
        }
        if school_name:
            fields_to_update["school"] = school_name
        if course_level_name:
            fields_to_update["course_level"] = course_level_name
        if course_vertical_name:
            fields_to_update["course"] = course_vertical_name
        if grade:
            fields_to_update["grade"] = grade

        update_contact_fields(existing_contact['id'], fields_to_update)

        return existing_contact
    else:
        # Get language_id from the parameter or use default if not provided
        if language_id is None or language_id == "":
            # Try to get default language ID from Glific Settings
            try:
                language_id = frappe.db.get_single_value("Glific Settings", "default_language_id")
            except Exception as e:
                frappe.logger().warning(f"Error getting default_language_id: {str(e)}")
                language_id = "1"  # Default to English if not found
        
        # Ensure language_id is an integer
        try:
            language_id = int(language_id)
        except (ValueError, TypeError):
            frappe.logger().warning(f"Invalid language_id format: {language_id}, using default (1)")
            language_id = 1  # Default to English if not a valid integer
        
        frappe.logger().info(f"Creating Glific contact with language_id: {language_id}")

        # Create new contact with minimal required fields
        contact_data = {
            "query": """
            mutation createContact($input:ContactInput!) {
                createContact(input: $input) {
                    contact { id name phone }
                    errors { key message }
                }
            }
            """,
            "variables": {
                "input": {
                    "name": student_name,
                    "phone": phone,
                    "languageId": language_id
                }
            }
        }

        # Add fields if available
        fields = {}
        # Always add buddy_name
        fields["buddy_name"] = {
            "value": student_name,
            "type": "string",
            "inserted_at": datetime.now(timezone.utc).isoformat()
        }
        
        if school_name:
            fields["school"] = {
                "value": school_name,
                "type": "string",
                "inserted_at": datetime.now(timezone.utc).isoformat()
            }

        if batch_id:
            fields["batch_id"] = {
                "value": batch_id,
                "type": "string",
                "inserted_at": datetime.now(timezone.utc).isoformat()
            }

        if course_level_name:
            fields["course_level"] = {
                "value": course_level_name,
                "type": "string",
                "label": "course_level",
                "inserted_at": datetime.now(timezone.utc).isoformat()
            }

        if course_vertical_name:
            fields["course"] = {
                "value": course_vertical_name,
                "type": "string",
                "inserted_at": datetime.now(timezone.utc).isoformat()
            }

        if grade:
            fields["grade"] = {
                "value": grade,
                "type": "string",
                "inserted_at": datetime.now(timezone.utc).isoformat()
            }

        if fields:
            contact_data["variables"]["input"]["fields"] = json.dumps(fields)

        # Execute request
        try:
            response = requests.post(
                f"{settings.api_url}/api",
                json=contact_data,
                headers=get_glific_auth_headers()
            )

            if response.status_code != 200:
                frappe.logger().error(f"Failed to create contact. Status: {response.status_code}, Response: {response.text}")
                return None

            result = response.json()

            if "errors" in result:
                frappe.logger().error(f"GraphQL errors: {result['errors']}")
                return None

            contact = result.get("data", {}).get("createContact", {}).get("contact")

            if not contact:
                frappe.logger().error(f"No contact in response: {result}")
                return None

            # ADDED: Opt-in the newly created contact
            frappe.logger().info(f"Contact created. Now attempting opt-in...")
            try:
                optin_result = optin_contact(phone, student_name)
                if optin_result:
                    frappe.logger().info(f"Successfully opted in new contact: {phone}")
                else:
                    frappe.logger().warning(f"Failed to opt-in new contact: {phone}")
            except Exception as e:
                frappe.logger().warning(f"Error during opt-in for new contact: {str(e)}")
                # Continue even if opt-in fails

            # Add to group
            if group_id and 'id' in contact:
                add_contact_to_group(contact['id'], group_id)

            return contact

        except Exception as e:
            frappe.logger().error(f"Exception in add_student_to_glific_for_onboarding: {str(e)}", exc_info=True)
            return None




def create_or_get_teacher_group_for_batch(batch_name, batch_id):
    """
    Create a Glific group for teachers in a batch or get existing one

    Args:
        batch_name: The Batch document name (link field)
        batch_id: The batch_id field value from the Batch document
    """

    # Handle edge case for no active batch
    if not batch_id or batch_id == "no_active_batch_id" or not batch_name:
        frappe.logger().warning(f"Invalid batch for teacher group: batch_name={batch_name}, batch_id={batch_id}")
        return None

    # Check if we already have a mapping for this batch document
    existing_mapping = frappe.get_all("Glific Teacher Group",
                                   filters={"batch": batch_name},
                                   fields=["name", "glific_group_id", "group_label"])

    if existing_mapping:
        frappe.logger().info(f"Found existing teacher group mapping for batch {batch_name}")
        return {
            "group_id": existing_mapping[0]["glific_group_id"],
            "label": existing_mapping[0]["group_label"]
        }

    # Derive group label from batch_id
    group_label = f"teacher_batch_{batch_id}"

    # Check if this group already exists in Glific
    existing_group = check_glific_group_exists(group_label)

    if existing_group:
        # Group exists in Glific, create mapping
        frappe.logger().info(f"Found existing Glific group: {existing_group}")

        teacher_group = frappe.new_doc("Glific Teacher Group")
        teacher_group.batch = batch_name
        teacher_group.batch_id = batch_id
        teacher_group.glific_group_id = existing_group["id"]
        teacher_group.group_label = existing_group["label"]
        teacher_group.description = f"Teachers from batch {batch_id}"
        teacher_group.created_date = frappe.utils.now_datetime()
        teacher_group.insert(ignore_permissions=True)
        frappe.db.commit()

        return {
            "group_id": existing_group["id"],
            "label": existing_group["label"]
        }

    # Group doesn't exist, create it in Glific
    frappe.logger().info(f"Creating new Glific group for teacher batch {batch_id}")
    new_group = create_glific_group(group_label, f"Teachers from batch {batch_id}")

    if new_group:
        # Create mapping
        teacher_group = frappe.new_doc("Glific Teacher Group")
        teacher_group.batch = batch_name
        teacher_group.batch_id = batch_id
        teacher_group.glific_group_id = new_group["id"]
        teacher_group.group_label = new_group["label"]
        teacher_group.description = f"Teachers from batch {batch_id}"
        teacher_group.created_date = frappe.utils.now_datetime()
        teacher_group.insert(ignore_permissions=True)
        frappe.db.commit()

        return {
            "group_id": new_group["id"],
            "label": new_group["label"]
        }

    # Failed to create group
    frappe.logger().error(f"Failed to create Glific group for batch {batch_id}")
    return None
