import frappe
import random
import string

def generate_unique_batch_keyword(doc):
    school = frappe.get_doc("School", doc.school)
    batch = frappe.get_doc("Batch", doc.batch)
    
    school_part = school.name1[:2].upper()
    batch_part = batch.name1[:2].upper()
    random_number = f"{random.randint(10, 99):02d}"
    random_letters = ''.join(random.choices(string.ascii_uppercase, k=2))
    
    keyword = f"{school_part}{batch_part}{random_number}{random_letters}"
    
    # Check if the generated keyword already exists
    while frappe.db.exists("Batch onboarding", {"batch_skeyword": keyword}):
        random_number = f"{random.randint(10, 99):02d}"
        random_letters = ''.join(random.choices(string.ascii_uppercase, k=2))
        keyword = f"{school_part}{batch_part}{random_number}{random_letters}"
    
    return keyword
