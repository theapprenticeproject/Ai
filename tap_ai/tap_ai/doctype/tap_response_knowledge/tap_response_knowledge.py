# Copyright (c) 2026, Anish Aman and contributors
# For license information, please see license.txt

from frappe.model.document import Document


class TAPResponseKnowledge(Document):
	def validate(self):
		if self.student_query and not self.normalized_query:
			from tap_ai.services.kb.direct_response_bank import normalize_text

			self.normalized_query = normalize_text(self.student_query)
		if self.student_query and not self.title:
			self.title = self.student_query
