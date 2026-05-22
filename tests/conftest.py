import sys
import os

# Add the project root to sys.path so `from tap_ai.services.X import Y` works
# without a full Frappe bench environment.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
