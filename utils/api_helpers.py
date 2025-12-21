import os
import requests
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Get token from environment
TRADIER_TOKEN = os.getenv("TRADIER_TOKEN")

# Create API headers
HEADERS = {
    "Authorization": f"Bearer {TRADIER_TOKEN}",
    "Accept": "application/json"
}
