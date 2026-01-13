import os
import json
import base64
import requests
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("SCHWAB_APP_KEY")
client_secret = os.getenv("SCHWAB_APP_SECRET")
token_path = os.path.expanduser("~/.schwab/tokens.json")

print(f"Client ID: {client_id[:5]}...")
print(f"Token Path: {token_path}")

if not os.path.exists(token_path):
    print("❌ Token file not found.")
    exit(1)

with open(token_path, "r") as f:
    tokens = json.load(f)

refresh_token = tokens.get("refresh_token")
print(f"Refresh Token: {refresh_token[:10]}...")

# Attempt refresh
credentials = f"{client_id}:{client_secret}"
base64_credentials = base64.b64encode(credentials.encode("utf-8")).decode("utf-8")
headers = {
    "Authorization": f"Basic {base64_credentials}",
    "Content-Type": "application/x-www-form-urlencoded"
}
data = {
    "grant_type": "refresh_token",
    "refresh_token": refresh_token
}

print("Attempting refresh...")
response = requests.post("https://api.schwabapi.com/v1/oauth/token", headers=headers, data=data)
print(f"Status Code: {response.status_code}")
print(f"Response: {response.text}")
