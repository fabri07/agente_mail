import sys
import logging
logging.basicConfig(level=logging.ERROR)
sys.path.append('.')
from app.config import SCOPES, load_settings
from app.gmail_client import GmailClient
settings = load_settings()
gmail = GmailClient(settings.credentials_file, settings.token_file, SCOPES)
service = gmail.build_service()
profile = service.users().getProfile(userId='me').execute()
print(profile.get('messagesTotal'))
