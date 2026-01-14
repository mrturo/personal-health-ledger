# Credentials directory

This directory should contain your Google API credentials:

- `credentials.json` - OAuth2 client credentials (for personal use)
- `service_account.json` - Service account key (for automation)
- `token.json` - OAuth2 access token (auto-generated on first use)

**IMPORTANT**: Never commit credential files to version control!

## How to obtain credentials

See the main README.md file for detailed instructions on:

1. Creating a Google Cloud Project
2. Enabling the Google Drive API
3. Creating OAuth2 credentials or Service Account
4. Downloading and saving credential files

## File descriptions

### credentials.json (OAuth2)
Downloaded from Google Cloud Console when creating an OAuth2 Desktop application client ID.

### service_account.json (Service Account)
Downloaded from Google Cloud Console when creating a service account and generating a JSON key.

### token.json (Auto-generated)
Created automatically on first OAuth2 authentication. Contains access and refresh tokens.
Delete this file to re-authenticate.
