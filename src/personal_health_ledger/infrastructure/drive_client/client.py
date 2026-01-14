"""
Google Drive client implementation.

Provides OAuth2 and Service Account authentication, file listing,
downloading, and metadata tracking with MD5 checksum validation.
"""

import json
import logging
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

from personal_health_ledger.utils.exceptions import AuthenticationError, DriveClientError
from personal_health_ledger.utils.hashing import compute_file_hash
from personal_health_ledger.utils.parameters import DriveConfig

logger = logging.getLogger(__name__)


class DriveFileMetadata:
    """Metadata for a Google Drive file."""

    def __init__(
        self,
        drive_file_id: str,
        name: str,
        mime_type: str,
        modified_time: str,
        md5_checksum: str | None = None,
    ) -> None:
        """
        Initialize Drive file metadata.

        Args:
            drive_file_id: Google Drive file ID.
            name: File name.
            mime_type: MIME type.
            modified_time: Last modified time (ISO format).
            md5_checksum: MD5 checksum (if available).
        """
        self.drive_file_id = drive_file_id
        self.name = name
        self.mime_type = mime_type
        self.modified_time = modified_time
        self.md5_checksum = md5_checksum

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "drive_file_id": self.drive_file_id,
            "name": self.name,
            "mime_type": self.mime_type,
            "modified_time": self.modified_time,
            "md5_checksum": self.md5_checksum,
        }


class DriveClient:
    """
    Google Drive client with authentication and file operations.

    Supports OAuth2 and Service Account authentication.
    Maintains local index for checksum-based download optimization.
    """

    def __init__(self, config: DriveConfig) -> None:
        """
        Initialize Drive client.

        Args:
            config: Drive configuration.

        Raises:
            AuthenticationError: If authentication fails.
        """
        self.config = config
        self.service: Any = None
        self.cache_dir = Path(config.cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.index_file = Path(config.index_file)
        self.index: dict[str, dict[str, Any]] = {}

        self._load_index()
        self._authenticate()

    def _authenticate(self) -> None:
        """
        Authenticate with Google Drive API.

        Raises:
            AuthenticationError: If authentication fails.
        """
        try:
            if self.config.auth_method == "oauth2":
                creds: Credentials | ServiceAccountCredentials = self._authenticate_oauth2()
            elif self.config.auth_method == "service_account":
                creds = self._authenticate_service_account()
            else:
                raise AuthenticationError(f"Unknown auth method: {self.config.auth_method}")

            self.service = build("drive", "v3", credentials=creds)
            logger.info(f"Authenticated with Google Drive using {self.config.auth_method}")

        except Exception as e:
            raise AuthenticationError(f"Authentication failed: {e}") from e

    def _authenticate_oauth2(self) -> Credentials:
        """
        Authenticate using OAuth2 installed app flow.

        Returns:
            Valid credentials.
        """
        creds: Credentials | None = None
        token_path = Path(self.config.oauth2.token_path)

        if token_path.exists():
            creds = Credentials.from_authorized_user_file(
                str(token_path), self.config.oauth2.scopes
            )

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.config.oauth2.credentials_path, self.config.oauth2.scopes
                )
                creds = flow.run_local_server(port=0)

            token_path.parent.mkdir(parents=True, exist_ok=True)
            with open(token_path, "w") as token:
                token.write(creds.to_json())

        return creds

    def _authenticate_service_account(self) -> ServiceAccountCredentials:
        """
        Authenticate using Service Account.

        Returns:
            Service account credentials.
        """
        creds = ServiceAccountCredentials.from_service_account_file(
            self.config.service_account.credentials_path,
            scopes=self.config.service_account.scopes,
        )
        return creds  # type: ignore[no-any-return]

    def _load_index(self) -> None:
        """Load local file index from disk."""
        if self.index_file.exists():
            try:
                with open(self.index_file, encoding="utf-8") as f:
                    self.index = json.load(f)
                logger.info(f"Loaded index with {len(self.index)} entries")
            except Exception as e:
                logger.warning(f"Failed to load index: {e}")
                self.index = {}
        else:
            self.index = {}

    def _save_index(self) -> None:
        """Save local file index to disk."""
        try:
            self.index_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.index_file, "w", encoding="utf-8") as f:
                json.dump(self.index, f, indent=2)
            logger.debug(f"Saved index with {len(self.index)} entries")
        except Exception as e:
            logger.error(f"Failed to save index: {e}")

    def find_folder(self, folder_name: str) -> str | None:
        """
        Find folder ID by name.

        Args:
            folder_name: Name of the folder to find.

        Returns:
            Folder ID if found, None otherwise.
        """
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
            results = self.service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get("files", [])

            if files:
                folder_id: str = files[0]["id"]
                logger.info(f"Found folder '{folder_name}' with ID: {folder_id}")
                return folder_id
            else:
                logger.warning(f"Folder '{folder_name}' not found")
                return None

        except Exception as e:
            logger.error(f"Error finding folder: {e}")
            return None

    def list_files(self, folder_id: str | None = None) -> list[DriveFileMetadata]:
        """
        List files in a folder.

        Args:
            folder_id: Folder ID to list files from. If None, uses config.

        Returns:
            List of file metadata objects.

        Raises:
            DriveClientError: If listing fails.
        """
        if folder_id is None:
            if self.config.folder_id:
                folder_id = self.config.folder_id
            elif self.config.folder_name:
                folder_id = self.find_folder(self.config.folder_name)
                if not folder_id:
                    raise DriveClientError(f"Folder not found: {self.config.folder_name}")
            else:
                raise DriveClientError("No folder_id or folder_name configured")

        try:
            query = f"'{folder_id}' in parents and trashed=false"
            fields = "files(id, name, mimeType, modifiedTime, md5Checksum)"

            files_metadata: list[DriveFileMetadata] = []
            page_token = None

            while True:
                results = (
                    self.service.files()
                    .list(q=query, fields=fields, pageToken=page_token, pageSize=100)
                    .execute()
                )

                for file_data in results.get("files", []):
                    if "application/vnd.google-apps" not in file_data.get("mimeType", ""):
                        metadata = DriveFileMetadata(
                            drive_file_id=file_data["id"],
                            name=file_data["name"],
                            mime_type=file_data["mimeType"],
                            modified_time=file_data["modifiedTime"],
                            md5_checksum=file_data.get("md5Checksum"),
                        )
                        files_metadata.append(metadata)

                page_token = results.get("nextPageToken")
                if not page_token:
                    break

            logger.info(f"Listed {len(files_metadata)} files from folder {folder_id}")
            return files_metadata

        except Exception as e:
            raise DriveClientError(f"Failed to list files: {e}") from e

    def download_file(
        self, file_metadata: DriveFileMetadata, force: bool = False
    ) -> Path:
        """
        Download file from Drive with checksum-based optimization.

        Args:
            file_metadata: Metadata of the file to download.
            force: Force download even if file exists with matching checksum.

        Returns:
            Path to the downloaded file.

        Raises:
            DriveClientError: If download fails.
        """
        local_path = self.cache_dir / file_metadata.name

        if not force and file_metadata.drive_file_id in self.index:
            cached_info = self.index[file_metadata.drive_file_id]
            cached_path = Path(cached_info["local_path"])

            if cached_path.exists():
                if file_metadata.md5_checksum:
                    local_md5 = compute_file_hash(str(cached_path), "md5")
                    if local_md5 == file_metadata.md5_checksum:
                        logger.info(f"Skipping download (checksum match): {file_metadata.name}")
                        return cached_path

        try:
            request = self.service.files().get_media(fileId=file_metadata.drive_file_id)

            with open(local_path, "wb") as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logger.debug(f"Download progress: {int(status.progress() * 100)}%")

            self.index[file_metadata.drive_file_id] = {
                "local_path": str(local_path),
                "name": file_metadata.name,
                "md5_checksum": file_metadata.md5_checksum,
                "modified_time": file_metadata.modified_time,
            }
            self._save_index()

            logger.info(f"Downloaded: {file_metadata.name}")
            return local_path

        except Exception as e:
            raise DriveClientError(f"Failed to download file {file_metadata.name}: {e}") from e

    def sync_folder(self, force: bool = False) -> list[Path]:
        """
        Sync entire folder from Drive.

        Args:
            force: Force re-download of all files.

        Returns:
            List of local file paths.
        """
        files_metadata = self.list_files()
        local_paths: list[Path] = []

        for file_metadata in files_metadata:
            try:
                local_path = self.download_file(file_metadata, force=force)
                local_paths.append(local_path)
            except DriveClientError as e:
                logger.error(f"Failed to download {file_metadata.name}: {e}")

        logger.info(f"Synced {len(local_paths)} files")
        return local_paths
