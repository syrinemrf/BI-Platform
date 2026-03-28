"""
File handlers for various data formats.
"""
import pandas as pd
import json
import os
from typing import Dict, Any, Optional, Union
from pathlib import Path
import aiofiles
import httpx
from io import BytesIO, StringIO

from config import settings


class FileHandler:
    """Base class for file handling operations."""

    SUPPORTED_EXTENSIONS = {
        '.csv': 'csv',
        '.xlsx': 'excel',
        '.xls': 'excel',
        '.json': 'json'
    }

    @classmethod
    def get_file_type(cls, filename: str) -> Optional[str]:
        """Detect file type from extension."""
        ext = Path(filename).suffix.lower()
        return cls.SUPPORTED_EXTENSIONS.get(ext)

    @classmethod
    def is_supported(cls, filename: str) -> bool:
        """Check if file extension is supported."""
        return cls.get_file_type(filename) is not None


class CSVHandler:
    """Handler for CSV files."""

    @staticmethod
    def read(file_path: str, **kwargs) -> pd.DataFrame:
        """Read CSV file into DataFrame."""
        default_params = {
            'encoding': 'utf-8',
            'low_memory': False
        }
        default_params.update(kwargs)

        # Try different encodings if utf-8 fails
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']

        for encoding in encodings:
            try:
                default_params['encoding'] = encoding
                return pd.read_csv(file_path, **default_params)
            except UnicodeDecodeError:
                continue

        raise ValueError(f"Could not read CSV with any supported encoding")

    @staticmethod
    def read_from_bytes(content: bytes, **kwargs) -> pd.DataFrame:
        """Read CSV from bytes content."""
        return pd.read_csv(BytesIO(content), **kwargs)

    @staticmethod
    def write(df: pd.DataFrame, file_path: str, **kwargs) -> None:
        """Write DataFrame to CSV."""
        default_params = {
            'index': False,
            'encoding': 'utf-8'
        }
        default_params.update(kwargs)
        df.to_csv(file_path, **default_params)


class ExcelHandler:
    """Handler for Excel files."""

    @staticmethod
    def read(file_path: str, sheet_name: Union[str, int] = 0, **kwargs) -> pd.DataFrame:
        """Read Excel file into DataFrame."""
        return pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)

    @staticmethod
    def read_from_bytes(content: bytes, sheet_name: Union[str, int] = 0, **kwargs) -> pd.DataFrame:
        """Read Excel from bytes content."""
        return pd.read_excel(BytesIO(content), sheet_name=sheet_name, **kwargs)

    @staticmethod
    def get_sheet_names(file_path: str) -> list:
        """Get list of sheet names in Excel file."""
        xl = pd.ExcelFile(file_path)
        return xl.sheet_names

    @staticmethod
    def read_all_sheets(file_path: str) -> Dict[str, pd.DataFrame]:
        """Read all sheets from Excel file."""
        return pd.read_excel(file_path, sheet_name=None)

    @staticmethod
    def write(df: pd.DataFrame, file_path: str, sheet_name: str = 'Sheet1', **kwargs) -> None:
        """Write DataFrame to Excel."""
        default_params = {
            'index': False
        }
        default_params.update(kwargs)
        df.to_excel(file_path, sheet_name=sheet_name, **default_params)


class JSONHandler:
    """Handler for JSON files."""

    @staticmethod
    def read(file_path: str, **kwargs) -> pd.DataFrame:
        """Read JSON file into DataFrame."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Handle different JSON structures
        if isinstance(data, list):
            return pd.DataFrame(data)
        elif isinstance(data, dict):
            # Check for common patterns
            if 'data' in data:
                return pd.DataFrame(data['data'])
            elif 'records' in data:
                return pd.DataFrame(data['records'])
            elif 'results' in data:
                return pd.DataFrame(data['results'])
            else:
                # Try to normalize nested structure
                return pd.json_normalize(data)

        raise ValueError("Unsupported JSON structure")

    @staticmethod
    def read_from_bytes(content: bytes, **kwargs) -> pd.DataFrame:
        """Read JSON from bytes content."""
        data = json.loads(content.decode('utf-8'))
        if isinstance(data, list):
            return pd.DataFrame(data)
        return pd.json_normalize(data)

    @staticmethod
    def write(df: pd.DataFrame, file_path: str, **kwargs) -> None:
        """Write DataFrame to JSON."""
        default_params = {
            'orient': 'records',
            'indent': 2
        }
        default_params.update(kwargs)
        df.to_json(file_path, **default_params)


class APIHandler:
    """Handler for API endpoints."""

    @staticmethod
    async def fetch(url: str, method: str = 'GET', headers: Dict = None, params: Dict = None, body: Dict = None) -> pd.DataFrame:
        """Fetch data from API endpoint."""
        async with httpx.AsyncClient(timeout=30.0) as client:
            if method.upper() == 'GET':
                response = await client.get(url, headers=headers, params=params)
            elif method.upper() == 'POST':
                response = await client.post(url, headers=headers, json=body)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                return pd.DataFrame(data)
            elif isinstance(data, dict):
                if 'data' in data:
                    return pd.DataFrame(data['data'])
                return pd.json_normalize(data)

            raise ValueError("Unsupported API response structure")


def load_file(file_path: str, file_type: str = None, **kwargs) -> pd.DataFrame:
    """
    Load file into pandas DataFrame.

    Args:
        file_path: Path to the file
        file_type: Optional file type override (csv, excel, json)
        **kwargs: Additional arguments for the reader

    Returns:
        pandas DataFrame
    """
    if file_type is None:
        file_type = FileHandler.get_file_type(file_path)

    if file_type == 'csv':
        return CSVHandler.read(file_path, **kwargs)
    elif file_type == 'excel':
        return ExcelHandler.read(file_path, **kwargs)
    elif file_type == 'json':
        return JSONHandler.read(file_path, **kwargs)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def load_from_bytes(content: bytes, file_type: str, **kwargs) -> pd.DataFrame:
    """
    Load file content (bytes) into pandas DataFrame.

    Args:
        content: File content as bytes
        file_type: File type (csv, excel, json)
        **kwargs: Additional arguments for the reader

    Returns:
        pandas DataFrame
    """
    if file_type == 'csv':
        return CSVHandler.read_from_bytes(content, **kwargs)
    elif file_type == 'excel':
        return ExcelHandler.read_from_bytes(content, **kwargs)
    elif file_type == 'json':
        return JSONHandler.read_from_bytes(content, **kwargs)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


async def save_upload_file(file, filename: str) -> str:
    """
    Save uploaded file to disk.

    Args:
        file: UploadFile object
        filename: Destination filename

    Returns:
        Full path to saved file
    """
    # Ensure upload directory exists
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)

    file_path = os.path.join(settings.UPLOAD_DIR, filename)

    # Read file content
    content = await file.read()

    # Write synchronously (more reliable on Windows)
    with open(file_path, 'wb') as f:
        f.write(content)

    return file_path


def get_file_info(file_path: str) -> Dict[str, Any]:
    """
    Get file metadata.

    Args:
        file_path: Path to the file

    Returns:
        Dictionary with file info
    """
    stat = os.stat(file_path)
    return {
        'path': file_path,
        'name': os.path.basename(file_path),
        'size': stat.st_size,
        'modified': stat.st_mtime,
        'extension': Path(file_path).suffix.lower()
    }
