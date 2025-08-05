"""Earth Engine utilities."""

import logging

import ee

# geemap is not used yet
# import geemap

logger = logging.getLogger(__name__)

import ee
import json
from pathlib import Path
from typing import Optional
import logging

logger = logging.getLogger(__name__)

import ee
import json
from pathlib import Path
from typing import Optional
import logging
import os

logger = logging.getLogger(__name__)


def init_ee_from_credentials(
        credentials_path: Path = Path("credentials.json"),
        project: Optional[str] = None,
        use_highvolume: bool = True
) -> None:
    """Initialize Earth Engine using local credentials file. Authenticate if necessary.

    Args:
        credentials_path: Path to local credentials JSON file
        project: The project name (optional)
        use_highvolume: Whether to use the high volume server

    Raises:
        FileNotFoundError: If credentials file doesn't exist
        ValueError: If credentials file is invalid
    """
    # Set the high volume endpoint if requested
    opt_url = "https://earthengine-highvolume.googleapis.com" if use_highvolume else None

    # Load credentials from file if it exists
    if credentials_path.exists():
        try:
            with open(credentials_path) as f:
                credentials = json.load(f)

            # Set environment variable to point to our credentials
            os.environ['EARTHENGINE_TOKEN'] = json.dumps(credentials)
            logger.debug(f"Using credentials from {credentials_path}")
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid credentials file: {e}")
    else:
        logger.debug(f"No credentials found at {credentials_path}")

    try:
        logger.debug(f"Initializing Earth Engine with project {project} {'with high volume' if use_highvolume else ''}")
        ee.Initialize(project=project, opt_url=opt_url)
    except (ee.EEException, Exception) as e:
        logger.debug(f"Initialization failed: {str(e)}. Attempting authentication...")
        # Remove any existing token environment variable for clean authentication
        if 'EARTHENGINE_TOKEN' in os.environ:
            del os.environ['EARTHENGINE_TOKEN']

        # Perform authentication
        ee.Authenticate(auth_mode="notebook")

        # Save the new credentials
        credentials_path.parent.mkdir(parents=True, exist_ok=True)
        with open(credentials_path, 'w') as f:
            json.dump(ee.data.get_persistent_credentials(), f, indent=2)

        # Reinitialize with new credentials
        ee.Initialize(project=project, opt_url=opt_url)

    logger.debug("Earth Engine initialized successfully")

def init_ee_from_credentials_2(
        credentials_path: Path = Path("credentials.json"),
        project: Optional[str] = None,
        use_highvolume: bool = True
) -> None:
    """Initialize Earth Engine using local credentials file. Authenticate if necessary.

    Args:
        credentials_path: Path to local credentials JSON file
        project: The project name (optional)
        use_highvolume: Whether to use the high volume server

    Raises:
        FileNotFoundError: If credentials file doesn't exist
        ValueError: If credentials file is invalid
    """
    # Set the high volume endpoint if requested
    opt_url = "https://earthengine-highvolume.googleapis.com" if use_highvolume else None

    # Load credentials from file if it exists
    if credentials_path.exists():
        try:
            with open(credentials_path) as f:
                credentials = json.load(f)

            # Configure EE with these credentials
            ee.data.setAuthToken(
                token_type=credentials.get('token_type', 'Bearer'),
                access_token=credentials.get('access_token', ''),
                expires_in=credentials.get('expires_in', 3600),
                refresh_token=credentials.get('refresh_token', '')
            )

            logger.debug(f"Using credentials from {credentials_path}")
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"Invalid credentials file: {e}")
    else:
        logger.debug(f"No credentials found at {credentials_path}")

    try:
        logger.debug(f"Initializing Earth Engine with project {project} {'with high volume' if use_highvolume else ''}")
        ee.Initialize(project=project, opt_url=opt_url)
    except ee.EEException as e:
        logger.debug(f"Initialization failed: {str(e)}. Attempting authentication...")
        ee.Authenticate(auth_mode="notebook")  # This will create new credentials
        if credentials_path.exists():
            logger.warning(f"Overwriting existing credentials at {credentials_path}")
        # Save the new credentials
        with open(credentials_path, 'w') as f:
            json.dump(ee.data.get_persistent_credentials(), f)
        # Reinitialize with new credentials
        ee.Initialize(project=project, opt_url=opt_url)

    logger.debug("Earth Engine initialized successfully")



def init_ee(project: str | None = None, use_highvolume: bool = True) -> None:
    """Initialize Earth Engine. Authenticate if necessary.

    Args:
        project (str): The project name.
        use_highvolume (bool): Whether to use the high volume server (https://earthengine-highvolume.googleapis.com).

    """
    logger.debug(f"Initializing Earth Engine with project {project} {'with high volume' if use_highvolume else ''}")
    opt_url = "https://earthengine-highvolume.googleapis.com" if use_highvolume else None
    try:
        ee.Initialize(project=project, opt_url=opt_url)
        # geemap.ee_initialize(project=project, opt_url="https://earthengine-highvolume.googleapis.com")
    except Exception:
        logger.debug("Initializing Earth Engine failed, trying to authenticate before")
        ee.Authenticate()
        ee.Initialize(project=project, opt_url=opt_url)
        # geemap.ee_initialize(project=project, opt_url="https://earthengine-highvolume.googleapis.com")
    logger.debug("Earth Engine initialized")
