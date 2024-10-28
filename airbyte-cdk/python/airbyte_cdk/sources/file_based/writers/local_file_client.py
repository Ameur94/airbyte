# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

import logging
import os
import time

import psutil

AIRBYTE_STAGING_DIRECTORY = os.getenv("AIRBYTE_STAGING_DIRECTORY", "/staging/files")
DEFAULT_LOCAL_DIRECTORY = "/tmp/airbyte-file-transfer"


class LocalFileTransferClient:
    def __init__(self):
        """
        Initialize the LocalFileTransferClient. It uses a default local directory for file saving.
        """
        self._local_directory = AIRBYTE_STAGING_DIRECTORY if os.path.exists(AIRBYTE_STAGING_DIRECTORY) else DEFAULT_LOCAL_DIRECTORY

    def write(self, file_uri: str, fp, file_size: int, logger: logging.Logger):
        """
        Write the file to a local directory.
        """
        # Remove left slashes from source path format to make relative path for writing locally
        file_relative_path = file_uri.lstrip("/")
        local_file_path = os.path.join(self._local_directory, file_relative_path)

        # Ensure the local directory exists
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

        # Get the absolute path
        absolute_file_path = os.path.abspath(local_file_path)

        # Get available disk space
        disk_usage = psutil.disk_usage("/")
        available_disk_space = disk_usage.free

        # Get available memory
        memory_info = psutil.virtual_memory()
        available_memory = memory_info.available

        # logger.info(f"Writing file to {local_file_path}.")
        # Log file size, available disk space, and memory
        logger.info(
            f"Writing file to '{local_file_path}' "
            f"with size: {file_size / (1024 * 1024):,.2f} MB ({file_size / (1024 * 1024 * 1024):.2f} GB), "
            f"available disk space: {available_disk_space / (1024 * 1024):,.2f} MB ({available_disk_space / (1024 * 1024 * 1024):.2f} GB),"
            f"available memory: {available_memory / (1024 * 1024):,.2f} MB ({available_memory / (1024 * 1024 * 1024):.2f} GB)."
        )

        with open(local_file_path, "wb") as f:
            # Measure the time for reading
            logger.info("Starting to read the file")
            start_read_time = time.time()
            file_content = fp.read()  # Read the file content
            read_duration = time.time() - start_read_time
            logger.info(f"Time taken to read the file: {read_duration:,.2f} seconds.")

            # Measure the time for writing
            logger.info("Starting to write the file locally")
            start_write_time = time.time()
            f.write(file_content)
            write_duration = time.time() - start_write_time
            logger.info(f"Time taken to write the file: {write_duration:,.2f} seconds.")

        logger.info(f"File {file_relative_path} successfully written to {self._local_directory}.")

        return {"file_url": absolute_file_path, "bytes": file_size, "file_relative_path": file_relative_path}
