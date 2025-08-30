#!/usr/bin/env python3
"""
EPG XML Cacher - Hourly EPG data fetching and merging for DVR systems
Fetches EPG data from a URL, sanitizes UTF-8 content, and intelligently merges
missing data from previous versions with configurable time tolerance.
"""

import os
import sys
import time
import logging
import shutil
import codecs
import re
import gzip
import zipfile
import tempfile
import csv
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import requests
import schedule
from croniter import croniter
import urllib.parse
import unicodedata


class EPGCacher:
    def __init__(self):
        """Initialize the EPG Cacher with environment variables and logging."""
        # Environment variables
        self.epg_url = os.getenv("EPG_URL")
        if not self.epg_url:
            raise ValueError("EPG_URL environment variable is required")
        self.skip_cron = os.getenv("SKIP_CRON", "* * * * *")
        
        self.epg2_url = os.getenv("EPG2_URL")  # Optional second EPG URL for images
        self.time_tolerance_minutes = int(os.getenv("TIME_TOLERANCE_MINUTES", "10"))
        
        # File paths
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        self.epg_file_escaped = os.path.join(output_dir, "epg.xml")
        self.epg_file = os.path.join(output_dir, "epg_unescaped.xml")
        self.epg_old_file = os.path.join(output_dir, "epg_old.xml")
        self.channel_mapping_file = os.path.join(output_dir, "channel_mapping.csv")
        self.channels_epg1_file = os.path.join(output_dir, "channels_epg1.csv")
        self.channels_epg2_file = os.path.join(output_dir, "channels_epg2.csv")
        
        # Setup logging
        self.setup_logging()
        
        # HTTP session for reuse
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'EPG-Cacher/1.0',
            'Accept': 'application/xml, text/xml, */*'
        })
        
        # Create sample channel mapping file if none exists
        self.create_sample_channel_mapping()
        
        # Load channel mapping if available
        self.channel_mapping = self.load_channel_mapping()
        
        epg2_info = f", Image EPG: {self.epg2_url}" if self.epg2_url else ", No image EPG"
        mapping_info = f", Channel mappings: {len(self.channel_mapping)}" if self.channel_mapping else ""
        self.logger.info(f"EPG Cacher initialized - URL: {self.epg_url}{epg2_info}{mapping_info}, "
                        f"Time tolerance: {self.time_tolerance_minutes} minutes")

    def setup_logging(self):
        """Setup comprehensive logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('epg_cacher.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger('EPGCacher')

    def is_now_in_cron(self, cron_expr: str, grace_period: int = 60) -> bool:
        """
        Check if the current time matches a cron expression (within grace_period seconds).
        
        Args:
            cron_expr: A standard cron string (e.g., "0 9 * * *").
            grace_period: How close (in seconds) current time needs to be to match.
        
        Returns:
            True if current time is within cron schedule, False otherwise.
        """
        now = datetime.now()
        base = now - timedelta(seconds=grace_period)
        
        # Get previous scheduled run
        itr = croniter(cron_expr, base)
        prev_time = itr.get_next(datetime)

        return abs((prev_time - now).total_seconds()) <= grace_period

    def load_channel_mapping(self) -> Dict[str, str]:
        """
        Load channel mapping from CSV file.
        
        Returns:
            Dictionary mapping EPG1 channel IDs to EPG2 channel IDs
        """
        mapping = {}
        
        if not os.path.exists(self.channel_mapping_file):
            self.logger.info(f"No channel mapping file found at {self.channel_mapping_file}")
            return mapping
        
        try:
            with open(self.channel_mapping_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, None)  # Skip header if present
                
                for row in reader:
                    if len(row) >= 2:
                        epg1_channel = row[0].strip()
                        epg2_channel = row[1].strip()
                        if epg1_channel and epg2_channel:
                            mapping[epg1_channel] = epg2_channel
            
            self.logger.info(f"Loaded {len(mapping)} channel mappings from {self.channel_mapping_file}")
            
        except Exception as e:
            self.logger.error(f"Error loading channel mapping file: {e}")
        
        return mapping

    def save_epg1_channels_to_csv(self, epg_root: ET.Element):
        """
        Save EPG1 channel IDs to channels_epg1.csv.
        
        Args:
            epg_root: Parsed XML root from EPG1 source
        """
        try:
            # Extract channel IDs from EPG1
            channel_data = []
            
            # Get channels from channel elements
            for channel in epg_root.findall('.//channel'):
                channel_id = channel.get('id', '').strip()
                if channel_id:
                    # Try to get channel name from display-name element
                    channel_name = ""
                    display_name = channel.find('display-name')
                    if display_name is not None and display_name.text:
                        channel_name = display_name.text.strip()
                    
                    channel_data.append((channel_id, channel_name))
            
            # Also get unique channels from programme elements (in case channels are only in programmes)
            programme_channels = set()
            for programme in epg_root.findall('.//programme'):
                channel_id = programme.get('channel', '').strip()
                if channel_id:
                    programme_channels.add(channel_id)
            
            # Add programme channels that weren't in channel elements
            existing_channel_ids = {ch_id for ch_id, _ in channel_data}
            for channel_id in programme_channels:
                if channel_id not in existing_channel_ids:
                    channel_data.append((channel_id, ""))
            
            if not channel_data:
                self.logger.info("No channel IDs found in EPG1")
                return
            
            # Sort for consistent output
            channel_data.sort(key=lambda x: x[0])
            
            # Write to channels_epg1.csv
            with open(self.channels_epg1_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow(['EPG1_Channel_ID', 'Channel_Name'])
                
                # Write channel data
                for channel_id, channel_name in channel_data:
                    writer.writerow([channel_id, channel_name])
            
            self.logger.info(f"Saved {len(channel_data)} EPG1 channels to {self.channels_epg1_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving EPG1 channels to CSV: {e}")

    def save_epg2_channels_to_csv(self, epg2_root: ET.Element):
        """
        Save EPG2 channel IDs to channels_epg2.csv.
        
        Args:
            epg2_root: Parsed XML root from EPG2 source
        """
        try:
            # Extract channel data from EPG2
            channel_data = []
            
            # Get channels from channel elements
            channel_ids_seen = set()
            for channel in epg2_root.findall('.//channel'):
                channel_id = channel.get('id', '').strip()
                if channel_id and channel_id not in channel_ids_seen:
                    channel_ids_seen.add(channel_id)
                    
                    # Try to get channel name from display-name element
                    channel_name = ""
                    display_name = channel.find('display-name')
                    if display_name is not None and display_name.text:
                        channel_name = display_name.text.strip()
                    
                    channel_data.append((channel_id, channel_name))
            
            # Also get channels from programme elements (in case channels are only in programmes)
            for programme in epg2_root.findall('.//programme'):
                channel_id = programme.get('channel', '').strip()
                if channel_id and channel_id not in channel_ids_seen:
                    channel_ids_seen.add(channel_id)
                    channel_data.append((channel_id, ""))
            
            if not channel_data:
                self.logger.info("No channel IDs found in EPG2")
                return
            
            # Sort for consistent output
            channel_data.sort(key=lambda x: x[0])
            
            # Write to channels_epg2.csv
            with open(self.channels_epg2_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow(['EPG2_Channel_ID', 'Channel_Name'])
                
                # Write channel data
                for channel_id, channel_name in channel_data:
                    writer.writerow([channel_id, channel_name])
            
            self.logger.info(f"Saved {len(channel_data)} EPG2 channels to {self.channels_epg2_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving EPG2 channels to CSV: {e}")

    def create_sample_channel_mapping(self):
        """
        Create an empty channel mapping CSV file with just the header row.
        """
        if os.path.exists(self.channel_mapping_file):
            return  # Don't overwrite existing file
        
        try:
            with open(self.channel_mapping_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['EPG1_Channel_ID', 'EPG2_Channel_ID'])
            
            self.logger.info(f"Created empty channel mapping file: {self.channel_mapping_file}")
            
        except Exception as e:
            self.logger.error(f"Error creating channel mapping file: {e}")

    def sanitize_utf8(self, text: str) -> str:
        """
        Sanitize text to ensure proper UTF-8 encoding and remove invalid characters.
        
        Args:
            text: Raw text to sanitize
            
        Returns:
            Sanitized UTF-8 text
        """
        if not text:
            return ""
        
        # Remove or replace invalid XML characters
        # XML 1.0 valid characters: #x9 | #xA | #xD | [#x20-#xD7FF] | [#xE000-#xFFFD] | [#x10000-#x10FFFF]
        valid_chars = []
        for char in text:
            code = ord(char)
            if (code == 0x09 or code == 0x0A or code == 0x0D or 
                (0x20 <= code <= 0xD7FF) or 
                (0xE000 <= code <= 0xFFFD) or 
                (0x10000 <= code <= 0x10FFFF)):
                valid_chars.append(char)
            else:
                # Replace invalid characters with space
                valid_chars.append(' ')
        
        sanitized = ''.join(valid_chars)
        
        # Ensure proper UTF-8 encoding
        try:
            # Encode to bytes and decode back to ensure valid UTF-8
            sanitized = sanitized.encode('utf-8', errors='replace').decode('utf-8')
        except UnicodeError:
            self.logger.warning("UTF-8 encoding error during sanitization")
            sanitized = sanitized.encode('utf-8', errors='ignore').decode('utf-8')
        
        return sanitized

    def fetch_epg_data(self) -> Optional[str]:
        """
        Fetch EPG data from the configured URL.
        
        Returns:
            Raw EPG XML data as string, None if fetch failed
        """
        try:
            self.logger.info(f"Fetching EPG data from {self.epg_url}")
            
            if not self.epg_url:
                raise ValueError("EPG_URL is not configured")
            
            response = self.session.get(self.epg_url, timeout=60)
            response.raise_for_status()
            
            # Get content with proper encoding detection
            content = response.content
            
            # Try to detect encoding from response headers
            encoding = response.encoding or 'utf-8'
            
            try:
                # Decode with detected encoding
                text_content = content.decode(encoding)
            except UnicodeDecodeError:
                # Fallback to UTF-8 with error handling
                self.logger.warning(f"Encoding {encoding} failed, falling back to UTF-8")
                text_content = content.decode('utf-8', errors='replace')
            
            # Sanitize the content
            sanitized_content = self.sanitize_utf8(text_content)
            
            self.logger.info(f"Successfully fetched EPG data: {len(sanitized_content)} characters")
            return sanitized_content
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching EPG data: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching EPG data: {e}")
            return None

    def fetch_epg2_data(self) -> Optional[str]:
        """
        Fetch EPG data from the second EPG URL (for images).
        Supports both regular XML files and gzip compressed files.
        
        Returns:
            Raw EPG XML data as string from second URL, None if fetch failed or URL not configured
        """
        if not self.epg2_url:
            self.logger.info("No EPG2_URL configured, skipping image EPG fetch")
            return None
            
        try:
            self.logger.info(f"Fetching image EPG data from {self.epg2_url}")
            
            response = self.session.get(self.epg2_url, timeout=60)
            response.raise_for_status()
            
            content = response.content
            
            # Check if content is gzipped
            if content.startswith(b'\x1f\x8b'):
                self.logger.info("Detected gzipped content, decompressing...")
                try:
                    # Decompress gzip content
                    content = gzip.decompress(content)
                    
                    # If the decompressed content is still an archive (like ZIP), handle it
                    if content.startswith(b'PK'):
                        self.logger.info("Detected ZIP archive inside gzip, extracting...")
                        with tempfile.NamedTemporaryFile() as temp_file:
                            temp_file.write(content)
                            temp_file.flush()
                            
                            with zipfile.ZipFile(temp_file.name, 'r') as zip_file:
                                # Find the first XML file in the archive
                                xml_files = [name for name in zip_file.namelist() if name.lower().endswith('.xml')]
                                
                                if xml_files:
                                    xml_filename = xml_files[0]  # Use the first XML file found
                                    self.logger.info(f"Extracting XML file: {xml_filename}")
                                    content = zip_file.read(xml_filename)
                                else:
                                    self.logger.error("No XML files found in the ZIP archive")
                                    return None
                    
                except gzip.BadGzipFile:
                    self.logger.warning("Failed to decompress as gzip, treating as regular content")
                    content = response.content
                except zipfile.BadZipFile:
                    self.logger.warning("Failed to extract ZIP archive, using decompressed content as is")
                except Exception as e:
                    self.logger.error(f"Error processing compressed content: {e}")
                    return None
            
            # Handle content encoding
            try:
                # Try to detect encoding
                encoding = response.encoding or 'utf-8'
                
                # If we have bytes, decode them
                if isinstance(content, bytes):
                    try:
                        text_content = content.decode(encoding)
                    except UnicodeDecodeError:
                        self.logger.warning(f"Encoding {encoding} failed for EPG2, falling back to UTF-8")
                        text_content = content.decode('utf-8', errors='replace')
                else:
                    text_content = content
                    
            except Exception as e:
                self.logger.error(f"Error decoding content: {e}")
                return None
            
            # Sanitize the content - ensure text_content is a string
            if isinstance(text_content, str):
                sanitized_content = self.sanitize_utf8(text_content)
            else:
                sanitized_content = self.sanitize_utf8(str(text_content))
            
            self.logger.info(f"Successfully processed image EPG data: {len(sanitized_content)} characters")
            return sanitized_content
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Network error fetching image EPG data: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error fetching image EPG data: {e}")
            return None

    def extract_programme_images(self, epg2_root: ET.Element) -> Dict[str, List[str]]:
        """
        Extract programme images from the second EPG.
        
        Args:
            epg2_root: Parsed XML root from second EPG
            
        Returns:
            Dictionary mapping programme identifiers to image URLs
        """
        image_map = {}
        
        for programme in epg2_root.findall('.//programme'):
            # Build programme identifier from channel and time
            channel = programme.get('channel', '')
            start = programme.get('start', '')
            
            if channel and start:
                prog_id = f"{channel}_{start}"
                
                # Extract images from various elements
                images = []
                
                # Look for icon elements
                for icon in programme.findall('.//icon'):
                    src = icon.get('src', '')
                    if src:
                        images.append(src)
                
                # Look for image elements
                for img in programme.findall('.//image'):
                    if img.text:
                        images.append(img.text.strip())
                
                # Look for poster/thumbnail attributes or elements
                for poster in programme.findall('.//poster'):
                    if poster.text:
                        images.append(poster.text.strip())
                
                for thumb in programme.findall('.//thumbnail'):
                    if thumb.text:
                        images.append(thumb.text.strip())
                
                if images:
                    image_map[prog_id] = images
                    self.logger.debug(f"Found {len(images)} images for programme {prog_id}")
        
        self.logger.info(f"Extracted images for {len(image_map)} programmes from second EPG")
        return image_map

    def merge_programme_images(self, main_root: ET.Element, image_map: Dict[str, List[str]]) -> int:
        """
        Merge images from the second EPG into the main EPG programmes using channel mapping.
        
        Args:
            main_root: Main EPG XML root element
            image_map: Dictionary mapping programme identifiers to image URLs
            
        Returns:
            Number of programmes that received images
        """
        merged_count = 0
        
        if not self.channel_mapping:
            self.logger.info("No channel mappings available for image merging")
            return merged_count
        
        # Loop through every assigned EPG1 channel id -> EPG2 channel id mapping
        for epg1_channel, epg2_channel in self.channel_mapping.items():
            if not epg2_channel:  # Skip empty EPG2 mappings
                continue
                
            self.logger.debug(f"Processing channel mapping: {epg1_channel} -> {epg2_channel}")
            
            # Find all programmes for this EPG1 channel
            epg1_programmes = main_root.findall(f".//programme[@channel='{epg1_channel}']")
            
            # Loop through every programme in this channel
            for epg1_programme in epg1_programmes:
                epg1_start = epg1_programme.get('start', '')
                if not epg1_start:
                    continue
                
                epg1_start_time = self.parse_datetime(epg1_start)
                if not epg1_start_time:
                    continue
                
                # Find matching programme in EPG2 based on start time
                matched_images = None
                tolerance = timedelta(minutes=self.time_tolerance_minutes)
                
                # Search through EPG2 programmes for this channel
                for prog_id, images in image_map.items():
                    # Check if this image belongs to the mapped EPG2 channel
                    if not prog_id.startswith(f"{epg2_channel}_"):
                        continue
                    
                    # Extract start time from prog_id
                    parts = prog_id.split('_', 1)
                    if len(parts) >= 2:
                        epg2_start_str = parts[1]
                        epg2_start_time = self.parse_datetime(epg2_start_str)
                        
                        if epg2_start_time:
                            # Normalize for comparison
                            epg1_start_naive = epg1_start_time.replace(tzinfo=None) if epg1_start_time.tzinfo else epg1_start_time
                            epg2_start_naive = epg2_start_time.replace(tzinfo=None) if epg2_start_time.tzinfo else epg2_start_time
                            
                            time_diff = abs(epg1_start_naive - epg2_start_naive)
                            if time_diff <= tolerance:
                                matched_images = images
                                self.logger.debug(f"Time match found: {epg1_channel}@{epg1_start} -> {epg2_channel}@{epg2_start_str} (diff: {time_diff})")
                                break
                
                # Add images if found
                if matched_images:
                    # Remove existing icon elements to avoid duplicates
                    existing_icons = epg1_programme.findall('icon')
                    for icon in existing_icons:
                        epg1_programme.remove(icon)
                    
                    # Add new images
                    for i, image_url in enumerate(matched_images):
                        icon = ET.SubElement(epg1_programme, 'icon')
                        icon.set('src', image_url)
                        
                        if i == 0:
                            icon.set('width', '300')
                            icon.set('height', '200')
                    
                    merged_count += 1
                    self.logger.info(f"Added {len(matched_images)} images to programme {epg1_channel} at {epg1_start}")
        
        return merged_count

    def backup_current_epg(self) -> bool:
        """
        Backup current epg.xml to epg_old.xml.
        
        Returns:
            True if backup successful or no current file exists, False on error
        """
        try:
            if os.path.exists(self.epg_file):
                shutil.copy2(self.epg_file, self.epg_old_file)
                self.logger.info(f"Backed up {self.epg_file} to {self.epg_old_file}")
            else:
                self.logger.info(f"No existing {self.epg_file} to backup")
            return True
        except Exception as e:
            self.logger.error(f"Failed to backup EPG file: {e}")
            return False

    def parse_epg_xml(self, xml_content: str) -> Optional[ET.Element]:
        """
        Parse EPG XML content.
        
        Args:
            xml_content: XML content as string
            
        Returns:
            Parsed XML root element, None if parsing failed
        """
        try:
            # Clean up common XML issues
            xml_content = xml_content.strip()
            
            # Parse the XML
            root = ET.fromstring(xml_content)
            return root
        except ET.ParseError as e:
            self.logger.error(f"XML parsing error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Unexpected error parsing XML: {e}")
            return None

    def parse_datetime(self, dt_str: str) -> Optional[datetime]:
        """
        Parse datetime string from EPG format.
        
        Args:
            dt_str: Datetime string in various EPG formats
            
        Returns:
            Parsed datetime object, None if parsing failed
        """
        if not dt_str:
            return None
        
        # Common EPG datetime formats
        formats = [
            '%Y%m%d%H%M%S %z',  # XMLTV format with timezone
            '%Y%m%d%H%M%S',     # XMLTV format without timezone
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%SZ'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(dt_str, fmt)
            except ValueError:
                continue
        
        self.logger.warning(f"Could not parse datetime: {dt_str}")
        return None

    def get_programme_time_range(self, programme: ET.Element) -> Tuple[Optional[datetime], Optional[datetime]]:
        """
        Extract start and stop times from a programme element.
        
        Args:
            programme: Programme XML element
            
        Returns:
            Tuple of (start_time, stop_time), either can be None if not parseable
        """
        start_str = programme.get('start', '')
        stop_str = programme.get('stop', '')
        
        start_time = self.parse_datetime(start_str)
        stop_time = self.parse_datetime(stop_str)
        
        return start_time, stop_time

    def programmes_overlap(self, prog1: ET.Element, prog2: ET.Element) -> bool:
        """
        Check if two programmes overlap within the time tolerance.
        
        Args:
            prog1: First programme element
            prog2: Second programme element
            
        Returns:
            True if programmes overlap within tolerance
        """
        start1, stop1 = self.get_programme_time_range(prog1)
        start2, stop2 = self.get_programme_time_range(prog2)
        
        if not all([start1, stop1, start2, stop2]):
            return False
        
        # Type assertion: we've verified all values are not None above
        assert start1 is not None and stop1 is not None
        assert start2 is not None and stop2 is not None
        
        tolerance = timedelta(minutes=self.time_tolerance_minutes)
        
        # Check if programmes overlap with tolerance
        # prog1 starts before prog2 ends (with tolerance) AND prog1 ends after prog2 starts (with tolerance)
        overlap = (start1 <= stop2 + tolerance) and (stop1 + tolerance >= start2)
        
        return overlap

    def merge_missing_programmes(self, new_root: ET.Element, old_root: ET.Element) -> int:
        """
        Merge missing programmes from old EPG into new EPG.
        
        Args:
            new_root: New EPG XML root element
            old_root: Old EPG XML root element
            
        Returns:
            Number of programmes merged
        """
        merged_count = 0
        
        # Get all programmes from both EPGs organized by channel
        new_programmes_by_channel = {}
        old_programmes_by_channel = {}
        
        # Organize new programmes by channel
        for programme in new_root.findall('.//programme'):
            channel = programme.get('channel', '')
            if channel not in new_programmes_by_channel:
                new_programmes_by_channel[channel] = []
            new_programmes_by_channel[channel].append(programme)
        
        # Organize old programmes by channel
        for programme in old_root.findall('.//programme'):
            channel = programme.get('channel', '')
            if channel not in old_programmes_by_channel:
                old_programmes_by_channel[channel] = []
            old_programmes_by_channel[channel].append(programme)
        
        # For each channel in old EPG, check for missing programmes
        for channel, old_programmes in old_programmes_by_channel.items():
            new_programmes = new_programmes_by_channel.get(channel, [])
            
            for old_programme in old_programmes:
                # Skip programs older than 1 day
                start_time, _ = self.get_programme_time_range(old_programme)
                if start_time:
                    # Normalize datetimes for comparison (remove timezone info if present)
                    start_time_naive = start_time.replace(tzinfo=None) if start_time.tzinfo else start_time
                    one_day_ago = datetime.now() - timedelta(days=1)
                    if start_time_naive < one_day_ago:
                        continue  # Skip this old programme
                
                # Check if this programme is missing in new EPG
                is_missing = True
                
                for new_programme in new_programmes:
                    if self.programmes_overlap(old_programme, new_programme):
                        is_missing = False
                        break
                
                if is_missing:
                    # Add the missing programme to new EPG
                    new_root.append(old_programme)
                    merged_count += 1
                    
                    start_time, stop_time = self.get_programme_time_range(old_programme)
                    self.logger.info(f"Merged missing programme for channel {channel}: "
                                   f"{start_time} - {stop_time}")
        
        return merged_count

    def merge_missing_channels(self, new_root: ET.Element, old_root: ET.Element) -> int:
        """
        Merge missing channel information from old EPG into new EPG.
        
        Args:
            new_root: New EPG XML root element
            old_root: Old EPG XML root element
            
        Returns:
            Number of channels merged
        """
        merged_count = 0
        
        # Get existing channel IDs in new EPG
        new_channel_ids = set()
        for channel in new_root.findall('.//channel'):
            channel_id = channel.get('id', '')
            if channel_id:
                new_channel_ids.add(channel_id)
        
        # Add missing channels from old EPG
        for old_channel in old_root.findall('.//channel'):
            channel_id = old_channel.get('id', '')
            if channel_id and channel_id not in new_channel_ids:
                new_root.append(old_channel)
                merged_count += 1
                self.logger.info(f"Merged missing channel: {channel_id}")
        
        return merged_count

    def save_epg_file(self, root: ET.Element) -> bool:
        """
        Save EPG XML to file with proper UTF-8 encoding.
        
        Args:
            root: XML root element to save
            
        Returns:
            True if save successful, False otherwise
        """
        try:
            # Create XML string with proper declaration
            xml_str = ET.tostring(root, encoding='unicode', method='xml')
            
            # Add XML declaration if not present
            if not xml_str.startswith('<?xml'):
                xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str
            
            # Sanitize the final XML content
            xml_str = self.sanitize_utf8(xml_str)
            
            # Write to file with UTF-8 encoding
            with codecs.open(self.epg_file, 'w', encoding='utf-8') as f:
                f.write(xml_str)
            
            self.logger.info(f"Successfully saved EPG file: {self.epg_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save EPG file: {e}")
            return False
        
    def plex_safe_channel_id(self, raw_id: str) -> str:
        """
        Convert a channel id into a Plex-safe form.
        - URL-decode first (%20 -> space, etc.)
        - Remove diacritics (č -> c, š -> s, etc.)
        - Remove spaces
        - Allow only letters, digits, '.', '-', '_'
        - Lowercase for consistency
        """
        # Decode percent escapes
        decoded = urllib.parse.unquote(raw_id)

        # Remove diacritics
        normalized = unicodedata.normalize("NFKD", decoded)
        no_diacritics = "".join(c for c in normalized if not unicodedata.combining(c))

        # Remove spaces
        no_spaces = no_diacritics.replace(" ", "")

        # Keep only safe characters
        safe = re.sub(r"[^A-Za-z0-9._-]", "", no_spaces)

        return safe.lower()
    
    def save_escaped_epg_file(self) -> bool:
        """
        Read EPG XML from self.epg_file, escape all channel ids and programme channel refs
        using plex_safe_channel_id(), and save to self.epg_file_escaped.
        """
        try:
            # Parse existing EPG file
            tree = ET.parse(self.epg_file)
            root = tree.getroot()

            # Process all <channel> elements
            for channel in root.findall("channel"):
                old_id = channel.get("id")
                if old_id:
                    new_id = self.plex_safe_channel_id(old_id)
                    channel.set("id", new_id)

            # Process all <programme> elements
            for programme in root.findall("programme"):
                old_channel = programme.get("channel")
                if old_channel:
                    new_channel = self.plex_safe_channel_id(old_channel)
                    programme.set("channel", new_channel)

            # Convert back to string
            xml_str = ET.tostring(root, encoding="unicode", method="xml")

            # Add XML declaration if not present
            if not xml_str.startswith("<?xml"):
                xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str

            # Sanitize XML content
            xml_str = self.sanitize_utf8(xml_str)

            # Write to new file
            with codecs.open(self.epg_file_escaped, "w", encoding="utf-8") as f:
                f.write(xml_str)

            self.logger.info(f"Successfully saved escaped EPG file: {self.epg_file_escaped}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to save escaped EPG file: {e}")
            return False

    def update_epg(self):
        """Main EPG update process."""
        self.logger.info("Starting EPG update process")

        if self.is_now_in_cron(self.skip_cron):
            self.logger.info("This time is configured to be skipped from scanning")
            return
        
        try:
            # Step 1: Backup current EPG
            if not self.backup_current_epg():
                self.logger.error("Failed to backup current EPG, aborting update")
                return
            
            # Step 2: Fetch new EPG data
            new_epg_content = self.fetch_epg_data()
            if not new_epg_content:
                self.logger.error("Failed to fetch new EPG data, keeping existing file")
                return
            
            # Step 3: Parse new EPG data
            new_root = self.parse_epg_xml(new_epg_content)
            if new_root is None:
                self.logger.error("Failed to parse new EPG data, keeping existing file")
                return
            
            # Step 3.5: Save EPG1 channels to separate CSV file
            self.save_epg1_channels_to_csv(new_root)
            
            # Step 4: If we have old EPG data, merge missing information
            merged_programmes = 0
            merged_channels = 0
            
            if os.path.exists(self.epg_old_file):
                try:
                    with codecs.open(self.epg_old_file, 'r', encoding='utf-8') as f:
                        old_epg_content = f.read()
                    
                    old_root = self.parse_epg_xml(old_epg_content)
                    if old_root is not None:
                        # Merge missing channels and programmes
                        merged_channels = self.merge_missing_channels(new_root, old_root)
                        merged_programmes = self.merge_missing_programmes(new_root, old_root)
                        
                        self.logger.info(f"Merged {merged_channels} channels and "
                                       f"{merged_programmes} programmes from old EPG")
                    else:
                        self.logger.warning("Could not parse old EPG file for merging")
                        
                except Exception as e:
                    self.logger.warning(f"Could not read old EPG file for merging: {e}")
            
            # Step 5: Fetch and merge images from second EPG if configured
            merged_images = 0
            if self.epg2_url:
                epg2_content = self.fetch_epg2_data()
                if epg2_content:
                    epg2_root = self.parse_epg_xml(epg2_content)
                    if epg2_root is not None:
                        # Save EPG2 channels to separate CSV file
                        self.save_epg2_channels_to_csv(epg2_root)
                        
                        # Extract and merge images
                        image_map = self.extract_programme_images(epg2_root)
                        merged_images = self.merge_programme_images(new_root, image_map)
                        self.logger.info(f"Added images to {merged_images} programmes from second EPG")
                    else:
                        self.logger.warning("Could not parse second EPG file for images")
                else:
                    self.logger.warning("Could not fetch second EPG for images")
            
            # Step 6: Save updated EPG
            if self.save_epg_file(new_root):
                image_info = f", {merged_images} images" if merged_images > 0 else ""
                self.logger.info(f"EPG update completed successfully. "
                               f"Merged: {merged_channels} channels, {merged_programmes} programmes{image_info}")
            else:
                self.logger.error("Failed to save updated EPG file")
            
            # Step 7: Save escaped EPG
            if self.save_escaped_epg_file():
                self.logger.info(f"EPG escaped write completed successfully.")
            else:
                self.logger.error("Failed to save escaped EPG file")
                
        except Exception as e:
            self.logger.error(f"Unexpected error during EPG update: {e}")

    def run_scheduler(self):
        """Run the hourly scheduler."""
        self.logger.info("Starting EPG cacher scheduler - updates every hour")
        
        # Schedule hourly updates
        schedule.every().hour.do(self.update_epg)
        
        # Run initial update
        self.update_epg()
        
        # Keep running and check for scheduled tasks
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except KeyboardInterrupt:
                self.logger.info("Received interrupt signal, shutting down")
                break
            except Exception as e:
                self.logger.error(f"Scheduler error: {e}")
                time.sleep(60)  # Continue after error


def main():
    """Main entry point."""
    try:
        cacher = EPGCacher()
        cacher.run_scheduler()
    except ValueError as e:
        print(f"Configuration error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
