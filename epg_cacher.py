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
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
import requests
import schedule


class EPGCacher:
    def __init__(self):
        """Initialize the EPG Cacher with environment variables and logging."""
        # Environment variables
        self.epg_url = os.getenv("EPG_URL")
        if not self.epg_url:
            raise ValueError("EPG_URL environment variable is required")
        
        self.time_tolerance_minutes = int(os.getenv("TIME_TOLERANCE_MINUTES", "10"))
        
        # File paths
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        self.epg_file = os.path.join(output_dir, "epg.xml")
        self.epg_old_file = os.path.join(output_dir, "epg_old.xml")
        
        # Setup logging
        self.setup_logging()
        
        # HTTP session for reuse
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'EPG-Cacher/1.0',
            'Accept': 'application/xml, text/xml, */*'
        })
        
        self.logger.info(f"EPG Cacher initialized - URL: {self.epg_url}, "
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
        skipped_channels = 0
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
                        skipped_channels += 1
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
                    
        self.logger.info(f"Skipped {skipped_channels} old programmes.")
        
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

    def update_epg(self):
        """Main EPG update process."""
        self.logger.info("Starting EPG update process")
        
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
            
            # Step 5: Save updated EPG
            if self.save_epg_file(new_root):
                self.logger.info(f"EPG update completed successfully. "
                               f"Merged: {merged_channels} channels, {merged_programmes} programmes")
            else:
                self.logger.error("Failed to save updated EPG file")
                
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
