#!/usr/bin/env python3
"""
Main entry point for EPG Channel Mapping Web UI
"""

import csv
import os
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from typing import List, Dict, Tuple, Optional

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', 'epg-cacher-secret-key-change-in-production')

# Configure Flask app for header size limits
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max request size
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)
CHANNEL_MAPPING_FILE = os.path.join(output_dir, 'channel_mapping.csv')
CHANNELS_EPG1_FILE = os.path.join(output_dir, 'channels_epg1.csv')
CHANNELS_EPG2_FILE = os.path.join(output_dir, 'channels_epg2.csv')
EPG_FILE = os.path.join(output_dir, 'epg.xml')

class ChannelMappingManager:
    """Manages channel mapping operations for the web UI."""
    
    def __init__(self, csv_file: str = CHANNEL_MAPPING_FILE):
        self.csv_file = csv_file
        self.channels_epg1_file = CHANNELS_EPG1_FILE
        self.channels_epg2_file = CHANNELS_EPG2_FILE
        
    def load_mappings(self) -> List[Dict[str, str]]:
        """Load all channel mappings from CSV file."""
        mappings = []
        
        if not os.path.exists(self.csv_file):
            logger.warning(f"CSV file {self.csv_file} not found")
            return mappings
            
        try:
            with open(self.csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    epg1_channel = row.get('EPG1_Channel_ID', '').strip()
                    epg2_channel = row.get('EPG2_Channel_ID', '').strip()
                    
                    mappings.append({
                        'epg1_channel': epg1_channel,
                        'epg2_channel': epg2_channel
                    })
                    
        except Exception as e:
            logger.error(f"Error loading mappings: {e}")
            
        return mappings
    
    def save_mappings(self, mappings: List[Dict[str, str]]) -> bool:
        """Save channel mappings to CSV file."""
        try:
            with open(self.csv_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                
                # Write header
                writer.writerow(['EPG1_Channel_ID', 'EPG2_Channel_ID'])
                
                # Write mappings
                for mapping in mappings:
                    epg1 = mapping.get('epg1_channel', '').strip()
                    epg2 = mapping.get('epg2_channel', '').strip()
                    writer.writerow([epg1, epg2])
                    
            logger.info(f"Saved {len(mappings)} mappings to {self.csv_file}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving mappings: {e}")
            return False
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about channel mappings based on both mapping file and EPG1 channels."""
        mappings = self.load_mappings()
        epg1_channels = self.load_epg1_channels()
        
        # Get all unique EPG1 channel IDs from both sources
        mapped_channel_ids = {m['epg1_channel'] for m in mappings if m['epg1_channel']}
        epg1_channel_ids = {ch['id'] for ch in epg1_channels}
        all_channel_ids = mapped_channel_ids.union(epg1_channel_ids)
        
        # Count channels that have actual mappings (both EPG1 and EPG2)
        mapped_channels = len([m for m in mappings if m['epg1_channel'] and m['epg2_channel']])
        
        # Count pseudo-unmapped channels (mapped but no EPG2)
        pseudo_unmapped_channels = len([m for m in mappings if m['epg1_channel'] and not m['epg2_channel']])
        
        # Total is all unique channels from both sources
        total_channels = len(all_channel_ids)
        
        # Unmapped is total minus mapped
        unmapped_channels = total_channels - mapped_channels
        
        # Calculate percentages
        coverage_percentage = (mapped_channels / total_channels * 100) if total_channels > 0 else 0.0
        pseudo_unmapped_percentage = (pseudo_unmapped_channels / total_channels * 100) if total_channels > 0 else 0.0
        total_completion_percentage = ((mapped_channels + pseudo_unmapped_channels) / total_channels * 100) if total_channels > 0 else 0.0
        
        return {
            'total': total_channels,
            'mapped': mapped_channels,
            'unmapped': unmapped_channels,
            'pseudo_unmapped': pseudo_unmapped_channels,
            'coverage_percentage': round(float(coverage_percentage), 1),
            'pseudo_unmapped_percentage': round(float(pseudo_unmapped_percentage), 1),
            'total_completion_percentage': round(float(total_completion_percentage), 1)
        }
    
    def get_unmapped_channels(self) -> List[Dict[str, str]]:
        """Get channels from EPG1 that are not in the mapping file at all."""
        mappings = self.load_mappings()
        epg1_channels = self.load_epg1_channels()
        
        # Get channel IDs that are already in mappings
        mapped_channel_ids = {m['epg1_channel'] for m in mappings if m['epg1_channel']}
        
        # Return EPG1 channels that are not in mappings at all
        unmapped = [ch for ch in epg1_channels if ch['id'] not in mapped_channel_ids]
        
        return unmapped
    
    def get_pseudo_unmapped_channels(self) -> List[Dict[str, str]]:
        """Get channels that are in mapping file but have no EPG2 channel assigned."""
        mappings = self.load_mappings()
        epg1_channels = self.load_epg1_channels()
        
        # Create a lookup for EPG1 channel names
        epg1_lookup = {ch['id']: ch['name'] for ch in epg1_channels}
        
        # Find mappings with EPG1 but no EPG2
        pseudo_unmapped = []
        for mapping in mappings:
            if mapping['epg1_channel'] and not mapping['epg2_channel']:
                pseudo_unmapped.append({
                    'id': mapping['epg1_channel'],
                    'name': epg1_lookup.get(mapping['epg1_channel'], mapping['epg1_channel'])
                })
        
        return pseudo_unmapped
    
    def load_epg1_channels(self) -> List[Dict[str, str]]:
        """Load EPG1 channels from channels_epg1.csv file."""
        epg1_channels = []
        
        if not os.path.exists(self.channels_epg1_file):
            logger.info(f"EPG1 channels CSV file {self.channels_epg1_file} not found")
            return epg1_channels
            
        try:
            with open(self.channels_epg1_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    channel_id = row.get('EPG1_Channel_ID', '').strip()
                    channel_name = row.get('Channel_Name', '').strip()
                    
                    if channel_id:
                        epg1_channels.append({
                            'id': channel_id,
                            'name': channel_name or channel_id
                        })
                        
        except Exception as e:
            logger.error(f"Error loading EPG1 channels: {e}")
            
        return epg1_channels

    def load_epg2_channels(self) -> List[Dict[str, str]]:
        """Load EPG2 channels from channels_epg2.csv file."""
        epg2_channels = []
        
        if not os.path.exists(self.channels_epg2_file):
            logger.info(f"EPG2 channels CSV file {self.channels_epg2_file} not found")
            return epg2_channels
            
        try:
            with open(self.channels_epg2_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    channel_id = row.get('EPG2_Channel_ID', '').strip()
                    channel_name = row.get('Channel_Name', '').strip()
                    
                    if channel_id:
                        epg2_channels.append({
                            'id': channel_id,
                            'name': channel_name or channel_id
                        })
                        
        except Exception as e:
            logger.error(f"Error loading EPG2 channels: {e}")
            
        return epg2_channels
    
    def parse_datetime(self, datetime_str: str) -> Optional[datetime]:
        """Parse XMLTV datetime format (YYYYMMDDHHMMSS +TTTT)."""
        try:
            # XMLTV format: 20240821120000 +0000
            if ' ' in datetime_str:
                dt_part = datetime_str.split(' ')[0]
            else:
                dt_part = datetime_str
            
            # Parse the main datetime part
            if len(dt_part) >= 14:
                return datetime.strptime(dt_part[:14], '%Y%m%d%H%M%S')
            elif len(dt_part) >= 12:
                return datetime.strptime(dt_part[:12], '%Y%m%d%H%M')
            elif len(dt_part) >= 8:
                return datetime.strptime(dt_part[:8], '%Y%m%d')
            else:
                return None
        except ValueError:
            return None
    
    def load_epg_data(self) -> Optional[ET.Element]:
        """Load and parse EPG XML data."""
        if not os.path.exists(EPG_FILE):
            return None
        
        try:
            tree = ET.parse(EPG_FILE)
            return tree.getroot()
        except ET.ParseError as e:
            logger.error(f"Error parsing EPG XML: {e}")
            return None
        except Exception as e:
            logger.error(f"Error loading EPG file: {e}")
            return None
    
    def get_epg_channels(self) -> List[Dict[str, str]]:
        """Get channels from EPG data."""
        epg_root = self.load_epg_data()
        if epg_root is None:
            return []
        
        channels = []
        channel_ids_seen = set()
        
        # Get channels from channel elements
        for channel in epg_root.findall('.//channel'):
            channel_id = channel.get('id', '').strip()
            if channel_id and channel_id not in channel_ids_seen:
                channel_ids_seen.add(channel_id)
                
                # Get channel name from display-name
                channel_name = channel_id
                display_name = channel.find('display-name')
                if display_name is not None and display_name.text:
                    channel_name = display_name.text.strip()
                
                channels.append({
                    'id': channel_id,
                    'name': channel_name
                })
        
        # Also get channels from programme elements if not in channel list
        for programme in epg_root.findall('.//programme'):
            channel_id = programme.get('channel', '').strip()
            if channel_id and channel_id not in channel_ids_seen:
                channel_ids_seen.add(channel_id)
                channels.append({
                    'id': channel_id,
                    'name': channel_id
                })
        
        return sorted(channels, key=lambda x: x['name'].lower())
    
    def get_epg_programmes(self, start_time: datetime, hours: int = 12) -> Dict[str, List[Dict]]:
        """Get EPG programmes for a specific time window."""
        epg_root = self.load_epg_data()
        if epg_root is None:
            return {}
        
        end_time = start_time + timedelta(hours=hours)
        programmes_by_channel = {}
        
        for programme in epg_root.findall('.//programme'):
            channel_id = programme.get('channel', '').strip()
            if not channel_id:
                continue
            
            # Parse programme times
            start_str = programme.get('start', '')
            stop_str = programme.get('stop', '')
            
            prog_start = self.parse_datetime(start_str)
            prog_stop = self.parse_datetime(stop_str)
            
            if not prog_start:
                continue
            
            # Check if programme overlaps with our time window
            if prog_stop and prog_stop <= start_time:
                continue  # Programme ends before our window
            if prog_start >= end_time:
                continue  # Programme starts after our window
            
            # Get programme details
            title = ""
            title_elem = programme.find('title')
            if title_elem is not None and title_elem.text:
                title = title_elem.text.strip()
            
            desc = ""
            desc_elem = programme.find('desc')
            if desc_elem is not None and desc_elem.text:
                desc = desc_elem.text.strip()

            # Get programme icon
            icon_url = ""
            icon_elem = programme.find('icon')
            if icon_elem is not None:
                icon_url = icon_elem.get('src', '').strip()
            
            # Add programme to channel
            
            # Add programme to channel
            if channel_id not in programmes_by_channel:
                programmes_by_channel[channel_id] = []
            
            programmes_by_channel[channel_id].append({
                'start': prog_start,
                'stop': prog_stop,
                'start_str': start_str,
                'stop_str': stop_str,
                'title': title or 'No Title',
                'desc': desc,
                'icon': icon_url
            })
        
        # Sort programmes by start time for each channel
        for channel_id in programmes_by_channel:
            programmes_by_channel[channel_id].sort(key=lambda x: x['start'])
        
        return programmes_by_channel

# Initialize manager
mapping_manager = ChannelMappingManager()

# Error handlers for header/request size issues
@app.errorhandler(413)
def request_entity_too_large(error):
    """Handle request too large errors."""
    flash('Request too large. Please try again.', 'error')
    return redirect(url_for('index')), 413

@app.errorhandler(400)
def bad_request(error):
    """Handle bad request errors including header issues."""
    flash('Bad request. Please clear your browser cache and try again.', 'error')
    return redirect(url_for('index')), 400

@app.before_request
def clear_large_session():
    """Clear session if it becomes too large."""
    try:
        # Estimate session size
        session_size = len(str(request.cookies.get('session', '')))
        if session_size > 3000:  # If session cookie is larger than 3KB
            from flask import session
            session.clear()
            logger.warning("Cleared large session data")
    except Exception:
        pass

@app.route('/')
def index():
    """Main dashboard showing channel mapping overview."""
    stats = mapping_manager.get_stats()
    unmapped_channels = mapping_manager.get_unmapped_channels()
    pseudo_unmapped_channels = mapping_manager.get_pseudo_unmapped_channels()
    return render_template('index.html', 
                         stats=stats, 
                         unmapped_channels=unmapped_channels,
                         pseudo_unmapped_channels=pseudo_unmapped_channels)

@app.route('/mappings')
def mappings():
    """View and edit channel mappings."""
    mappings = mapping_manager.load_mappings()
    epg1_channels = mapping_manager.load_epg1_channels()
    epg2_channels = mapping_manager.load_epg2_channels()
    
    # Filter out EPG1 channels that are already mapped
    existing_epg1_channels = {mapping['epg1_channel'] for mapping in mappings if mapping['epg1_channel']}
    available_epg1_channels = [channel for channel in epg1_channels if channel['id'] not in existing_epg1_channels]
    
    return render_template('mappings.html', 
                         mappings=mappings, 
                         epg1_channels=available_epg1_channels, 
                         epg2_channels=epg2_channels)

@app.route('/add_mapping', methods=['POST'])
def add_mapping():
    """Add a new channel mapping."""
    try:
        epg1_channel = request.form.get('epg1_channel', '').strip()
        epg2_channel = request.form.get('epg2_channel', '').strip()
        
        if not epg1_channel:
            flash('EPG1 Channel ID is required.', 'error')
            return redirect(url_for('mappings'))
        
        # Load existing mappings
        mappings = mapping_manager.load_mappings()
        
        # Check if EPG1 channel already exists
        for mapping in mappings:
            if mapping['epg1_channel'] == epg1_channel:
                flash(f'EPG1 channel "{epg1_channel}" already exists.', 'warning')
                return redirect(url_for('mappings'))
        
        # Add new mapping
        mappings.append({
            'epg1_channel': epg1_channel,
            'epg2_channel': epg2_channel
        })
        
        # Save updated mappings
        if mapping_manager.save_mappings(mappings):
            flash(f'Added new channel mapping for "{epg1_channel}".', 'success')
        else:
            flash('Error adding channel mapping.', 'error')
            
    except Exception as e:
        logger.error(f"Error adding mapping: {e}")
        flash(f'Error adding mapping: {str(e)}', 'error')
    
    return redirect(url_for('mappings'))

@app.route('/delete_mapping', methods=['POST'])
def delete_mapping():
    """Delete a channel mapping."""
    try:
        epg1_channel = request.form.get('epg1_channel', '').strip()
        
        if not epg1_channel:
            flash('EPG1 Channel ID is required for deletion.', 'error')
            return redirect(url_for('mappings'))
        
        # Load existing mappings
        mappings = mapping_manager.load_mappings()
        
        # Filter out the mapping to delete
        original_count = len(mappings)
        mappings = [m for m in mappings if m['epg1_channel'] != epg1_channel]
        
        if len(mappings) == original_count:
            flash(f'Channel mapping for "{epg1_channel}" not found.', 'warning')
        else:
            # Save updated mappings
            if mapping_manager.save_mappings(mappings):
                flash(f'Deleted channel mapping for "{epg1_channel}".', 'success')
            else:
                flash('Error deleting channel mapping.', 'error')
                
    except Exception as e:
        logger.error(f"Error deleting mapping: {e}")
        flash(f'Error deleting mapping: {str(e)}', 'error')
    
    return redirect(url_for('mappings'))

@app.route('/api/stats')
def api_stats():
    """API endpoint for channel mapping statistics."""
    stats = mapping_manager.get_stats()
    return jsonify(stats)

@app.route('/api/mappings')
def api_mappings():
    """API endpoint for channel mappings."""
    mappings = mapping_manager.load_mappings()
    return jsonify(mappings)

@app.route('/api/epg1_channels')
def api_epg1_channels():
    """API endpoint for EPG1 channels."""
    epg1_channels = mapping_manager.load_epg1_channels()
    return jsonify(epg1_channels)

@app.route('/api/epg2_channels')
def api_epg2_channels():
    """API endpoint for EPG2 channels."""
    epg2_channels = mapping_manager.load_epg2_channels()
    return jsonify(epg2_channels)

@app.route('/epg')
def epg_viewer():
    """EPG viewer page."""
    # Get EPG channels - load all available data for client-side caching
    channels = mapping_manager.get_epg_channels()
    
    return render_template('epg_viewer.html', channels=channels)

@app.route('/api/epg_data')
def api_epg_data():
    """API endpoint to get full EPG data for client-side caching."""
    epg_root = mapping_manager.load_epg_data()
    if epg_root is None:
        return jsonify({'programmes': {}, 'error': 'No EPG data available'})
    
    programmes_json = {}
    channels = []
    
    # Get channels first
    for channel in epg_root.findall('.//channel'):
        channel_id = channel.get('id', '').strip()
        if channel_id:
            channel_name = channel_id
            display_name = channel.find('display-name')
            if display_name is not None and display_name.text:
                channel_name = display_name.text.strip()
            
            channels.append({
                'id': channel_id,
                'name': channel_name
            })
    
    # Get programmes
    
    for programme in epg_root.findall('.//programme'):
        channel_id = programme.get('channel', '').strip()
        if not channel_id:
            continue
        
        # Parse programme times
        start_str = programme.get('start', '')
        stop_str = programme.get('stop', '')
        
        prog_start = mapping_manager.parse_datetime(start_str)
        prog_stop = mapping_manager.parse_datetime(stop_str)
        
        if not prog_start:
            continue
        
        # Get programme details
        title = ""
        title_elem = programme.find('title')
        if title_elem is not None and title_elem.text:
            title = title_elem.text.strip()
        
        desc = ""
        desc_elem = programme.find('desc')
        if desc_elem is not None and desc_elem.text:
            desc = desc_elem.text.strip()
        
        # Get programme icon
        icon_url = ""
        icon_elem = programme.find('icon')
        if icon_elem is not None:
            icon_url = icon_elem.get('src', '').strip()
        
        # Add programme to channel
        if channel_id not in programmes_json:
            programmes_json[channel_id] = []
        
        programmes_json[channel_id].append({
            'start': prog_start.isoformat(),
            'stop': prog_stop.isoformat() if prog_stop else None,
            'title': title or 'No Title',
            'desc': desc,
            'icon': icon_url
        })
    
    # Sort programmes by start time for each channel
    for channel_id in programmes_json:
        programmes_json[channel_id].sort(key=lambda x: x['start'])
    
    return jsonify({
        'programmes': programmes_json,
        'channels': channels,
        'source': 'xml'
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)