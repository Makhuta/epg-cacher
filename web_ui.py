#!/usr/bin/env python3
"""
Main entry point for EPG Channel Mapping Web UI
"""

import csv
import os
import logging
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from typing import List, Dict, Tuple

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', 'epg-cacher-secret-key-change-in-production')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)
CHANNEL_MAPPING_FILE = os.path.join(output_dir, 'channel_mapping.csv')
CHANNELS_EPG1_FILE = os.path.join(output_dir, 'channels_epg1.csv')
CHANNELS_EPG2_FILE = os.path.join(output_dir, 'channels_epg2.csv')

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
        """Get statistics about channel mappings."""
        mappings = self.load_mappings()
        
        total_channels = len(mappings)
        mapped_channels = len([m for m in mappings if m['epg1_channel'] and m['epg2_channel']])
        unmapped_channels = total_channels - mapped_channels
        
        return {
            'total': total_channels,
            'mapped': mapped_channels,
            'unmapped': unmapped_channels
        }
    
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

# Initialize manager
mapping_manager = ChannelMappingManager()

@app.route('/')
def index():
    """Main dashboard showing channel mapping overview."""
    stats = mapping_manager.get_stats()
    return render_template('index.html', stats=stats)

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)