#!/bin/sh
set -e

gunicorn web_ui:app -w 4 -b 0.0.0.0:8000 & nginx -g 'daemon off;' & python epg_cacher.py
