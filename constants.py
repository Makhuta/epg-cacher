import os


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")

FETCH_CACHE_VBOX = os.path.join(OUTPUT_DIR, "fetch_cache_vbox.xml")
EPG_VBOX_PATH = os.path.join(OUTPUT_DIR, "epg_vbox.xml")
EPG_VBOX_OLD = os.path.join(OUTPUT_DIR, "epg_vbox_old.xml")
FETCH_CACHE_EPGSHARE = os.path.join(OUTPUT_DIR, "fetch_cache_epgshare.xml")
EPG_EPGSHARE_PATH = os.path.join(OUTPUT_DIR, "epg_epgshare.xml")
EPG_EPGSHARE_OLD = os.path.join(OUTPUT_DIR, "epg_epgshare_old.xml")

EPG_PATH = os.path.join(OUTPUT_DIR, "epg.xml")
EPG_OLD = os.path.join(OUTPUT_DIR, "epg_old.xml")

DATE_FMT = "%Y%m%d%H%M%S %z"  # XMLTV standard time format