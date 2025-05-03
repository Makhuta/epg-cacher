import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import tarfile
import gzip


from fetch_vbox import run as vbox_run
from fetch_epgshare import run as epgshare_run
from add_icons import run as add_icons_run
from constants import (
    OUTPUT_DIR
)


os.makedirs(OUTPUT_DIR, exist_ok=True)

EPG_URL = os.getenv("EPG_URL", "http://192.168.2.45:55555/vboxXmltv.xml")
EPG_URL_VBOX = os.getenv("EPG_URL_VBOX", "http://192.168.2.15:34402/xmltv/threadfin.xml")
EPG_URL_EPGSHARE = os.getenv("EPG_URL_EPGSHARE", "http://192.168.2.15:34403/xmltv/threadfin.xml")



"""

def attributes_match(existing, element) -> bool:
    existing_keys = existing.attrib.keys()
    element_keys = element.attrib.keys()

    if len(existing_keys) != len(element_keys):
        return False

    for existing_key in existing_keys:
        if existing_key not in element_keys:
            return False
        
        if existing_key == "start":
            continue

        if existing_key == "stop":
            if round_time_to_nearest_five(existing.attrib.get(existing_key), DATE_FMT) == round_time_to_nearest_five(element.attrib.get(existing_key), DATE_FMT):
                continue

        
        if existing.attrib.get(existing_key) != element.attrib.get(existing_key):
            return False
        
    return True

def element_exists_in_tree(element, tree):
    ""Check if the element already exists in tree based on a unique identifier or tag.""
    root = tree.getroot()
    # Example: Check if an element with the same 'id' or 'name' exists in the tree
    for existing_elem in root.findall(element.tag):
        if attributes_match(existing_elem, element):  # Adjust based on the unique field
            return True
    return False





def merge_epgs():
    fetch_tree = get_tree(FETCH_CACHE)
    epg_old_tree = get_tree(EPG_OLD)

    fetch_root = fetch_tree.getroot() if fetch_tree else None
    epg_old_root = epg_old_tree.getroot() if epg_old_tree else None

    epg_out_tree = create_empty_tree(fetch_root.tag)
    epg_out_root = epg_out_tree.getroot() if epg_out_tree else None

    # Writing fetched elements to out
    if fetch_root is not None:
        for fetch_elem in fetch_root:
            epg_out_root.append(fetch_elem)

    # Writing missing cached elements to out
    if epg_old_root is not None:
        for epg_old_elem in epg_old_root:
            if not element_exists_in_tree(epg_old_elem, epg_out_tree):
                if epg_old_elem.tag == "programme" and is_old_programme(epg_old_elem, DATE_FMT):
                    print("Is programme and is old, skipping...")
                    continue
                print(f"Element {epg_old_elem.tag} is missing, adding...")
                epg_out_root.append(epg_old_elem)

    return epg_out_tree
"""

def main():
    vbox_run(EPG_URL)
    #epgshare_run(EPG_URL_EPGSHARE)
    add_icons_run(EPG_URL_VBOX, EPG_URL_EPGSHARE)

    # Step 1: Download and cache
    # download_epg(EPG_URL)
    """
    download_epg(EPG_URL_EPGSHARE, FETCH_CACHE_EPGSHARE)

    # Step 2: Backup old EPG if exists
    backup_old_epg(EPG_PATH, EPG_OLD)

    #Step 3: Merge EPGs
    epg_out = merge_epgs()

    # Step 4: Write EPG
    write_epg(epg_out, EPG_PATH)

    print("DONE")
    """

if __name__ == "__main__":
    INTERVAL = int(os.getenv("INTERVAL", 3600))
    while True:
        main()
        time.sleep(INTERVAL)
