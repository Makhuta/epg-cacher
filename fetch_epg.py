import requests
import os
import shutil
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import re

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

EPG_URL = os.getenv("EPG_URL", None)
FETCH_CACHE = os.path.join(OUTPUT_DIR, "fetch_cache.xml")
EPG_PATH = os.path.join(OUTPUT_DIR, "epg.xml")
EPG_OLD = os.path.join(OUTPUT_DIR, "epg_old.xml")

DATE_FMT = "%Y%m%d%H%M%S %z"  # XMLTV standard time format

# Matches invalid XML characters (non-printable, except newline and tab)
INVALID_XML_CHARS = re.compile(
    r"[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]"
)

def clean_xml(content: str) -> str:
    return INVALID_XML_CHARS.sub("_", content)

def download_epg():
    print("Downloading EPG...")
    if EPG_URL is None:
        raise Exception("EPG_URL is None")
    r = requests.get(EPG_URL)
    r.raise_for_status()
    raw_text = r.content.decode("utf-8", errors="replace")
    cleaned = clean_xml(raw_text)
    with open(FETCH_CACHE, "w", encoding="utf-8") as f:
        f.write(cleaned)

def backup_old_epg() -> None:
    if os.path.exists(EPG_PATH):
        shutil.copy(EPG_PATH, EPG_OLD)
    else:
        open(EPG_OLD, "w").close()

def get_tree(path):
    """Load and return an ElementTree object from a file."""
    try:
        tree = ET.parse(path)
        return tree
    except ET.ParseError as e:
        print(f"Error parsing XML file {path}: {e}")
        return None

def create_empty_tree(tag:str = "root"):
    """Create an empty tree with a root element."""
    root = ET.Element(tag)  # Create a root element for the empty tree
    return ET.ElementTree(root)

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
        
        if existing.attrib.get(existing_key) != element.attrib.get(existing_key):
            return False
        
    return True

def element_exists_in_tree(element, tree):
    """Check if the element already exists in tree based on a unique identifier or tag."""
    root = tree.getroot()
    # Example: Check if an element with the same 'id' or 'name' exists in the tree
    for existing_elem in root.findall(element.tag):
        if attributes_match(existing_elem, element):  # Adjust based on the unique field
            return True
    return False

def get_datetime(stop_str):
    try:
        return datetime.strptime(stop_str, DATE_FMT)
    except Exception:
        return None

def is_old_programme(programme):
    one_day_ago = datetime.now().astimezone() - timedelta(days=1)
    stop = get_datetime(programme.attrib.get("stop", ""))
    print(stop, one_day_ago)
    return stop and stop < one_day_ago

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
                if epg_old_elem.tag == "programme" and is_old_programme(epg_old_elem):
                    print("Is programme and is old, skipping...")
                    continue
                print(f"Element {epg_old_elem.tag} is missing, adding...")
                epg_out_root.append(epg_old_elem)

    return epg_out_tree

def write_epg(tree):
    tree.write(EPG_PATH, encoding="utf-8", xml_declaration=True)


def main():
    # Step 1: Download and cache
    download_epg()

    # Step 2: Backup old EPG if exists
    backup_old_epg()

    #Step 3: Merge EPGs
    epg_out = merge_epgs()

    # Step 4: Write EPG
    write_epg(epg_out)

    print("DONE")

if __name__ == "__main__":
    INTERVAL = int(os.getenv("INTERVAL", 3600))
    while True:
        main()
        time.sleep(INTERVAL)
