from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import re
import requests
import os
import shutil


from constants import (
    DATE_FMT
)

def write_epg(tree, path):
    tree.write(path, encoding="utf-8", xml_declaration=True)


INVALID_XML_CHARS = re.compile(
    r"[^\x09\x0A\x0D\x20-\uD7FF\uE000-\uFFFD]"
)

def clean_xml(content: str) -> str:
    return INVALID_XML_CHARS.sub("_", content)



def download_epg(epg_url, path):
    print(f"Downloading EPG from '{epg_url}' ...")
    if epg_url is None:
        raise Exception("epg_url is None")
    r = requests.get(epg_url)
    r.raise_for_status()
    raw_text = r.content.decode("utf-8", errors="replace")
    cleaned = clean_xml(raw_text)
    with open(path, "w", encoding="utf-8") as f:
        f.write(cleaned)

def download_epg_tree(epg_url):
    print(f"Downloading EPG from '{epg_url}' ...")
    if epg_url is None:
        raise Exception("epg_url is None")
    r = requests.get(epg_url)
    r.raise_for_status()
    raw_text = r.content.decode("utf-8", errors="replace")
    cleaned = clean_xml(raw_text)

    return get_tree_from_string(cleaned)


def backup_old_epg(current, old) -> None:
    if os.path.exists(current):
        shutil.copy(current, old)
    else:
        open(old, "w").close()

def get_tree_from_string(string):
    try:
        tree = ET.ElementTree(ET.fromstring(string))
        return tree
    except ET.ParseError as e:
        print(f"Error parsing XML string: {e}")
        return None

def get_tree(path):
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

def get_datetime(stop_str):
    try:
        return datetime.strptime(stop_str, DATE_FMT)
    except Exception:
        return None

def round_time_to_nearest_five(dt):
    return _round_time_to_nearest_five(get_datetime(dt))

def _round_time_to_nearest_five(dt):
    if dt is None:
        return None
    discard = timedelta(minutes=dt.minute % 5,
                        seconds=dt.second,
                        microseconds=dt.microsecond)
    dt -= discard
    if discard >= timedelta(minutes=2.5):
        dt += timedelta(minutes=5)
    return dt


def is_old_programme(programme):
    one_day_ago = datetime.now().astimezone() - timedelta(days=1)
    stop = get_datetime(programme.attrib.get("stop", ""))
    return stop and stop < one_day_ago


def is_same_channel(a, b) -> bool:
    return a.attrib.get("channel") == b.attrib.get("channel")

def is_overlap(a, b):
    programme_start, programme_stop, start_a, stop_b = (a.attrib.get("start"), a.attrib.get("stop"), b.attrib.get("start"), b.attrib.get("stop"))
    programme_start = datetime.strptime(programme_start, DATE_FMT)
    programme_stop = datetime.strptime(programme_stop, DATE_FMT)
    start_a = datetime.strptime(start_a, DATE_FMT)
    stop_b = datetime.strptime(stop_b, DATE_FMT)

    return (programme_start < stop_b and programme_stop > start_a)

def attributes_match(existing, element) -> bool:
    existing_keys = existing.attrib.keys()
    element_keys = element.attrib.keys()

    if len(existing_keys) != len(element_keys):
        return False

    keys_programme = ["id"]
    keys_channel = ["channel", "start", "stop"]



    # Programme check
    if all([key in keys_programme for key in existing_keys]) and all([key in keys_programme for key in element_keys]):
        for existing_key in existing_keys:
            if existing_key not in element_keys:
                return False
            
            if existing.attrib.get(existing_key) != element.attrib.get(existing_key):
                return False
            
        return True



    # Channel check
    if not all([key in keys_channel for key in existing_keys]) or not all([key in keys_channel for key in element_keys]):
        return False

    for existing_key in existing_keys:
        if existing_key not in element_keys:
            return False
        
    if not is_same_channel(existing, element):
        return False
    
    return is_overlap(existing, element)


def element_exists_in_tree(element, tree):
    """Check if the element already exists in tree based on a unique identifier or tag."""
    root = tree.getroot()
    # Example: Check if an element with the same 'id' or 'name' exists in the tree
    for existing_elem in root.findall(element.tag):
        if attributes_match(existing_elem, element):  # Adjust based on the unique field
            return True
    return False


def merge_epgs(main, seccond):
    main_tree = get_tree(main)
    main_tree_root = main_tree.getroot() if main_tree else None
    seccond_tree = get_tree(seccond)
    seccond_tree_root = seccond_tree.getroot() if seccond_tree else None

    if seccond_tree is not None:
        for seccond_tree_elem in seccond_tree_root:
            if element_exists_in_tree(seccond_tree_elem, main_tree):
                if seccond_tree_elem.tag == "programme" and is_old_programme(seccond_tree_elem):
                    print("Is programme and is old, skipping...")
                    continue
                main_tree_root.append(seccond_tree_elem)

    return main_tree