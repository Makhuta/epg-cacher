from constants import (
    EPG_PATH,
    EPG_OLD,
)

from functions import (
    write_epg,
    download_epg_tree,
    attributes_match,
)

def add_icons(a, b):
    for a_programme in a.getroot().findall("programme"):
        for b_programme in b.getroot().findall("programme"):
            if attributes_match(a_programme, b_programme):
                icons_b = b_programme.findall("icon")
                if len(icons_b) < 1:
                    continue
                icons_a = a_programme.findall("icon")
                if len(icons_a) > 0:
                    continue
                a_programme.append(icons_b[0])

    return a


def run(url_vbox, url_epgshare):
    # Download data from Threadfins
    vbox_tree = download_epg_tree(url_vbox)
    epgshare_tree = download_epg_tree(url_epgshare)

    merged_epgs = add_icons(vbox_tree, epgshare_tree)

    write_epg(merged_epgs, EPG_PATH)

    print("Adding icons done")