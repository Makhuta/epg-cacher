from constants import (
    FETCH_CACHE_VBOX,
    EPG_VBOX_PATH,
    EPG_VBOX_OLD,
)

from functions import (
    write_epg,
    download_epg,
    backup_old_epg,
    merge_epgs,
)

def run(url):
    # Download data from vbox
    download_epg(url, FETCH_CACHE_VBOX)

    # Backup old EPG data
    backup_old_epg(EPG_VBOX_PATH, EPG_VBOX_OLD)


    merged_epgs = merge_epgs(FETCH_CACHE_VBOX, EPG_VBOX_OLD)

    write_epg(merged_epgs, EPG_VBOX_PATH)

    print("VBox done")