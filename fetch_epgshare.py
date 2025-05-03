from constants import (
    FETCH_CACHE_EPGSHARE,
    EPG_EPGSHARE_PATH,
    EPG_EPGSHARE_OLD,
)

from functions import (
    write_epg,
    download_epg,
    backup_old_epg,
    merge_epgs,
)

def run(url):
    # Download data from EPGShare
    download_epg(url, FETCH_CACHE_EPGSHARE)

    # Backup old EPG data
    backup_old_epg(EPG_EPGSHARE_PATH, EPG_EPGSHARE_OLD)


    merged_epgs = merge_epgs(FETCH_CACHE_EPGSHARE, EPG_EPGSHARE_OLD)

    write_epg(merged_epgs, EPG_EPGSHARE_PATH)

    print("EPGShare done")