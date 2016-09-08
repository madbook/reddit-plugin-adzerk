from pylons import app_globals as g

def get_js_config():
    return {
        "network": g.az_selfserve_network_id,
        "site": g.az_selfserve_site_ids["desktop"],
        "priorities": g.az_selfserve_priorities,
        "zones": g.adzerk_zones,
    }
