import json
import pkg_resources

from pylons import app_globals as g
from pylons.i18n import N_

from r2.lib.plugin import Plugin
from r2.lib.configparse import ConfigValue
from r2.lib.js import Module


class Adzerk(Plugin):
    needs_static_build = True

    errors = {
        "INVALID_SITE_PATH":
            N_("invalid site path/name"),
    }

    config = {
        ConfigValue.str: [
            'adzerk_engine_domain',
        ],

        ConfigValue.int: [
            'az_selfserve_salesperson_id',
            'az_selfserve_network_id',
        ],

        ConfigValue.float: [
            'display_ad_skip_probability',
        ],

        ConfigValue.tuple: [
            'display_ad_skip_keywords',
        ],

        ConfigValue.dict(ConfigValue.str, ConfigValue.int): [
            'az_selfserve_priorities',
            'az_selfserve_site_ids',
            'adzerk_zones',
        ],

        ConfigValue.tuple_of(ConfigValue.int): [
            'blank_campaign_ids',
        ],
    }

    live_config = {

        ConfigValue.float: [
            'events_collector_ad_serving_sample_rate',
            'ad_log_sample_rate',
        ],

        ConfigValue.int: [
            'adzerk_reporting_link_group_size',
            'adzerk_reporting_campaign_group_size',
            'adzerk_reporting_timeout',
        ],

    }

    js = {
        'reddit-init': Module('reddit-init.js',
            'adzerk/adzerk.js',
        ),

        'display': Module('display.js',
            'lib/json2.js',
            'custom-event.js',
            'do-not-track.js',
            'frames.js',
            'adzerk/base64.js',
            'adzerk/display.js',
        ),

        'companion': Module('companion.js',
            'adzerk/companion.js',
        ),

        'ad-dependencies': Module('ad-dependencies.js',
            'adzerk/jquery.js',
        ),
    }

    def add_routes(self, mc):
        mc('/api/request_promo/', controller='adzerkapi', action='request_promo')

    def declare_queues(self, queues):
        from r2.config.queues import MessageQueue
        queues.declare({
            "adzerk_q": MessageQueue(bind_to_self=True),
            "adzerk_reporting_q": MessageQueue(bind_to_self=True),
        })

    def load_controllers(self):
        from lib.events import AdEventQueue

        g.ad_events = AdEventQueue()

        # replace standard adserver with Adzerk.
        from adzerkpromote import AdzerkApiController
        from adzerkpromote import hooks as adzerkpromote_hooks
        adzerkpromote_hooks.register_all()
