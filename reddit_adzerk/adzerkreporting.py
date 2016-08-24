"""
Generate and processes daily/lifetime reports for ad campaigns.

Runs link reports by day, by campaign, and stores in traffic db.
Runs campaign reports for lifetime impression, clicks, and spend.

A cron job is used to queue reports for promos that are currently
serving, or served the day before.  The queue blocks until a
report can be retrived until moving on to the next item.  If a
report is pending for more than `adzerk_reporting_timeout` it is
assumed to have failed and is generated.
"""

import itertools
import json
import math
import pytz
import time
from collections import defaultdict
from datetime import datetime, timedelta

from dateutil.parser import parse as parse_date
from pylons import app_globals as g
from sqlalchemy.orm import scoped_session, sessionmaker

from r2.lib import (
    amqp,
    promote,
)
from r2.models import (
    Link,
    PromoCampaign,
)
from r2.models.traffic import (
    engine,
    AdserverClickthroughsByCodename,
    AdserverImpressionsByCodename,
    AdserverSpentPenniesByCodename,
    AdserverTargetedClickthroughsByCodename,
    AdserverTargetedImpressionsByCodename,
    AdserverTargetedSpentPenniesByCodename,
)

from reddit_adzerk import (
    adzerk_api,
    report,
)

RETRY_SLEEP_SECONDS = 3

Session = scoped_session(sessionmaker(bind=engine))


def queue_promo_reports():
    """
    Queue reports for promos that are currently
    serving, or served the day before.
    """
    prev_promos = promote.get_served_promos(offset=-1)
    promos = promote.get_served_promos(offset=0)

    campaigns = set()
    campaigns_by_link = defaultdict(set)

    for campaign, link in itertools.chain(prev_promos, promos):
        campaigns.add(campaign)
        campaigns_by_link[link].add(campaign)

    links = [(link, link_campaigns) for link, link_campaigns in campaigns_by_link.items()]

    # sort and group links together in `adzerk_reporting_link_group_size` sized groups
    def sort_links(items):
        link, link_campaigns = items
        start, end = _get_campaigns_date_range(link_campaigns)

        return start

    links = sorted(links, key=sort_links)
    link_groups = defaultdict(lambda: defaultdict(list))

    for i, items in enumerate(links):
        link, link_campaigns = items
        group = i / g.live_config.get("adzerk_reporting_link_group_size", 50)
        link_groups[group]["links"].append(link)
        link_groups[group]["campaigns"] = link_groups[group]["campaigns"] + list(link_campaigns)

    # sort and group campaigns together in `adzerk_reporting_campaign_group_size` sized groups
    campaigns = sorted(campaigns, key=lambda c: c.start_date)
    campaigns_groups = defaultdict(list)

    for i, campaign in enumerate(campaigns):
        group = i / g.live_config.get("adzerk_reporting_campaign_group_size", 100)
        campaigns_groups[group].append(campaign)

    for group, items in campaigns_groups.items():
        _generate_promo_reports(items)

    for group, items in link_groups.items():
        _generate_link_reports(items)

    amqp.worker.join()


def _generate_link_reports(items):
    links = items["links"]
    campaigns = items["campaigns"]

    g.log.info("queuing report for link %s" % ",".join(l._fullname for l in links))
    amqp.add_item("adzerk_reporting_q", json.dumps({
        "action": "generate_daily_link_reports",
        "link_ids": [l._id for l in links],
        "campaign_ids": [c._id for c in campaigns],
    }))


def _generate_promo_reports(campaigns):
    g.log.info("queuing report for campaigns %s" % ",".join(c._fullname for c in campaigns))
    amqp.add_item("adzerk_reporting_q", json.dumps({
        "action": "generate_lifetime_campaign_reports",
        "campaign_ids": [c._id for c in campaigns],
    }))


def _get_campaigns_date_range(campaigns):
    start = min(promo.start_date for promo in campaigns)
    end = max(promo.end_date for promo in campaigns)

    return (start, end)

def _normalize_usage(impressions, clicks, spent):
    # adzerk processes clicks faster than impressions
    # throw away results that are obviously wrong.
    if clicks > impressions:
        impressions = 0
        clicks = 0
        spent = 0

    return (impressions, clicks, spent)


def _get_total_impressions(report_fragment):
    return report_fragment.get("TotalImpressions", 0)


def _get_total_clicks(report_fragment):
    return report_fragment.get("TotalUniqueBucketClicks", 0)


def _get_total_spent(report_fragment):
    return report_fragment.get("TotalTrueRevenue", 0)


def _get_total_usage(report_fragment):
    impressions = _get_total_impressions(report_fragment)
    clicks = _get_total_clicks(report_fragment)
    spent = _get_total_spent(report_fragment)

    return _normalize_usage(impressions, clicks, spent)


def _get_impressions(report_fragment):
    return report_fragment.get("Impressions", 0)


def _get_clicks(report_fragment):
    return report_fragment.get("UniqueBucketClicks", 0)


def _get_spent(report_fragment):
    return report_fragment.get("TrueRevenue", 0)


def _get_usage(report_fragment):
    impressions = _get_impressions(report_fragment)
    clicks = _get_clicks(report_fragment)
    spent = _get_spent(report_fragment)

    return _normalize_usage(impressions, clicks, spent)

def _get_date(report_fragment):
    date = report_fragment.get("Date")

    if not date:
        return None

    return parse_date(date)


def _get_fullname(cls, report_fragment):
    fullname = report_fragment.get("Title", "")

    if not fullname.startswith(cls._fullname_prefix):
        return None
    else:
        return fullname


def _get_campaign_id(report_fragment):
    return report_fragment.get("Grouping", {}).get("CampaignId", None)


def _get_flight_id(report_fragment):
    return report_fragment.get("Grouping", {}).get("OptionId", None)


def _handle_generate_daily_link_reports(link_ids, campaign_ids):
    now = datetime.utcnow()
    links = Link._byID(link_ids, data=True, return_dict=False)
    campaigns = PromoCampaign._byID(campaign_ids, data=True, return_dict=False)

    if not campaigns:
        return

    links_start, links_end = _get_campaigns_date_range(campaigns)
    now = now.replace(tzinfo=pytz.utc)
    links_start = links_start.replace(tzinfo=pytz.utc)
    links_end = links_end.replace(tzinfo=pytz.utc)

    # if data has already been processed then there's no need
    # to redo it.  use the last time the report was run as a 
    # starting point, but subtract 24hrs since initial numbers
    # are preliminary.
    last_run = min(getattr(l, "last_daily_report_run", links_start) for l in links)
    start = max(
        last_run - timedelta(hours=24),
        links_start,
    )

    # in cases where we may be running a report well after a link
    # has completed ensure we always use the actual start.
    if start > links_end:
        start = links_start

    end = min([now, links_end])

    link_fullnames = ",".join([l._fullname for l in links])
    g.log.info("generating report for link %s (%s-%s)" % (
        link_fullnames, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))

    report_id = report.queue_report(
        start=start,
        end=end,
        groups=["optionId", "day"],
        parameters=[{
            "campaignId": l.external_campaign_id,
        } for l in links],
    )

    g.log.info("processing report for link (%s/%s)" %
        (link_fullnames, report_id))

    try:
        _process_daily_link_reports(
            links=links,
            report_id=report_id,
            queued_date=now,
        )

        g.log.info("successfully processed report for link (%s/%s)" %
            (link_fullnames, report_id))
    except report.ReportFailedException as e:
        g.log.error(e)
        # retry if report failed
        _generate_link_reports(links)


def _handle_generate_lifetime_campaign_reports(campaign_ids):
    now = datetime.utcnow()
    campaigns = PromoCampaign._byID(campaign_ids, data=True, return_dict=False)
    start = min(c.start_date for c in campaigns).replace(tzinfo=pytz.utc)
    end = max(c.end_date for c in campaigns).replace(tzinfo=pytz.utc)
    now = now.replace(tzinfo=pytz.utc)

    end = min([now, end])

    campaign_fullnames = ",".join(c._fullname for c in campaigns)

    g.log.info("generating report for campaigns %s (%s-%s)" % (
        campaign_fullnames, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')))

    report_id = report.queue_report(
        start=start,
        end=end,
        groups=["optionId"],
        parameters=[{
            "flightId": c.external_flight_id,
        } for c in campaigns],
    )

    try:
        _process_lifetime_campaign_reports(
            campaigns=campaigns,
            report_id=report_id,
            queued_date=now,
        )

        g.log.info("successfully processed report for campaigns (%s/%s)" %
            (campaign_fullnames, report_id))
    except report.ReportFailedException as e:
        g.log.error(e)
        # retry if report failed
        _generate_promo_reports(campaigns)


def _process_lifetime_campaign_reports(campaigns, report_id, queued_date):
    """
    Processes report for the lifetime of the campaigns.

    Exponentially backs off on retries, throws on timeout.
    """

    attempt = 1
    campaign_fullnames = ",".join(c._fullname for c in campaigns)

    while True:
        try:
            report_result = report.fetch_report(report_id)
            break
        except report.ReportPendingException as e:
            timeout = (datetime.utcnow().replace(tzinfo=pytz.utc) -
                timedelta(seconds=g.live_config.get("adzerk_reporting_timeout", 500)))

            if queued_date < timeout:
                raise report.ReportFailedException("campign reports timed out (%s/%s)" %
                    (campaign_fullnames, report_id))
            else:
                sleep_time = math.pow(RETRY_SLEEP_SECONDS, attempt)
                attempt = attempt + 1

                g.log.warning("campaign reports still pending, retrying in %d seconds (%s/%s)" %
                    (sleep_time, campaign_fullnames, report_id))

                time.sleep(sleep_time)

    report_records = report_result.get("Records", None)
    campaigns_by_fullname = {c._fullname: c for c in campaigns}

    if report_records:
        for detail in report_records[0].get("Details", []):
            campaign_fullname = _get_fullname(PromoCampaign, detail)

            if not campaign_fullname:
                flight_id = _get_flight_id(detail)
                g.log.error("invalid fullname for campaign (%s/%s)" %
                    (campaign_fullname, flight_id))
                continue

            campaign = campaigns_by_fullname.get(campaign_fullname)

            if not campaign:
                flight_id = _get_flight_id(detail)
                g.log.warning("no campaign for flight (%s/%s)" %
                    (campaign_fullname, flight_id))
                continue

            impressions, clicks, spent = _get_usage(detail)

            campaign.adserver_spent_pennies = int(spent * 100)
            campaign.adserver_impressions = impressions
            campaign.adserver_clicks = clicks
            campaign.last_lifetime_report = report_id
            campaign.last_lifetime_report_run = queued_date

            campaign._commit()


def _reporting_factory():
    return dict(
        impressions=0,
        clicks=0,
        spent_pennies=0,
    )

def _process_daily_link_reports(links, report_id, queued_date):
    """
    Processes report grouped by day and flight.

    Exponentially backs off on retries, throws on timeout.
    """

    link_fullnames = ",".join([l._fullname for l in links])
    attempt = 1

    while True:
        try:
            report_result = report.fetch_report(report_id)
            break
        except report.ReportPendingException as e:
            timeout = (datetime.utcnow().replace(tzinfo=pytz.utc) -
                timedelta(seconds=g.live_config.get("adzerk_reporting_timeout", 500)))

            if queued_date < timeout:
                raise report.ReportFailedException("link reports timed out (%s/%s)" %
                    (link_fullnames, report_id))
            else:
                sleep_time = math.pow(RETRY_SLEEP_SECONDS, attempt)
                attempt = attempt + 1

                g.log.warning("link reports still pending, retrying in %d seconds (%s/%s)" %
                    (sleep_time, link_fullnames, report_id))

                time.sleep(sleep_time)

    g.log.debug(report_result)

    link_ids = [l._id for l in links]
    campaigns = list(PromoCampaign._query(PromoCampaign.c.link_id.in_(link_ids)))
    campaigns_by_fullname = {c._fullname: c for c in campaigns}
    links_by_id = { l._id: l for l in links}

    # report is by date, by flight. each record is a day (not grouped by campaign)
    # and each detail is a flight for that day.
    for record in report_result.get("Records", []):
        date = _get_date(record)

        link_details = defaultdict(_reporting_factory)
        campaign_details = defaultdict(_reporting_factory)
        for detail in record.get("Details", []):
            campaign_fullname = _get_fullname(PromoCampaign, detail)

            if not campaign_fullname:
                flight_id = _get_flight_id(detail)
                g.log.error("invalid fullname for campaign (%s/%s)" %
                    (campaign_fullname, flight_id))
                continue

            campaign = campaigns_by_fullname.get(campaign_fullname)
            link = links_by_id[campaign.link_id]

            if not campaign:
                flight_id = _get_flight_id(detail)
                g.log.warning("no campaign for flight (%s/%s)" %
                    (campaign_fullname, flight_id))
                continue

            impressions, clicks, spent = _get_usage(detail)

            # if the price changes then there may be multiple records for each campaign/date.
            campaign_values = campaign_details[(campaign, date)]
            campaign_values["impressions"] = campaign_values["impressions"] + impressions
            campaign_values["clicks"] = campaign_values["clicks"] + clicks
            campaign_values["spent_pennies"] = campaign_values["spent_pennies"] + (spent * 100.)

            link_values = link_details[(link, date)]
            link_values["impressions"] = link_values["impressions"] + impressions
            link_values["clicks"] = link_values["clicks"] + clicks
            link_values["spent_pennies"] = link_values["spent_pennies"] + (spent * 100.)

        for (campaign, date), values in campaign_details.iteritems():
            # hack around `target_name`s for multi subreddit collections
            # being overly long.
            if (campaign.target.is_collection and
                    "/r/" in campaign.target.pretty_name):

                subreddit = "multi_%s" % PromoCampaign.SUBREDDIT_TARGET
            else:
                subreddit = campaign.target_name

            _insert_daily_campaign_reporting(
                codename=campaign._fullname,
                date=date,
                subreddit=subreddit,
                **values
            )

        for (link, date), values in link_details.iteritems():
            _insert_daily_link_reporting(
                codename=link._fullname,
                date=date,
                **values
            )

    link.last_daily_report = report_id
    link.last_daily_report_run = queued_date
    link._commit()


def process_report_q():
    @g.stats.amqp_processor('adzerk_reporting_q')
    def _processor(message):
        data = json.loads(message.body)
        action = data.get("action")

        if action == "generate_daily_link_reports":
            _handle_generate_daily_link_reports(
                link_ids=data.get("link_ids"),
                campaign_ids=data.get("campaign_ids"),
            )
        elif action == "generate_lifetime_campaign_reports":
            _handle_generate_lifetime_campaign_reports(
                campaign_ids=data.get("campaign_ids"),
            )
        else:
            g.log.warning("adzerk_reporting_q: unknown action - \"%s\"" % action)

    amqp.consume_items("adzerk_reporting_q", _processor, verbose=False)


def _insert_daily_link_reporting(
        codename, date, impressions,
        clicks, spent_pennies):

    date = date.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=None,
    )
    clicks_row = AdserverClickthroughsByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=clicks,
        pageview_count=clicks,
    )

    impressions_row = AdserverImpressionsByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=impressions,
        pageview_count=impressions,
    )

    spent_row = AdserverSpentPenniesByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=spent_pennies,
        pageview_count=spent_pennies,
    )

    Session.merge(clicks_row)
    Session.merge(impressions_row)
    Session.merge(spent_row)
    Session.commit()


def _insert_daily_campaign_reporting(
        codename, date, impressions,
        clicks, spent_pennies, subreddit=None):

    date = date.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=None,
    )
    clicks_row = AdserverTargetedClickthroughsByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=clicks,
        pageview_count=clicks,
        subreddit=subreddit,
    )

    impressions_row = AdserverTargetedImpressionsByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=impressions,
        pageview_count=impressions,
        subreddit=subreddit,
    )

    spent_row = AdserverTargetedSpentPenniesByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=spent_pennies,
        pageview_count=spent_pennies,
        subreddit=subreddit,
    )

    Session.merge(clicks_row)
    Session.merge(impressions_row)
    Session.merge(spent_row)
    Session.commit()
