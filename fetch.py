import argparse
import arrow
import ConfigParser
import datetime
import hashlib
import jinja2
import json
import logging
import lxml.etree
import os
import requests
import sys

from models import Alert

try:
    requests.packages.urllib3.disable_warnings()
except AttributeError:
    pass

ATOM_NS = "{http://www.w3.org/2005/Atom}"
CAP_NS = "{urn:oasis:names:tc:emergency:cap:1.1}"


class Parser(object):
    """ A convenience object to hold our functionality """


    def __init__(self, pushover_token, pushover_user, directory):
        self.pushover_token = pushover_token
        self.pushover_user = pushover_user
        self.current_dir = directory
        self.counties = None
        self.fips_watch_list = None
        self.ugc_watch_list = None


    def create_alert_title(self, p_alert):
        """ Formats the title for an alert message """
        # The push notification title should be like:
        # 'Arapahoe County (CO) Weather Alert'
        # The message is the title with the last characters
        # of the identifier added.
        msg_title = '%s (%s) Weather Alert' % (p_alert.county, p_alert.state)
        return msg_title


    def create_alert_message(self, p_alert):
        """ Creates the message body for the alert message """

        # If there are details, we append those into the title. This only
        # happens when it's a generic "Special Weather Statement" and helps
        # add context to the alert.
        if p_alert.details:
            title = p_alert.title.replace('issued', '(%s) issued' % p_alert.details)
        else:
            title = p_alert.title

        message = '%s (%s)' % (title, p_alert.alert_id[-5:])
        return message


    def send_pushover_alert(self, id, title, message, url):
        """ Sends an alert via Pushover API """
        api_url = 'https://api.pushover.net:443/1/messages.json'
        request = requests.post(api_url, data={
            "title": title,
            "token": self.pushover_token,
            "user": self.pushover_user,
            "message": message,
            "sound": "falling",
            "url": 'http://wxalerts.org/alerts/%s.html' % id,
        }, verify=False)

        if not request.ok:
            logger.error("Error sending push: %s\n" % request.text)
        else:
            logger.info("Sent push: %s" % title)


    def check_new_alerts(self, created_ts):
        """ Looks at the alerts created this run for ones we care about """

        # Keep track of alerts that match our watched counties
        matched_alerts = []

        # Iterate over the alerts in the latest run
        for alert_record in Alert.select().where(Alert.created == created_ts):

            # Get all the UGC codes for the alert
            if alert_record.ugc_codes:
                ugc_codes = alert_record.ugc_codes.split(',')
            else:
                ugc_codes = []

            # Get all the FIPS codes for the alert
            if alert_record.fips_codes:
                fips_codes = alert_record.fips_codes.split(',')
            else:
                fips_codes = []

            # Compare the fips and ugc codes to see if they overlap. If they
            # do, then we have a match
            ugc_match = set(ugc_codes).intersection(self.ugc_watch_list)
            fips_match = set(fips_codes).intersection(self.fips_watch_list)

            matched_counties = []

            # See if any of the UGC codes match our target counties
            for ugc_code in ugc_match:
                matched_counties = [county for county in self.counties if county['ugc'] == ugc_code]

            # See if any of the FIPS codes match our target counties
            for fips_code in fips_match:
                matched_counties = [county for county in self.counties if county['fips'] == fips_code]

            if len(matched_counties) > 0:
                # Because the counties we check are very far apart, the matched
                # counties should never be more than one. We only care about the
                # first, so just assign it.
                alert_record.county = matched_counties[0]['name']
                alert_record.state = matched_counties[0]['state']
                matched_alerts.append(alert_record)

        return matched_alerts


    def details_for_alert(self, alert):
        """ Fetches the NOAA detail XML feed for an alert and saves the description """
        logger.info('Fetching Detail Link for Alert %s' % alert.alert_id)
        tree = lxml.etree.parse(alert.api_url)

        info_el = tree.find(CAP_NS + 'info')
        headline = info_el.find(CAP_NS + 'headline').text
        event = info_el.find(CAP_NS + 'event').text
        issuer = info_el.find(CAP_NS + 'senderName').text
        description = info_el.find(CAP_NS + 'description').text
        instructions = info_el.find(CAP_NS + 'instruction').text

        area_el = info_el.find(CAP_NS + 'area')
        area = area_el.find(CAP_NS + 'areaDesc').text
        
        return {
            'headline': headline, 
            'event': event, 
            'issuer': issuer, 
            'description': description, 
            'instructions': instructions, 
            'area': area,
        }


    def fetch(self, run_timestamp):
        """ Fetches the NOAA alerts XML feed and inserts into database """

        # Create an XML doc from the URL contents
        logger.info('Fetching Alerts Feed')
        tree = lxml.etree.parse('http://alerts.weather.gov/cap/us.php?x=1')

        # Keep track of how many alerts we create
        total_count = 0
        insert_count = 0
        existing_count = 0

        for entry_el in tree.findall(ATOM_NS + 'entry'):

            total_count += 1

            alert_id = hashlib.sha224(entry_el.find(ATOM_NS + 'id').text).hexdigest()
            title = entry_el.find(ATOM_NS + 'title').text
            event = entry_el.find(CAP_NS + 'event').text
            expires_dt = arrow.get(entry_el.find(CAP_NS + 'expires').text)
            url = entry_el.find(ATOM_NS + 'link').attrib['href']
            api_url = entry_el.find(ATOM_NS + 'id').text
            
            # Calculate the expiration timetamp
            expires = expires_dt.isoformat()
            expires_utc_ts = int(expires_dt.to('UTC').timestamp)

            fips_list = []
            ugc_list = []

            # Get the FIPS and UGC codes that this alert applies to
            geocode_el = entry_el.find(CAP_NS + 'geocode')

            if geocode_el is not None:
                for value_name_el in geocode_el.findall(ATOM_NS + 'valueName'):
                    if value_name_el.text == 'FIPS6':
                        fips_el = value_name_el.getnext()
                        if fips_el is not None and fips_el.text is not None:
                            fips_list = fips_el.text.split(' ')
                    elif value_name_el.text == 'UGC':
                        ugc_el = value_name_el.getnext()
                        if ugc_el is not None and ugc_el.text is not None:
                            ugc_list = ugc_el.text.split(' ')

            # If it's a special or severe weather statement, look inside it to see
            # if we can extract any keywords. We'll store these separately but put them
            # in any push messages we send out.
            sub_events = []
            if event in ('Severe Weather Statement', 'Special Weather Statement'):
                summary = entry_el.find(ATOM_NS + 'summary').text.upper()
                for item in ('Thunderstorm', 'Strong Storm', 'Wind', 'Rain', 'Hail', 'Tornado', 'Flood'):
                    if item.upper() in summary:
                        sub_events.append(item)

            # Concatenate the sub events (if any) into a detail string
            detail = ', '.join(sub_events)

            # See if this alert exists. If it does, don't do anything since
            # we don't update existing alerts. (NOAA doesn't do this I think?)
            try:
                alert_record = Alert.get(Alert.alert_id == alert_id)
                existing_count += 1
            except Exception, _:
                insert_count += 1
                alert_record = Alert.create(
                    alert_id=alert_id,
                    title=title,
                    event=event,
                    details=detail,
                    description = None,
                    expires=expires,
                    expires_utc_ts=expires_utc_ts,
                    url=url,
                    api_url=api_url,
                    fips_codes=','.join(fips_list),
                    ugc_codes=','.join(ugc_list),
                    created=run_timestamp,
                )

        # Log our totals
        logger.debug("Found %d alerts in feed." % total_count)
        logger.info("Inserted %d new alerts." % insert_count)
        logger.debug("Matched %d existing alerts." % existing_count)


if __name__ == '__main__':

    # Parse the command-line arguments
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--purge', dest='purge', action='store_true')
    argparser.set_defaults(purge=False)
    argparser.add_argument('--nopush', dest='nopush', action='store_true')
    argparser.set_defaults(nopush=False)
    argparser.add_argument('--debug', dest='debug', action='store_true')
    argparser.set_defaults(debug=False)
    args = vars(argparser.parse_args())

    # Set up logger
    logging.basicConfig(filename='log.txt', level=logging.INFO, format='%(asctime)s  %(message)s')
    logging.Formatter(fmt='%(asctime)s', datefmt='%Y-%m-%d,%H:%M:%S')
    logger = logging.getLogger(__name__)

    # Set up the template engine
    template_loader = jinja2.FileSystemLoader('./templates')
    template_env = jinja2.Environment(loader=template_loader)
    template_file = "detail.html"
    template = template_env.get_template(template_file)

    # Make sure the requests library only logs errors
    logging.getLogger("requests").setLevel(logging.ERROR)

    # Log debug-level statements if we are in debugging mode
    if args['debug']:
        logger.level = logging.DEBUG

    # Make sure we can load our files regardless of where the script is called from
    CUR_DIR = os.path.dirname(os.path.realpath(__file__))

    # Set up the output directory
    OUTPUT_DIR = os.path.join(CUR_DIR, 'output')
    if not os.path.exists(OUTPUT_DIR):
        sys.exit('Error! Output directory does not exist.')

    # Load the configuration
    config = ConfigParser.ConfigParser()
    config_filepath = os.path.join(CUR_DIR, 'config.txt')
    config.read(config_filepath)

    # Get the list of events that we don't want to be alerted about
    try:
        ignored_events = config.get('events', 'ignored').split(',')
    except ConfigParser.NoSectionError:
        ignored_events = []

    # Instantiate our parser object
    PUSHOVER_TOKEN = config.get('pushover', 'token')
    PUSHOVER_USER = config.get('pushover', 'user')
    parser = Parser(PUSHOVER_TOKEN, PUSHOVER_USER, CUR_DIR)

    # Load the counties we want to monitor
    counties_filepath = os.path.join(CUR_DIR, 'counties.json')
    with open(counties_filepath, 'r') as f:
        parser.counties = json.loads(f.read())

    # Assign the fips and ugc codes to watch for
    parser.fips_watch_list = [str(c['fips']) for c in parser.counties]
    parser.ugc_watch_list = [str(c['ugc']) for c in parser.counties]

    # If we got a command-line flag to purge the saved alerts, do
    # that before we fetch new alerts. If we didn't get the purge command,
    # delete any alerts that are now expired.
    if args['purge']:
        Alert.delete().execute()
    else:
        ago_ts = arrow.utcnow().replace(days=-1).timestamp
        count = Alert.delete().where(Alert.expires_utc_ts < ago_ts).execute()
        logger.debug("Deleted %d expired alerts." % count)

    # Create a timestamp that will act as a numeric identifier for
    # this fetching run. We'll use this later to see if a record
    # has been added in this run
    run_ts = arrow.utcnow().timestamp

    # Go grab the current alerts and process them
    parser.fetch(run_ts)

    # Find any new alerts that match our counties
    for alert in parser.check_new_alerts(run_ts):

        # See if they are in the list of alerts to ignore
        if alert.event not in ignored_events:

            # Get the details about the alert from the API
            details = parser.details_for_alert(alert)

            # Render the detail page
            output = template.render({'alert': details , 'expires': int(alert.expires_utc_ts) })
            detail_filepath = os.path.join(OUTPUT_DIR, '%s.html' % alert.alert_id)
            with open(detail_filepath, 'w') as f:
                f.write(output)

            # Construct the title and message body for the alert
            alert_title = parser.create_alert_title(alert)
            alert_msg = parser.create_alert_message(alert)
            alert_id = alert.alert_id
            logger.info('Alert to send: %s' % alert_title)

            # Check the argument to see if we should be sending the push
            if not args['nopush']:
                parser.send_pushover_alert(alert_id, alert_title, alert_msg, alert.url)
            else:
                logger.info('Sending pushes disabled by argument')

        else:
            logger.info('Ignoring %s, %s alert for %s' % (alert.county, alert.state, alert.event))
