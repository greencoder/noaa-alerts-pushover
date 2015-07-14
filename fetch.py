import argparse
import arrow
import ConfigParser
import hashlib
import json
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

# Time between checks (in microseconds)
DELAY = 1200000

class Parser(object):
    """ A convenience object to hold our functionality """

    def __init__(self, pushover_token, pushover_user, directory):
        self.pushover_token = pushover_token
        self.pushover_user = pushover_user
        self.current_dir = directory
        self.counties = None
        self.fips_watch_list = None
        self.ugc_watch_list = None

    def log(self, message):
        """ Logs a message to the console and a log file """
        msg = '%s\t%s\n' % (arrow.utcnow(), message)
        sys.stdout.write(msg)
        with open(os.path.join(self.current_dir, 'log.txt'), 'a') as fhandle:
            fhandle.write(msg)

    def send_alert(self, p_alert):
        """ Sends an alert via Pushover API """

        # The push notification title should be like:
        # 'Arapahoe County (CO) Weather Alert'
        # The message is the title with the last characters
        # of the identifier added.

        msg_title = '%s (%s) Weather Alert' % (p_alert.county, p_alert.state)
        message = '%s (%s)' % (p_alert.title, p_alert.alert_id[-5:])

        self.log('Sending alert: %s' % msg_title)

        # Send out the push notification
        url = 'https://api.pushover.net:443/1/messages.json'
        request = requests.post(url, data={
            "title": msg_title,
            "token": self.pushover_token,
            "user": self.pushover_user,
            "message": message,
            "sound": "falling",
            "url": p_alert.url,
        }, verify=False)

        if not request.ok:
            self.log("Error sending push: %s\n" % request.text)
        else:
            self.log("Sent push: %s" % msg_title)

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

    def fetch(self, run_timestamp):
        """ Fetches the NOAA alerts XML feed and inserts into database """

        # Create an XML doc from the URL contents
        self.log('Fetching Alerts Feed')
        tree = lxml.etree.parse('http://alerts.weather.gov/cap/us.php?x=1')

        for entry_el in tree.findall(ATOM_NS + 'entry'):

            alert_id = hashlib.sha224(entry_el.find(ATOM_NS + 'id').text).hexdigest()
            title = entry_el.find(ATOM_NS + 'title').text
            event = entry_el.find(CAP_NS + 'event').text
            expires = arrow.get(entry_el.find(CAP_NS + 'expires').text).isoformat()
            url = entry_el.find(ATOM_NS + 'link').attrib['href']

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

            # See if this alert exists. If it does, don't do anything since
            # we don't update existing alerts. (NOAA doesn't do this I think?)
            try:
                alert_record = Alert.get(Alert.alert_id == alert_id)
            except Exception, _:
                alert_record = Alert.create(
                    alert_id=alert_id,
                    title=title,
                    event=event,
                    expires=expires,
                    url=url,
                    fips_codes=','.join(fips_list),
                    ugc_codes=','.join(ugc_list),
                    created=run_timestamp,
                )
                alert_record.save()

if __name__ == '__main__':

    # Parse the command-line arguments
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--purge', dest='purge', action='store_true')
    argparser.set_defaults(purge=False)
    args = vars(argparser.parse_args())

    # Make sure we can load our files regardless of where the script is called from
    CUR_DIR = os.path.dirname(os.path.realpath(__file__))

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
        now_ts = arrow.utcnow().timestamp
        Alert.delete().where(Alert.expires < now_ts)

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
            parser.send_alert(alert)
        else:
            parser.log("Ignoring %s, %s alert for %s" % (alert.county, alert.state, alert.event))
