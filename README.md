## noaa-alerts-pushover

Sends out NOAA Severe Weather Alerts via Pushover. Any time a new alert is created, you'll get a push.

## Configuration

Open the [counties.json](counties.json) file and add the counties you wish to monitor. You can find them on the [NOAA website](http://www.nws.noaa.gov/emwin/winugc.htm).

Create a file called `config.txt` and format it as follows:
```
[pushover]
token = YOUR_PUSHOVER_TOKEN
user = YOUR_PUSHOVER_USER
````

## Usage

Run the [fetch.py](fetch.py) command to call NOAA and send push notifications for any matching alerts:
```
$ python fetch.py
```

You can add the optional `--purge` argument to clear the database of any saved alerts:
````
$ python fetch.py --purge 
```

*Note: When the fetch program runs, it saves any new alerts to a local sqlite file. You will not receive a push notification twice for the same alert.*

## Feedback

Feedback and pull requests are welcome.
