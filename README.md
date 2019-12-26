# Klaviyo Metric Aggregations
Helper classes & scripts to walk the Klaviyo metric timeline API programmatically and construct aggregates from timeline data. 

See [bin/message_aggregates_from_timeline.py](https://github.com/ben-liang/klaviyo_metric_aggregations/blob/master/bin/message_aggregates_from_timeline.py) for a sample script to perform these aggregations.
  
To get started, first set up the virtual environment by running (on MacOSx/Linux):

```
$ python3 -m venv env
$ pip install -r requirements.txt
$ source .env/bin/activate
```

To run the sample script, run:
```
$ python ./bin/message_aggregates_from_timeline.py $API_KEY --metric_name="Received Email"
```
