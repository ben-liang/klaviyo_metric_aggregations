def get_metric_id(metric_name, client):
    """
    Gets metric ID from metric name.
    """
    event_id = None
    response = client.metrics()
    metrics = response["data"]
    for m in metrics:
        if m["name"] == metric_name:
            event_id = m["id"]
    return event_id