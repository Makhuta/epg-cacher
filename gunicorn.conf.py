# Gunicorn configuration file for EPG Channel Mapping Web UI

# Server socket
bind = "0.0.0.0:8000"
backlog = 2048

# Worker processes
workers = 2
worker_class = "sync"
worker_connections = 1000
timeout = 30
keepalive = 2

# Restart workers after this many requests, to help prevent memory leaks
max_requests = 1000
max_requests_jitter = 100

# Header size limits to fix "Request Header Fields Too Large" error
limit_request_line = 8190
limit_request_fields = 200
limit_request_field_size = 16384

# Logging
accesslog = "-"
errorlog = "-"
loglevel = "info"
access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'

# Process naming
proc_name = "epg_webui"

# Preload app for better performance
preload_app = True