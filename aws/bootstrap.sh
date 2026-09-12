#!/bin/bash
# EMR bootstrap action — installs Python deps needed by streaming_job.py
# on every node in the cluster. Upload this to S3 and reference it in
# emr_cluster_config.json under BootstrapActions.

set -e

sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install psycopg2-binary==2.9.9 sqlalchemy==2.0.30 boto3==1.34.113

echo "Bootstrap complete."
