#!/bin/bash

gcloud dataproc jobs submit pyspark --cluster mycluster --region us-central1 GetHR.py
