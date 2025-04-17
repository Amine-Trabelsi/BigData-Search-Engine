#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

ln -s /usr/bin/python3 /usr/bin/python

# Creating a virtual environment
# python3 -m venv .venv
# source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
python3 prepare_data.py
bash prepare_data.sh

# Run the indexer
bash index.sh /index/data

# Run the ranker
bash search.sh "this is a query!"

exec /bin/bash