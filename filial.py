# -*- encoding: utf-8 -*-
import pymssql
import timeit
import profile
import logging
from logging.handlers import TimedRotatingFileHandler
from logging import StreamHandler
import datetime
from datetime import date, timedelta
from zeep import Client
from lxml import etree
import os
import time
from tqdm import *
from requests import Session
from config import cb_sql_address,cb_sql_user_name,cb_sql_password,cb_sql_database
from config import prod_server_address,prod_server_user,prod_server_password
import sys

from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
from zeep.transports import Transport




