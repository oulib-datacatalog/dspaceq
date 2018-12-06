from celery.task import task

import logging
import re

from celeryconfig import DB_USERNAME, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT
import celeryconfig

import sqlalchemy
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, text

pg_db = {
    'drivername': 'postgres',
    'username': DB_USERNAME,
    'password': DB_PASSWORD,
    'database': DB_NAME,
    'host': DB_HOST,
    'port': DB_PORT
}

startdate_query = """
  select * from (
    select handle, item_id, max(start_date) as max_date
    from handle
    join item2bundle on item2bundle.item_id = handle.resource_id
    join bundle2bitstream on bundle2bitstream.bundle_id = item2bundle.bundle_id
    join resourcepolicy on resourcepolicy.dspace_object = bundle2bitstream.bitstream_id
    group by handle.handle, item2bundle.item_id
    having handle in (
      select distinct(handle)
      from handle
      join item2bundle on item2bundle.item_id = handle.resource_id
      join bundle2bitstream on bundle2bitstream.bundle_id = item2bundle.bundle_id
      join resourcepolicy on resourcepolicy.dspace_object = bundle2bitstream.bitstream_id
      where resourcepolicy.start_date >= :beg_date
      and resourcepolicy.start_date <= :end_date
    )
  ) as embargos
  where embargos.max_date >= :beg_date
  and embargos.max_date <= :end_date;
"""

metadata_query = """
    select metadata_field_id, text_value
    from metadatavalue
    where dspace_object_id = :item_id
    and metadata_field_id in :fields;
"""

# Metadata field values in DSpace
AUTHOR = 3
URI = 25
TITLE = 64
ALTERNATIVE_TITLE = 65
DEPARTMENT = 103


@task()
def report_embargoed_items(beg_date, end_date):
    """
    Report details regarding items coming out of embargo in the selected date range
    Returns list of list: [[handle, author, title, dept/college, date],]

    args:
       beg_date (string): 'YYYY-MM-DD'
       end_date (string): 'YYYY-MM-DD'
    """
    
    # regular expression to match YYYY-MM-DD
    re_datematch = "^([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))$"

    if not re.match(re_datematch, beg_date):
        return {"ERROR": "beg_date does not use YYYY-MM-DD format"}
    if not re.match(re_datematch, end_date):
        return {"ERROR": "end_date does not use YYYY-MM-DD format"}

    # TODO: check that end_date is greater than beg_date
    try:
        engine = create_engine(URL(**pg_db))
        conn = engine.connect()
    except sqlalchemy.exc.OperationalError as e:
        logging.error("Error with DB connection (from dspaceq/reports)\n{0}".format(e))
        return {"ERROR": "Issue connecting to database, try again in a few minutes"}

    try:
        res_items = conn.execute(text(startdate_query), beg_date=beg_date, end_date=end_date).fetchall()
    except sqlalchemy.exc.DataError as e:
        logging.error("Potential sql injection attempt\n{0}".format(e))
        return {"ERROR": "Could not process supplied dates"}

    results = []
    for item in res_items:
        handle, item_id, start_date = item
        res_meta = dict(conn.execute(text(metadata_query), item_id=item_id, fields=(AUTHOR, URI, TITLE, DEPARTMENT)).fetchall())
        results.append(
            [handle, 
             res_meta.get(AUTHOR, "Unknown"),
             res_meta.get(TITLE, "Unknown"),
             res_meta.get(DEPARTMENT, "Unknown"),
             start_date.isoformat()
            ]
        )
    return results

