from __future__ import unicode_literals
import boto3
import os
import pkg_resources
import re
import requests
import logging
import datetime
from celery import Celery
from bson.objectid import ObjectId
from itertools import compress
from json import loads
from lxml import etree
from collections import OrderedDict

from .config import alma_url

logging.basicConfig(level=logging.INFO)

try:
    import celeryconfig
except ImportError:
    logging.error("Failed to import celeryconfig")
    celeryconfig = None

try:
    from celeryconfig import ALMA_KEY, ALMA_RW_KEY, ETD_NOTIFICATION_EMAIL, ALMA_NOTIFICATION_EMAIL, REST_ENDPOINT
except ImportError:
    logging.error("Failed to import variables from celeryconfig")
    ALMA_KEY = ALMA_RW_KEY = ETD_NOTIFICATION_EMAIL = ALMA_NOTIFICATION_EMAIL = REST_ENDPOINT = ""

app = Celery()
app.config_from_object(celeryconfig)

class FailedIngest(Exception):
    """ Exception raised when ingest fails """
    pass


def get_mmsid(bag):
    """ get the mmsid from end of bag name """
    # The MMS ID can be 8 to 19 digits long (with the first two digits referring to the record type and
    # the last four digits referring to a unique identifier for the institution)
    # get an mmsid like value that is not at the beginning of the string
    mmsid = re.findall(r"(?<!^)(?<!\d)\d{8,19}(?!\d)", bag)  # not proceeded or followed by a digit and not at beginning
    if mmsid:
        return mmsid[-1]  # return rightmost match
    return None


def get_bags(url):
    """ iterate over pages of search results yielding bag metadata """
    data = loads(requests.get(url).text)
    while True:
        for item in data['results']:
            yield item
        if data['next'] is not None:
            data = loads(requests.get(data['next']).text)
        else:
            break


def get_requested_mmsids():
    """ queries requests for digitizations and returns list of mmsids """
    #return [bag['mmsid'] for bag in get_bags("https://cc.lib.ou.edu/api/catalog/data/catalog/etd/.json")]
    db_client = app.backend.database.client
    etd = db_client.catalog.etd
    return [bag['mmsid'] for bag in etd.find({})]


def get_requested_etds(mmsid):
    """ queries requests for digitization by mmsid regex and returns matching records """
    db_client = app.backend.database.client
    etd = db_client.catalog.etd
    return [requested_item for requested_item in etd.find({'mmsid': {'$regex': mmsid}})]


def get_bib_record(mmsid):
    try:
        result = requests.get(alma_url.format(mmsid, ALMA_KEY))
        if result.status_code == requests.codes.ok:
            return result.content
        else:
            logging.error(result.content)
            return {"error": "Alma server returned code: {0}".format(result.status_code)}
    except Exception as e:
        logging.error("Alma Connection Error")
        logging.error(e)
        return {"error": "Alma Connection Error - try again later."}


def get_marc_from_bib(bib_record):
    """ returns marc xml from bib record string"""
    record = etree.fromstring(bib_record).find("record")
    record.attrib['xmlns'] = "http://www.loc.gov/MARC21/slim"
    return etree.ElementTree(record)


def marc_xml_to_dc_xml(marc_xml):
    """ returns dublin core xml from marc xml """
    xml_path = pkg_resources.resource_filename(__name__, 'xslt/marc2dspacedc.xsl')
    marc2dc_xslt = etree.parse(xml_path)
    transform = etree.XSLT(marc2dc_xslt)
    return transform(marc_xml)

def validate_marc(marc_xml):
    xml = pkg_resources.resource_string(__name__, 'xslt/MARC21slim.xsd')
    schema = etree.XMLSchema(etree.fromstring(xml))
    parser = etree.XMLParser(schema=schema)
    return etree.fromstring(etree.tostring(marc_xml), parser)

def bib_to_dc(bib_record):
    """ returns dc as string from bib_record """
    return etree.tostring(marc_xml_to_dc_xml(validate_marc(get_marc_from_bib(bib_record))))


def list_s3_files(bag_name):
    s3_bucket=os.getenv('DEFAULT_BUCKET','ul-bagit')
    s3_destination='private/shareok/{0}/data/'.format(bag_name)
    s3 = boto3.client('s3')
    files = [x['Key'] for x in s3.list_objects(Bucket=s3_bucket, Prefix=s3_destination)['Contents']]
    return [f for f in files if f.endswith((".pdf", ".txt"))]


def missing_fields(bib_record):
    def missing_or_blank(xpath_val):
        """ check if xpath is missing or blank """
        results = root.xpath(xpath_val)
        if len(results) > 0:
            return results[0].text == ""
        else:
            return True

    # Fields to verify
    xpath_lookup = OrderedDict([
        ("245: Title", "record/datafield[@tag=245]"),
        ("100: Author", "record/datafield[@tag=100]"),
        ("260/264: Publish Year", "record/datafield[@tag=264]|record/datafield[@tag=260]"),
        ("502a: Thesis/Diss Tag", "record/datafield[@tag=502]/subfield[@code='a']"),
        ("690: School", "record/datafield[@tag=690]"),
        ("600/610/611/630/650/651: Subject Heading", "record/datafield[@tag=600]|record/datafield[@tag=610]|record/datafield[@tag=611]|record/datafield[@tag=630]|record/datafield[@tag=650]|record/datafield[@tag=651]")
    ])
    if bib_record is not None and type(bib_record) is not dict:
        root = etree.fromstring(bib_record)
        missing = map(missing_or_blank, xpath_lookup.values())
        return list(compress(xpath_lookup.keys(), missing))
    elif type(bib_record) is dict:
        return bib_record.values()  # The lookup of the bib_record failed pass along error message
    return ["Could not find record!"]


def guess_collection(bib_record):
    """ attempts to determine collection based off of marc21 tag 502 in Alma record """
    default_org = "OU"
    default_type = "THESIS"
    orgs = {"university of oklahoma": "OU"}
    types = {"thesis": "THESIS",
             "theses": "THESIS",
             "dissertation": "DISSERTATION",
             "dissertations": "DISSERTATION"}
    collections = {"OU_THESIS": "11244/23528",
                   "OU_DISSERTATION": "11244/10476"}
    tree = etree.XML(bib_record)
    sub502 = tree.find("record/datafield[@tag='502']/subfield[@code='a']")
    use_org = default_org
    use_type = default_type
    text = ""
    if sub502 is not None:
        text = sub502.text.lower()
    for org_key in orgs.keys():
        if org_key in text:
            use_org = orgs[org_key]
            break
    for type_key in types.keys():
        if type_key in text:
            use_type = types[type_key]
            break
    return collections["{0}_{1}".format(use_org, use_type)]


def check_missing(mmsids):
    """ Checks for missing fields stored in Alma """
    mmsids = [mmsids] if type(mmsids) != list else mmsids
    bib_records = map(get_bib_record, mmsids)
    missing = map(missing_fields, bib_records)
    return list(zip(mmsids, missing))


def get_alma_url_field(bib_record):
    tree = etree.XML(bib_record)
    url_element = tree.find(".//*[@tag='856'][@ind1='4'][@ind2='0']/subfield[@code='u']")
    if url_element is not None:
        return url_element.text
    else:
        return None


def chunk_list(_list, size):
    """ chunk larger list into smaller lists of defined size """
    for x in range(0, len(_list), size):
        yield _list[x:x + size]


def get_digitized_bags(mmsids):
    """ queries list of mmsids and yields iterator of bagnames

        This looks for digitized objects in S3 that have not been ingested into shareok

    """
    if type(mmsids) != list:
        mmsids = [mmsids]
    regex_list = '|'.join('^share.*{0}$'.format(mmsid) for mmsid in mmsids)
    options = {'bag':{'$regex': regex_list},
               'locations.s3.exists': True,
               'application.dspace.ingested': {'$ne': True},
              }
    db_client = app.backend.database.client
    digital_objects = db_client.catalog.digital_objects
    results = digital_objects.find(options)
    return [result['bag'].split('/')[-1] for result in results]

def update_ingest_status(bagname, url, application='dspace', project=None, ingested=True):
    options = {'ingested': ingested,
               'url': url,
               'datetime': datetime.datetime.utcnow().isoformat()
               }
    db_client = app.backend.database.client
    digital_objects = db_client.catalog.digital_objects

    # verify and update bagname to include shareok path in name
    if not re.match("^shareok", bagname):
        bagname = "shareok/{0}".format(bagname)

    if project is not None:
        document = digital_objects.find_one({'bag': bagname, 'project': project})
    else:
        document = digital_objects.find_one({'bag': bagname})

    if document:
        if not document.get('application'):
            document['application'] = {}
        if not document['application'].get(application):
            document['application'][application] = {}
        # iterate over options to preserve other existing attributes
        for option, value in options.items():
            document['application'][application][option] = value
        if document != {}:  # prevent overwriting with blank document
            status = digital_objects.update({'_id': ObjectId(document['_id'])}, document)
            if not status['nModified']:
                logging.error("Could not update dspace ingest status: {0}".format(document['bag']))