from celery.task import task
from celery import signature, Celery
from bson.objectid import ObjectId
from lxml import etree
from itertools import compress
from json import loads, dumps
import logging
import requests
import boto3
import pkg_resources
import re

from celeryconfig import ALMA_KEY, ALMA_RW_KEY, ETD_NOTIFICATION_EMAIL, ALMA_NOTIFICATION_EMAIL, REST_ENDPOINT
import celeryconfig

logging.basicConfig(level=logging.INFO)

base_url = "https://cc.lib.ou.edu"
digital_object_url = "{0}/api/catalog/data/catalog/digital_objects".format(base_url)
digital_object_search = "{0}/.json?query={{\"filter\":{{\"project\":\"private\",\"bag\":{{\"$regex\":\"^share*\"}}}}}}".format(digital_object_url)
# search string on digital_objects  {"filter":{"project":"private","bag":{"$regex":"^share*"}}}
etd_search = "{0}/api/catalog/data/catalog/etd/.json?query={{\"filter\":{{\"ingested\":{{\"$ne\":true}}}}}}".format(base_url)
# search string on etd  {"filter":{"ingested":{"$ne":true}}}
alma_url = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{0}?expand=None&apikey={1}"

app = Celery()
app.config_from_object(celeryconfig)

#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result


#def _get_config_parameter(group,param,config_file="cybercom.cfg"):
#    config = ConfigParser() #ConfigParser.ConfigParser()
#    config.read(config_file)
#    return config.get(group,param)


def get_mmsid(bag):
    """ get the mmsid from end of bag name """
    mmsid = bag.split("_")[-1].strip()  # using bag name formatting: 1990_somebag_0987654321
    if re.match("^[0-9]+$", mmsid):  # check that we have an mmsid like value
        return mmsid
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


def get_bib_record(mmsid):
    try:
        result = requests.get(alma_url.format(mmsid, ALMA_KEY))
        if result.status_code == requests.codes.ok:
            return result.content
        else:
            logging.error(result.content)
            return {"error": "Alma server returned code: {0}".format(result.status_code)}
    except:
        logging.error("Alma Connection Error")
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
    s3_bucket='ul-bagit'
    s3_destination='private/shareok/{0}/data/'.format(bag_name)
    s3 = boto3.client('s3')
    files = [x['Key'] for x in s3.list_objects(Bucket=s3_bucket, Prefix=s3_destination)['Contents']]
    return [x for x in files if x.endswith((".pdf", ".txt"))]


def missing_or_blank(xpath_val):
    """ check if xpath is missing or blank """
    results = root.xpath(xpath_val)  #TODO: look at moving back into missing_fields function
    if len(results) > 0:
        return results[0].text == ""
    else:
        return True
 

def missing_fields(bib_record):
    # Fields to verify
    xpath_lookup = {
        "Title": "record/datafield[@tag=245]",
        "Author": "record/datafield[@tag=100]",
        "Publish Year": "record/datafield[@tag=264]|record/datafield[@tag=260]",
        "Thesis/Diss Tag": "record/datafield[@tag=502]",
        "School": "record/datafield[@tag=690]",
        "Subject Heading": "record/datafield[@tag=650]"
    }
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
    use_org = None
    use_type = None
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
    use_org = use_org if use_org is not None else default_org
    use_type = use_type if use_type is not None else default_type
    return collections["{0}_{1}".format(use_org, use_type)]


def check_missing(mmsids):
    """ Checks for missing fields stored in Alma """
    mmsids = [mmsids] if type(mmsids) != list else mmsids
    bib_records = map(get_bib_record, mmsids)
    missing = map(missing_fields, bib_records)
    return zip(mmsids, missing)


@task()
def notify_etd_missing_fields():
    """
    Sends email to collections to notify of missing fields in Alma
    for requested Theses and Disertations
    """
    mmsids = [record.get("mmsid") for record in get_bags(etd_search) if record.get("mmsid") is not None]
    missing = map(check_missing, mmsids)
    send_email = signature(
       "emailq.tasks.tasks.sendmail",
       kwargs={
           'to': ETD_NOTIFICATION_EMAIL,
           'subject': 'Missing ETD Fields',
           'body': dumps(missing)
           })
    send_email.delay()
    logging.info("Sent ETD notification email")
    return "Notification Sent"


@task()
def ingest_thesis_disertation(bag, collection="", dspace_endpoint="", eperson="libir@ou.edu"):
    """
    Ingest a bagged thesis or disertation into dspace

    args:
       bag (string); Name of bag to ingest
       collection (string); dspace collection id to load into - if blank will determine from Alma
       dspace_endpoint (string); url to shareok / commons API endpoint - example: https://test.shareok.org/rest
       eperson (string); email address to send notification to
    """

    if collection == "":
        mmsid = get_mmsid(bag)
        bib_record = get_bib_record(mmsid)
        if type(bib_record) is not dict:
            collection = guess_collection(bib_record)
        else:
            logging.error("failed to get bib_record to determine")
            return bib_record  # failed - pass along error message

    logging.info("Processing bag: {0}\nCollection: {1}\nEperson: {2}".format(bag, collection, eperson))
    
    # files to include in ingest
    files = list_s3_files(bag)

    mmsid = get_mmsid(bag)
    dc = bib_to_dc(get_bib_record(mmsid))

    items = [{bag: {"files": files, "metadata": dc}}]
   
    # TODO: add chain to update alma with corresponding url
    ingest = signature(
            "libtoolsq.tasks.tasks.awsDissertation", 
            queue="shareok-repotools-prod-workerq",
            kwargs={"dspaceapiurl":dspace_endpoint, "collectionhandle":collection, "items":items}
            )
    ingest.delay()

    requests.post
    return "Kicked off ingest for: {0}".format(bag)


def get_alma_url_field(bib_record):
    tree = etree.XML(bib_record)
    url_element = tree.find(".//*[@tag='856'][@ind1='4'][@ind2='0']/subfield[@code='u']")
    if url_element is not None:
        return url_element.text
    else:
        return None


def _update_alma_url_field(bib_record, url):
    """ Updates the url text for the first instance of tag 856(ind1=4, ind2=0) and returns as string
        
        args:
          bib_record (string); bib record xml 
          url (string); url to place in the record
    """
    tree = etree.XML(bib_record)
    url_element = tree.find(".//*[@tag='856'][@ind1='4'][@ind2='0']/subfield[@code='u']")
    if url_element is None:
        record_tree = tree.find(".//record")
        datafield = etree.Element("datafield", ind1="4", ind2="0", tag="856")
        subfield = etree.Element("subfield", code="u")
        subfield.text = url
        datafield.append(subfield)
        record_tree.append(datafield)
    else:
        url_element.text = url
    return etree.tostring(tree, standalone="yes", encoding="UTF-8")


@task()
def update_alma_url_field(mmsid, url, notify=True):
    """
    Updates the Electronic location (tag 856) in Alma with the URL

    args:
       mmsid (string); MMSID of the object to update in Alma
       url (string); the corresponding url in ShareOK/Commons for the the object
       notify (boolean); notify alma team of update - default is true
    """

    """
    Example:
        <datafield ind1="4" ind2="0" tag="856">
        <subfield code="u">https://shareok.org/something/somethingelse/123454321/blah/blah</subfield>
        </datafield>
    """
    msg = "URL for Alma record {0} has been changed\nfrom: {1}\nto: {2}"
    result = requests.get(alma_url.format(mmsid, ALMA_KEY))
    if result.status_code == requests.codes.ok:
        old_url = get_alma_url_field(result.content)
        new_xml = _update_alma_url_field(result.content, url)
        put_result = requests.put(url=alma_url.format(mmsid, ALMA_RW_KEY), data=new_xml, headers={"content-type": "application/xml"})
        if put_result.status_code == requests.codes.ok:
            logging.info("Alma record updated for mmsid: {0}".format(mmsid))
            if notify == True:
                send_email = signature(
                    "emailq.tasks.tasks.sendmail",
                    kwargs={
                    'to': ALMA_NOTIFICATION_EMAIL,
                    'subject': 'ETD Record Updated',
                    'body': msg.format(mmsid, old_url, url)
                })
                send_email.delay()
                logging.info("Sent Alma notification email")
            return "Updated record"
        else:
            logging.error("Could not update record: {0}".format(mmsid))
            return {"error": "Could not update record"}
    else:
        logging.error("Could not access alma")
        return {"error": "Could not access alma"}

@task()
def remove_etd_catalog_record(id):
    """
    Removes the specified record from the etd digital catalog

    args:
      id (string); this is the value specified by "_id" in the digital catalog record
    """

    db_client = app.backend.database.client

    if "catalog" not in db_client.database_names():
        return {"error": "catalog is missing"}
    if "etd" not in db_client.catalog.collection_names():
        return {"error": "etd collection is missing"}

    etd = db_client.catalog.etd
    record = etd.find_one({'_id': ObjectId(id)})
    if record:
        #etd.remove({'_id': ObjectId(id)})
        etd.delete_one({'_id': ObjectId(id)})  # limit to at most one record
        logging.info("Removed {0} from etd collection")
        return "Record {0} has been removed".format(id)
    else:
        return {"error": "Record {0} not found"}
