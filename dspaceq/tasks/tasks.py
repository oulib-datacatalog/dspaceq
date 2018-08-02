import boto3
import re
import pkg_resources
import datetime

from itertools import compress
from tempfile import mkdtemp
from shutil import rmtree
from os.path import join
from os import mkdir
from lxml import etree
from subprocess import check_call, CalledProcessError

from celery.task import task
from celery import signature, group, Celery
from inspect import cleandoc
from bson.objectid import ObjectId
from json import loads, dumps
import logging
import requests
import jinja2

from utils import *
from config import alma_url

from celeryconfig import ALMA_KEY, ALMA_RW_KEY
from celeryconfig import ETD_NOTIFICATION_EMAIL, ALMA_NOTIFICATION_EMAIL, IR_NOTIFICATION_EMAIL
from celeryconfig import REST_ENDPOINT, QUEUE_NAME
import celeryconfig

logging.basicConfig(level=logging.INFO)

app = Celery()
app.config_from_object(celeryconfig)

s3 = boto3.resource("s3")
s3_bucket = 'ul-bagit'
ALMA_KEY = 'l7xxa730c5d8d8844676ab5e1607d1161322'
alma_url = "https://api-na.hosted.exlibrisgroup.com/almaws/v1/bibs/{0}?expand=None&apikey={1}"
DSPACE_BINARY = "/srv/oulib/dspace/bin/dspace"
DSPACE_FQDN = "test.shareok.org"

#Example task
@task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result

@task()
def get_mmsid(bag):
    """ get the mmsid from end of bag name """
    mmsid = bag.split("_")[-1].strip()  # using bag name formatting: 1990_somebag_0987654321
    if re.match("^[0-9]+$", mmsid):  # check that we have an mmsid like value
        return mmsid
    return None

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

def list_s3_files(bag_name):
    s3_bucket='ul-bagit'
    s3_destination='private/shareok/{0}/data/'.format(bag_name)
    s3 = boto3.client('s3')
    files = [x['Key'] for x in s3.list_objects(Bucket=s3_bucket, Prefix=s3_destination)['Contents']]
    #return ["{0}/{1}".format(s3_bucket, f) for f in files if f.endswith((".pdf", ".txt"))]
    return [f for f in files if f.endswith((".pdf", ".txt"))]


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

def get_marc_from_bib(bib_record):
    """ returns marc xml from bib record string"""
    record = etree.fromstring(bib_record).find("record")
    record.attrib['xmlns'] = "http://www.loc.gov/MARC21/slim"
    return etree.ElementTree(record)

files = list_s3_files("1945_Lowry_Ruth_99304054302042")
bib_record = get_bib_record(get_mmsid("1945_Lowry_Ruth_99304054302042"))
dc = bib_to_dc(bib_record)
dc

@task()
def ingest(bag_details, collection, notify_email="mpmalahy@ou.edu"):
    tempdir = mkdtemp(prefix="dspaceq_")
    for index, bag in enumerate(bag_details):
        bag_dir = join(tempdir, "item_{0}".format(index))
        mkdir(bag_dir)
        for file in bag["files"]:
            filename = file.split("/")[-1]
            s3.Bucket(s3_bucket).download_file(file, join(tempdir, "item_0", filename))
        with open(join(tempdir, "item_0", "contents"),"w") as f:
            filenames = [file.split("/")[-1] for file in files]
            f.write("\n".join(filenames))
            f.write("\ndublin_core.xml")
        with open(join(tempdir, "item_0", "dublin_core.xml"), "w") as f:
            f.write(bag["metadata"])
    try:
        check_call([DSPACE_BINARY, "import", "-a", "-e", notify_email, "-c", collection, "-s", tempdir, "-m", '{0}/mapfile'.format(tempdir)])
    except CalledProcessError as e:
        print("Failed to ingest: {0}".format(bag_details))
        print("Error: {0}".format(e))
        return {"Error": "Failed to ingest: {0}".format(bag_details)}
    finally:
        rmtree(tempdir)

@task()
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

collection = guess_collection(bib_record)
collection

@task()
def ingest_thesis_dissertation(bag="", collection="",): #dspace_endpoint=REST_ENDPOINT):
    """
    Ingest a bagged thesis or dissertation into dspace

    args:
       bag (string); Name of bag to ingest - if blank, will ingest all non-ingested items
       collection (string); dspace collection id to load into - if blank, will determine from Alma
       dspace_endpoint (string); url to shareok / commons API endpoint - example: https://test.shareok.org/rest
    """

    if bag == "":
        # Ingest requested items (bags) not yet ingested
        bags = get_digitized_bags([etd['mmsid'] for etd in get_requested_etds(".*")])
    else:
        bags = [bag]

    if bags == []:
        return "No items found ready for ingest"

    collections = defaultdict(list)
    failed = {}
    # files to include in ingest
    for bag in bags:
        files = list_s3_files(bag)
        logging.info("Using files: {0}".format(files))

        mmsid = get_mmsid(bag)
        bib_record = get_bib_record(mmsid)
        dc = bib_to_dc(bib_record)

        if collection == "":
            if type(bib_record) is not dict: #If this is a dictionary, we failed to get a valid bib_record
                collections[guess_collection(bib_record)].append({bag: {"files": files, "metadata": dc}})
            else:
                logging.error("failed to get bib_record to determine collection for: {0}".format(bag))
                failed[bag] = bib_record  # failed - pass along error message
        else:
            collections[collection].append({bag: {"files": files, "metadata": dc}})

    update_alma = signature(
        "dspaceq.tasks.tasks.update_alma_url_field",
        queue=QUEUE_NAME
    )
    update_catalog = signature(
        "dspaceq.tasks.tasks.update_catalog",
        queue=QUEUE_NAME
    )
    send_email = signature(
        "dspaceq.tasks.tasks.notify_dspace_etd_loaded",
        queue=QUEUE_NAME
    )
    for collection in collections.keys():
        collection_bags = [x.keys()[0] for x in collections[collection]]
        items = collections[collection]
        ingest = signature(
            "libtoolsq.tasks.tasks.awsDissertation",
            queue="test-queue",
            kwargs={"dspaceapiurl": dspace_endpoint,
                    "collectionhandle": collection,
                    "items": items
                    }
        )
        logging.info("Processing Collection: {0}\nBags:{1}".format(collection, collection_bags))
        chain = (ingest | group(update_alma, update_catalog, send_email))
        chain.delay()
    return {"Kicked off ingest": bags, "failed": failed}


@task()
def notify_etd_missing_fields():
    """
    Sends email to collections to notify of missing fields in Alma
    for requested Theses and Disertations
    """
    emailtmplt = """
    The following ETD requests have missing fields:
    {% for bag in bags %}========================
      mmsid: {{ bags[bag].mmsid }}
      Missing Details:{% for field in bags[bag].missing %}
        {{ field }}{% endfor %}
      Files:{% for etd_file in bags[bag].files %}
        {{ etd_file }}{% endfor %}  
    {% endfor %}
    """

    mmsids = get_requested_mmsids()
    digitized_bags = list(get_digitized_bags(mmsids))
    digitized_mmsids = [get_mmsid(bag) for bag in digitized_bags]
    missing = [item for item in check_missing(digitized_mmsids) if item[1] != []]
    bags_missing_details = {}
    for bag in digitized_bags:
        for mmsid, items in missing:
            if mmsid in bag:
                bags_missing_details[bag] = {}
                bags_missing_details[bag]['mmsid'] = mmsid
                bags_missing_details[bag]['missing'] = items
                bags_missing_details[bag]['files'] = ["https://s3.amazonaws.com/{0}".format(x) for x in list_s3_files(bag)]
    if bags_missing_details:
        env = jinja2.Environment()
        tmplt = env.from_string(cleandoc(emailtmplt))
        msg = tmplt.render(bags=bags_missing_details)
        send_email = signature(
           "emailq.tasks.tasks.sendmail",
           kwargs={
               'to': ETD_NOTIFICATION_EMAIL,
               'subject': 'Missing ETD Fields',
               'body': msg
               })
        send_email.delay()
        logging.info("Sent ETD notification email to {0}".format(ETD_NOTIFICATION_EMAIL))
        return "Notification Sent"
    logging.info("No missing attributes - no notification email")
    return "No Missing Details"


@task()
def ingest_thesis_dissertation(bag="", collection="", dspace_endpoint=REST_ENDPOINT):
    """
    Ingest a bagged thesis or dissertation into dspace

    args:
       bag (string); Name of bag to ingest - if blank, will ingest all non-ingested items
       collection (string); dspace collection id to load into - if blank, will determine from Alma
       dspace_endpoint (string); url to shareok / commons API endpoint - example: https://test.shareok.org/rest
    """

    if collection == "":
        mmsid = get_mmsid(bag)
        bib_record = get_bib_record(mmsid)
        if type(bib_record) is not dict:
            collection = guess_collection(bib_record)
        else:
            logging.error("failed to get bib_record to determine")
            return bib_record  # failed - pass along error message

    if bag == "":
        # Ingest requested items (bags) not yet ingested
        bags = get_digitized_bags([etd['mmsid'] for etd in get_requested_etds(".*")])
    else:
        bags = [bag]

    logging.info("Processing bag(s): {0}\nCollection: {1}".format(bags, collection))

    items = []
    # files to include in ingest
    for bag in bags:
        files = list_s3_files(bag)
        logging.info("Using files: {0}".format(files))

        mmsid = get_mmsid(bag)
        dc = bib_to_dc(get_bib_record(mmsid))

        items.append({bag: {"files": files, "metadata": dc}})

    ingest = signature(
            "libtoolsq.tasks.tasks.awsDissertation", 
            queue="shareok-repotools-prod-workerq",
            kwargs={"dspaceapiurl":dspace_endpoint, "collectionhandle":collection, "items":items}
            )
    update_alma = signature(
        "dspaceq.tasks.tasks.update_alma_url_field",
        queue=QUEUE_NAME
    )
    update_catalog = signature(
        "dspaceq.tasks.tasks.update_catalog",
        queue=QUEUE_NAME
    )
    send_email = signature(
        "emailq.tasks.tasks.notify_dspace_etd_loaded",
        queue=QUEUE_NAME
    )
    chain = (ingest | group(update_alma, update_catalog, send_email))
    chain.delay()
    return "Kicked off ingest for: {0}".format(bag)


@task()
def notify_dspace_etd_loaded(args):
    """
    Send email notifying repository group that new ETDs have been loaded into the repository
    This is called by the ingest_thesis_dissertation task

    args:
       args: {"success": {bagname: url}
    """
    ingested_items = args.get("success")
    if ingested_items:
        ingested_url_lookup = {get_mmsid(bag): url for bag, url in ingested_items.items()}
        mmsids_regex = "|".join([get_mmsid(bag) for bag in ingested_items.keys()])
        request_details = get_requested_etds(mmsids_regex)
        for request in request_details:
            request['url'] = ingested_url_lookup[request['mmsid']]

        emailtmplt = """
        The following ETD requests have been loaded into the repository:
        {% for request in request_details %}========================
        Requester: {{ requested_details[request].name }}
        Email: {{ requested_details[request].email }} 
        URL: {{ requested_details[request].url }}  
        {% endfor %}
        """
        env = jinja2.Environment()
        tmplt = env.from_string(cleandoc(emailtmplt))
        msg = tmplt.render(request_details=request_details)
        send_email = signature(
           "emailq.tasks.tasks.sendmail",
           kwargs={
               'to': IR_NOTIFICATION_EMAIL,
               'subject': 'ETD Requests Loaded into Repository',
               'body': msg
               })
        send_email.delay()
        return "Ingest notification sent"
    return "No items to ingest - no notification sent"


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
def update_alma_url_field(args, notify=True):
    """
    Updates the Electronic location (tag 856) in Alma with the URL
    This is called by the ingest_thesis_dissertation task

    args:
       args: {"success": {bagname: url}
       notify (boolean); notify alma team of update - default is true
    """

    """
    Example:
        <datafield ind1="4" ind2="0" tag="856">
        <subfield code="u">https://shareok.org/something/somethingelse/123454321/blah/blah</subfield>
        </datafield>
    """
    ingested_items = args.get("success")
    if ingested_items:
        status = {'success': [], 'fail': []}
        msg = "URL(tag 856) for Alma record {0} has been changed\nfrom: {1}\nto: {2}"
        for bagname, url in ingested_items.items():
            mmsid = get_mmsid(bagname)
            result = requests.get(alma_url.format(mmsid, ALMA_KEY))
            if result.status_code == requests.codes.ok:
                old_url = get_alma_url_field(result.content)
                new_xml = _update_alma_url_field(result.content, url)
                update_result = requests.put(url=alma_url.format(mmsid, ALMA_RW_KEY), data=new_xml, headers={"content-type": "application/xml"})
                if update_result.status_code == requests.codes.ok:
                    logging.info("Alma record updated for mmsid: {0}".format(mmsid))
                    status['success'].append([bagname, url])
                    if notify is True:
                        send_email = signature(
                            "emailq.tasks.tasks.sendmail",
                            kwargs={
                            'to': ALMA_NOTIFICATION_EMAIL,
                            'subject': 'ETD Record Updated - URL',
                            'body': msg.format(mmsid, old_url, url)
                        })
                        send_email.delay()
                        logging.info("Sent Alma notification email")
                else:
                    logging.error("Could not update record: {0}".format(mmsid))
                    status['fail'].append([bagname, "Could not update record"])
            else:
                # TODO: Add retry
                logging.error("Could not access alma")
                status['fail'].append([bagname, "Could not access alma"])
        return status


@task()
def update_datacatalog(args):
    """
    Adds ingested status into Data Catalog
    This is called by the ingest_thesis_dissertation task
    
    args:
       {"success": {bagname: url}
    """
    ingested_items = args.get("success")
    if ingested_items:
        for bagname, url in ingested_items.items():
            update_ingest_status(bagname, url, application='dspace', project='private', ingested=True)
        return "Updated data catalog"
    return "No items to update in data catalog"


@task()
def remove_etd_catalog_record(id):
    """
    Removes the specified record from the etd digital catalog

    args:
      id (string); this is the value specified by "_id" in the digital catalog record
    """
    # TODO: update to remove requests that have been ingested - query ingest status of exiting requests
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
