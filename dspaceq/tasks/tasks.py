from tempfile import mkdtemp
from shutil import rmtree
from os.path import join, isfile
from os import mkdir
from subprocess import check_call, CalledProcessError, check_output, STDOUT

from celery import signature, group, Celery, shared_task
from inspect import cleandoc
from collections import defaultdict
from bson.objectid import ObjectId
from lxml import etree
from six import ensure_text

import boto3
import logging
import requests
import jinja2

from .utils import *
from .config import alma_url

logging.basicConfig(level=logging.INFO)

try:
    import celeryconfig
except ImportError:
    logging.error("Failed to import celeryconfig")
    celeryconfig = None

try:
    from celeryconfig import ALMA_KEY, ALMA_RW_KEY, ETD_NOTIFICATION_EMAIL, ALMA_NOTIFICATION_EMAIL, REST_ENDPOINT
    from celeryconfig import IR_NOTIFICATION_EMAIL, QUEUE_NAME, DSPACE_BINARY, DSPACE_FQDN
except ImportError:
    logging.error("Failed to import variables from celeryconfig")
    ALMA_KEY = ALMA_RW_KEY = ETD_NOTIFICATION_EMAIL = ALMA_NOTIFICATION_EMAIL = REST_ENDPOINT = ""
    IR_NOTIFICATION_EMAIL = QUEUE_NAME = DSPACE_BINARY = DSPACE_FQDN = ""

app = Celery()
app.config_from_object(celeryconfig)

s3_bucket = 'ul-bagit'

#Example task
@shared_task()
def add(x, y):
    """ Example task that adds two numbers or strings
        args: x and y
        return addition or concatination of strings
    """
    result = x + y
    return result

@app.task()
def dspace_ingest(bag_details, collection, notify_email="libir@ou.edu"):
    """ Generates temporary directory and url for the bags to be downloaded from
        S3, prior to ingest into DSpace, then performs the ingest

        args: bag_details(dictionary), [{"bag name": {"files": [...], "metadata": "xml", "metadata_ou": "ou.xml"}}]
              collection (string); dspace collection id to load into - if blank,
                                   will determine from Alma
              dspace_endpoint (string); url to shareok / commons API endpoint
              - example: https://test.shareok.org/rest
    """

    item_match = {} #lookup to match item in mapfile to bag
    tempdir = mkdtemp(prefix="dspaceq_")
    results = []
    
    s3 = boto3.resource("s3")

    if type(bag_details) != list:
        bag_details = [bag_details]

    '''download files and metadata indicated by bag_details'''
    for index, bag in enumerate(bag_details):
        item_match["item_{0}".format(index)] = list(bag.keys())[0]
        bag_dir = join(tempdir, "item_{0}".format(index))
        bag_args = list(bag.values())[0]
        mkdir(bag_dir)
        if type(bag) == dict:
            files = bag_args["files"]
            for file in files:
                filename = file.split("/")[-1]
                s3.Bucket(s3_bucket).download_file(file, join(bag_dir, filename))
            with open(join(bag_dir, "contents"),"w") as f:
                filenames = [file.split("/")[-1] for file in files]
                f.write("\n".join(filenames))
            with open(join(bag_dir, "dublin_core.xml"), "w") as f:
                f.write(ensure_text(bag_args["metadata"]))
            for attrib in bag_args:
                if "metadata_" in attrib:
                    with open(join(bag_dir, attrib), "w") as f:
                        f.write(ensure_text(bag_args[attrib]))
        else:
            print('The submitted item for bag ingest does not match format', bag)
            results.append(bag, "Failed to ingest-check submitted formatting")

    try:
        check_call(["chmod", "-R", "0775", tempdir])
        check_call(["chgrp", "-R", "tomcat", tempdir])
        with open('{0}/ds_ingest_log.txt'.format(tempdir), "w") as f:
            check_call(["sudo", "-u", "tomcat", DSPACE_BINARY, "import", "-a", "-e", notify_email, "-c", collection.encode('ascii', 'ignore'), "-s", tempdir, "-m", ('{0}/mapfile'.format(tempdir))], stderr=f, stdout=f)
        with open('{0}/mapfile'.format(tempdir)) as f:
            for row in f.read().split('\n'):
                if row:
                    item_index, handle = row.split(" ")
                    results.append((item_match[item_index], handle))
    except CalledProcessError as e:
        exists = isfile('{0}/ds_ingest_log.txt'.format(tempdir))
        if exists:
            with open('{0}/ds_ingest_log.txt'.format(tempdir), "r") as f:
                print(f.read())
        print("Error: {0}".format(e))
        raise FailedIngest("failed to ingest")
    finally:
        rmtree(tempdir)
    return({"success": {item[0]:"{0}{1}".format(DSPACE_FQDN, item[1]) for item in results}})


@app.task()
def ingest_thesis_dissertation(bag="", collection="",): #dspace_endpoint=REST_ENDPOINT):
    """
    Ingest a bagged thesis or dissertation into dspace

    args:
       bag (string); Name of bag to ingest - if blank, will ingest all non-ingested items
       collection (string); dspace collection id to load into - if blank, will determine from Alma
    """

    if bag == "":
        # Ingest requested items (bags) not yet ingested
        bags = get_digitized_bags([etd['mmsid'] for etd in get_requested_etds(".*")])
    else:
        bags = [bag]

    if bags == []:
        return "No items found ready for ingest"

    s3 = boto3.resource("s3")

    collections = defaultdict(list)

    failed = {}
    good_bags = []
    for bag in bags:
        if check_missing(get_mmsid(bag))[0][1] != []:
            failed[bag] = "Missing required metadata in Alma - contact cataloging group"
            continue  #skip to next bag

        files = list_s3_files(bag)
        logging.debug("Using files: {0}".format(files))

        mmsid = get_mmsid(bag)
        bib_record = get_bib_record(mmsid)
        
        # Remove 590 tags from marc bib record
        marc_xml = get_marc_from_bib(bib_record).getroot()
        found_elements = marc_xml.xpath("datafield[@tag=590]")
        for element in found_elements:
            marc_xml.remove(element)
        namespaced_marc_xml = validate_marc(marc_xml)
        logging.debug(namespaced_marc_xml)

        dc_xml_element = marc_xml_to_dc_xml(namespaced_marc_xml).getroot()
        logging.debug(dc_xml_element)

        # Remove duplicate "date created" fields
        results = dc_xml_element.xpath("//dublin_core/dcvalue[@element='date' and @qualifier='created']")
        for result in results[1:]:
            dc_xml_element.remove(result)

        new_file_list = []
        error_in_file = False
        for file in files:
            if 'committee.txt' in file.lower():
                obj = s3.Object(s3_bucket, file)
                committee = obj.get()['Body'].read().decode('utf-8')
             # If committee.txt is present, add contents to dc metadata
                if committee:
                    for committee_member in committee.split("\n"):
                        try:
                            c = etree.Element("dcvalue", element='contributor', qualifier='committeeMember')
                            c.text = committee_member
                            dc_xml_element.insert(0, c)
                        except ValueError:
                            logging.error("Incompatible character found in committee.txt for {0}".format(bag))
                            failed[bag] = "Incompatible character found in committee.txt"
                            error_in_file = True
                            break
                    if error_in_file:
                        break  # break out of file handling loop

            elif 'abstract.txt' in file.lower():
            # If abstract.txt is present, add contents to dc metadata
                obj = s3.Object(s3_bucket, file)
                abstract = obj.get()['Body'].read().decode('utf-8')
                if abstract:
                    try:
                        a = etree.Element("dcvalue", element='description', qualifier='abstract')
                        a.text = abstract
                        dc_xml_element.insert(0, a)
                    except ValueError:
                        logging.error("Incompatible character found in abstract.txt for {0}".format(bag))
                        failed[bag] = "Incompatible character found in abstract.txt"
                        error_in_file = True
                        break

            else:
                new_file_list.append(file)
                
        if error_in_file:
            continue  # skip to next bag

        dc = etree.tostring(dc_xml_element)
        files = new_file_list

        if collection == "":
            if type(bib_record) is not dict: #If this is a dictionary, we failed to get a valid bib_record
                collections[guess_collection(bib_record)].append({bag: {"files": files, "metadata": dc}})
            else:
                logging.error("failed to get bib_record to determine collection for: {0}".format(bag))
                failed[bag] = bib_record  # failed - pass along error message
        else:
            collections[collection].append({bag: {"files": files, "metadata": dc}})
        
        good_bags.append(bag)
        
    update_alma = signature(
        "dspaceq.tasks.tasks.update_alma_url_field",
        queue=QUEUE_NAME
    )
    update_datacatalog = signature(
        "dspaceq.tasks.tasks.update_datacatalog",
        queue=QUEUE_NAME
    )
    send_etd_notification = signature(
        "dspaceq.tasks.tasks.notify_dspace_etd_loaded",
        queue=QUEUE_NAME
    )
    for collection in collections.keys():
        collection_bags = [list(bag_record.keys())[0] for bag_record in collections[collection]]
        items = collections[collection]
        ingest = signature(
            "dspaceq.tasks.tasks.dspace_ingest",
            queue=QUEUE_NAME,
            kwargs={"collection": collection ,
                    "bag_details": items
                    }
        )
        logging.info("Processing Collection: {0}\nBags:{1}".format(collection, collection_bags))
        chain = (ingest | group(update_alma, update_datacatalog, send_etd_notification))
        chain.delay()
    return {"Kicked off ingest": good_bags, "failed": failed}


@app.task()
def notify_etd_missing_fields():
    """
    Sends email to collections to notify of missing fields in Alma
    for requested Theses and Disertations
    """
    emailtmplt = r"""
    The following ETD requests have missing fields:
    The bags are accessible on norfile: ul-bagit\shareok\*
    {% for bag in bags %}========================
      bag: {{ bag }}
      mmsid: {{ bags[bag].mmsid }}
      Missing Details:{% for field in bags[bag].missing %}
        {{ field }}{% endfor %}
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
    if bags_missing_details:
        env = jinja2.Environment()
        tmplt = env.from_string(cleandoc(emailtmplt))
        msg = tmplt.render(bags=bags_missing_details)
        sendmail = signature(
           "emailq.tasks.tasks.sendmail",
           kwargs={
               'to': ETD_NOTIFICATION_EMAIL,
               'subject': 'Missing ETD Fields',
               'body': msg
               })
        sendmail.delay()
        logging.info("Sent ETD notification email to {0}".format(ETD_NOTIFICATION_EMAIL))
        return "Notification Sent"
    logging.info("No missing attributes - no notification email")
    return "No Missing Details"


@app.task()
def notify_dspace_etd_loaded(args):
    """
    Send email notifying repository group that new ETDs have been loaded into the repository
    This is called by the ingest_thesis_dissertation task
   
    args:
       args: {"success": {bagname: url}
     """  
    ingested_items = args.get("success")
    print(ingested_items)
    if ingested_items:
        ingested_url_lookup = {get_mmsid(bag): url for bag, url in ingested_items.items()}
        mmsids_regex = "|".join([get_mmsid(bag) for bag in ingested_items.keys()])
        request_details = get_requested_etds(mmsids_regex)
        print(request_details)
        for request in request_details:
            request['url'] = ingested_url_lookup[request['mmsid']]

        emailtmplt = r"""
        The following ETD requests have been loaded into the repository:
        {% for request in request_details %}========================
        Requester: {{ request.name }}
        Email: {{ request.email }}
        Creator: {{ request.creator }}
        Year: {{ request.year }}
        URL: {{ request.url }}
        {% endfor %}
        """
        env = jinja2.Environment()
        tmplt = env.from_string(cleandoc(emailtmplt))
        msg = tmplt.render(request_details=request_details)
        print(msg)
        send_mail = signature(
           "emailq.tasks.tasks.sendmail",
           kwargs={
               'to': IR_NOTIFICATION_EMAIL,
               'subject': 'ETD Requests Loaded into Repository',
               'body': msg
               })
        send_mail.delay()
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


@app.task()
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
                update_result = requests.put(url=alma_url.format(mmsid, ALMA_RW_KEY),
                    data=new_xml, headers={"content-type": "application/xml"})
                if update_result.status_code == requests.codes.ok:
                    logging.info("Alma record updated for mmsid: {0}".format(mmsid))
                    status['success'].append([bagname, url])
                    if notify is True:
                        sendmail = signature(
                            "emailq.tasks.tasks.sendmail",
                            kwargs={
                            'to': ALMA_NOTIFICATION_EMAIL,
                            'subject': 'ETD Record Updated - URL',
                            'body': msg.format(mmsid, old_url, url)
                        })
                        sendmail.delay()
                        logging.info("Sent Alma notification email")
                else:
                    logging.error("Could not update record: {0}".format(mmsid))
                    status['fail'].append([bagname, "Could not update record"])
            else:
                # TODO: Add retry
                logging.error("Could not access alma")
                status['fail'].append([bagname, "Could not access alma"])
        return status


@app.task()
def update_datacatalog(args):
    """
    Adds ingested status into Data Catalog
    This is called by the ingest_thesis_dissertation task

    args:
       {"success": {"bagname": "url"}}
    """
    ingested_items = args.get("success")
    if ingested_items:
        for bagname, url in ingested_items.items():
            update_ingest_status(bagname, url, application='dspace', project=None, ingested=True)
        return "Updated data catalog"
    return "No items to update in data catalog"


@app.task()
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
        etd.delete_one({'_id': ObjectId(id)})  # limit to at most one record
        logging.info("Removed {0} from etd collection")
        return "Record {0} has been removed".format(id)
    else:
        return {"error": "Record {0} not found"}


@app.task()
def list_missing_metadata_etd(bag=""):
    """
    Displays missing metadata fields from Alma for specified bags
    
    args:
        bag (string); Name of bag to ingest - if blank, will display all requested ingests
    """

    if bag == "":
        # Ingest requested items (bags) not yet ingested
        bags = get_digitized_bags([etd['mmsid'] for etd in get_requested_etds(".*")])
    else:
        bags = [bag]

    if bags == []:
        return "No items found ready for ingest"
    return check_missing([get_mmsid(bag) for bag in bags])

@app.task()
def verify_good_bags(bag="", collection="",): #dspace_endpoint=REST_ENDPOINT):
    """
    Ingest a bagged thesis or dissertation into dspace

    args:
       bag (string); Name of bag to ingest - if blank, will ingest all non-ingested items
       collection (string); dspace collection id to load into - if blank, will determine from Alma
    """

    if bag == "":
        # Ingest requested items (bags) not yet ingested
        bags = get_digitized_bags([etd['mmsid'] for etd in get_requested_etds(".*")])
    else:
        bags = [bag]

    if bags == []:
        return "No items found ready for ingest"

    collections = defaultdict(list)
    # initialize failed with bags with missing metadata
    failed = {}
    good_bags = []
    for bag in bags:
        if check_missing(get_mmsid(bag))[0][1] != []:
            failed[bag] = "Missing required metadata in Alma - contact cataloging group"
        else:
            good_bags.append(bag)
    # files to include in ingest
    # check missing returns the mmsid and a list of missing values
    return good_bags 

