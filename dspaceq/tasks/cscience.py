from celery.task import task
import os,json,requests, zipfile, StringIO,grp
from  glob import iglob
import pandas as pd
import numpy as np
from cscience_saf import _saf_builder, mkdir_p,uniquify
from subprocess import CalledProcessError , check_output,call

def _dspace_command(cmd):
    try:
        check_output(['sh','/srv/shareok/dspace/bin/dspace'] + cmd.split(' '))
    except CalledProcessError as e:
        raise Exception("Error: \n {0} \n Command: \n {1}".format(e.output,cmd))

def _get_metadata(stageDir):
    df_meta=None
    df_data=None
    for filename in iglob("{0}/*.xlsx".format(stageDir)):
        df = pd.read_excel(filename)
        columns= [x.lower() for x in df.columns]
        if "internal id" in columns and "taxonomy" in columns and "link" in columns:
            df_meta = df
        else:
            df_data = df
    if df_meta is None and df_data is None:
        raise Exception("Excel files could not be located.")
    return df_meta,df_data


@task()
def ingest_cscience_data(dropbox_data_url,cs_collection='11244/28096',destination_path='/tmp',eperson="libir@ou.edu",dspace_ingest=False):
    #create data staging area
    task_id = str(ingest_cscience_data.request.id)
    stageDir = os.path.join(destination_path, task_id)
    os.makedirs(stageDir)
    #set permissions
    os.chmod(stageDir, 0o775)
    os.chown(stageDir, -1, grp.getgrnam("tomcat").gr_gid)

    # get zip file and store it localy to destination path
    r = requests.get(dropbox_data_url.replace("dl=0","dl=1"), stream=True)
    z = zipfile.ZipFile(StringIO.StringIO(r.content))
    z.extractall(stageDir)
    #get excel file from dropbox
    df_meta,df_data = _get_metadata(stageDir)
    #Export metadata from shareok cscience collection
    outfile = "{0}/cs_data.csv".format(stageDir)
    cmd = "metadata-export -i {0} -f {1}".format(cs_collection,outfile)
    _dspace_command(cmd)
    #Load dspace CS data into dataframe
    cs_data= pd.read_csv("{0}/cs_data.csv".format(stageDir))
    result_tmpl =[]

    if isinstance(df_data, type(pd.DataFrame())):
        # Existing items
        existing_item = pd.merge(df_data,cs_data,right_on='dwc.npdg.sampleid[]',left_on='Sample ID')
        existing_item.columns=[s.split('(')[0].strip().replace('# of isolates from ','').split('[')[0].replace(".","_").replace(" ","_").strip().lower() for s in existing_item.columns]
        existing_item =existing_item.replace(np.nan, '', regex=True)
        existing_item.columns = list(uniquify(existing_item.columns))

        #existing_item = pd.merge(df_data,cs_data,right_on='dwc.npdg.sampleid[]',left_on='Sample ID')[cs_data.columns]
        #existing_item.columns=[s.split('[')[0].replace(".","_") for s in cs_data.columns]
        # New Items
        temp=pd.merge(df_data,cs_data,right_on='dwc.npdg.sampleid[]',left_on='Sample ID',how='left')
        temp =temp[pd.isnull(temp["dwc.npdg.sampleid[]"])] 
        new_item= temp[df_data.columns]
        new_item.columns=[s.split('(')[0].strip().replace('# of isolates from ','').replace(".","_").replace(" ","_").strip().lower() for s in df_data.columns]
        new_item =new_item.replace(np.nan, '', regex=True)
        new_item.columns = list(uniquify(new_item.columns))
        #temp=pd.merge(df_data,cs_data,right_on='dwc.npdg.sampleid[]',left_on='Sample ID',how='left')
        #temp =temp[pd.isnull(temp["dwc.npdg.sampleid[]"])]
        #new_item= temp[df_data.columns]

        #make saf
        cmd_tmpl = "import --{0} --eperson={1} --collection={2} --source={3} --mapfile={4}"
        wrkDir = _saf_builder(stageDir,existing_item)
        if wrkDir:
            dspace_cmd=cmd_tmpl.format("replace",eperson,cs_collection,wrkDir,os.path.join(wrkDir,"mapfile"))
            if dspace_ingest:
                _dspace_command(dspace_cmd)
        #return new_item.columns
        wrkDir = _saf_builder(stageDir,new_item)
        if wrkDir:
            dspace_cmd=cmd_tmpl.format("add",eperson,cs_collection,wrkDir,os.path.join(wrkDir,"mapfile"))
            if dspace_ingest:
                _dspace_command(dspace_cmd)
        result_tmpl.append("SAF's generated {0}".format(stageDir))
    if isinstance(df_meta, type(pd.DataFrame())):
        result_tmpl.append("Update Wiki need to code")

    return ";".join(result_tmpl)