#!/usr/bin/env python3

import json
import subprocess
import errno
import os
import sys
import base64
import requests
from pathlib import Path
from dateutil import parser

with open("config.json") as config_json:
    config = json.load(config_json)

product = {}

def handleXNAT(dataset):
    #decrypt xnat secret
    configenckey = str(Path.home())+"/.ssh/configEncrypt.key"
    if "BRAINLIFE_CONFIGENCKEY" in os.environ:
        configenckey = os.environ["BRAINLIFE_CONFIGENCKEY"]

    openssl = subprocess.Popen(["openssl", "rsautl", "-inkey", configenckey, "-decrypt"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    openssl.stdin.write(base64.b64decode(dataset["storage_config"]["secretEnc"]))
    secret = openssl.communicate()[0]
    openssl.stdin.close()

    #pull all parameters we need to upload file to xnat
    dataset_id=dataset["dataset_id"]
    hostname=dataset["storage_config"]["hostname"]
    token=dataset["storage_config"]["token"]
    project=dataset["storage_config"]["project"]
    path=dataset["storage_config"]["path"]
    datadir=dataset["dir"]

    subject=dataset["storage_config"]["meta"]["subject"]

    #"nosession" must be agreed in app-archive / warehouse/config.index.js
    session="nosession"
    if dataset["storage_config"]["meta"]["session"]:
        session = dataset["storage_config"]["meta"]["session"]

    datatype_id=dataset["datatype_id"]
    datatype_name=dataset["datatype_name"]
    create_date=dataset["create_date"]

    auth=(token, secret)

    #create necessary container to upload files to
    url=hostname+"/data/projects/"+project+"/subjects/"+subject
    ret = requests.put(url, auth=auth)
    print("creating sucject", ret)

    url+="/experiments/"+session

    date = parser.parse(create_date)
    ret = requests.put(url+"?xnat:mrSessionData/date="+date.strftime("%x"), auth=auth)
    print("creating session(experiment)", ret)

    #https://wiki.xnat.org/display/XAPI/Image+Session+Scans+API
    url+="/scans/"+dataset_id
    ret = requests.put(url+"?xsiType=xnat:mrScanData&xnat:mrScanData/type="+datatype_name+"&xnat:mrScanData/note"+datatype_id, auth=auth)
    print("creating scan", ret)

    print("zipping and uploading file", ret)
    zip_process = subprocess.Popen(["zip", "-r", "-", "."], stdout=subprocess.PIPE, cwd=datadir)

    formdata = {
        'upload': ('xnat.zip', zip_process.stdout),
    }
    params = {
        'extract': True
    }
    url+="/"+path
    ret = requests.post(url, files=formdata, params=params, auth=auth)
    print("uploading file", ret)

    #upload provenance info (can't get streaming to work..)
    prov = requests.get(dataset["provURL"])
    formdata = {
        'upload': ('provenance.json', prov.content)
    }
    ret = requests.post(url, files=formdata, auth=auth)
    print("uploading prov file", ret)

def handleLocal(dataset, storage):
    dest=os.environ["BRAINLIFE_ARCHIVE_"+storage]+"/"+dataset["project"]
    try:
        os.makedirs(dest)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(dest):
            pass
        else:
            raise

    tarname=dest+"/"+dataset_id+".tar"
    cmd=["tar", "hcvf", tarname]
    files=[]

    if "files" in dataset and dataset["files"] != None:
        #old archive method used !
        #we need to re-stage everything (using dataset._id as staging dirname) so that we can handle file_override
        #let's use dataset id as staging dir inside input directory (bad?)
        stagedir = dataset["dir"]+"/"+dataset_id

        files.append("-C")
        files.append(stagedir)

        #now we need to set symlink for all file/dir
        for file in dataset["files"]:
            if "filename" in file:
                path = file["filename"]
            else:
                path = file["dirname"]

            over_path = path
            if "files_override" in dataset and dataset["files_override"] != None:
                if file["id"] in dataset["files_override"]:
                    over_path = dataset["files_override"][file["id"]]
                    print("file override", file["id"], over_path)

            if path == ".":
                stagedir=dataset["dir"]
                if over_path != ".":
                    stagedir += "/"+over_path
                print("dot(.) path used.. using all local files as %s" % stagedir)
                files = ["-C", stagedir]
                files.extend(os.listdir(stagedir))
                break

            src_path = over_path
            link_path = stagedir
            if path != ".":
                src_path = "../"+over_path
                link_path = stagedir+"/"+path

            print("making sure stagedir exists", stagedir)
            #I only have to do this once, but I have to do it before I create the first symlink
            #I also don't know if I have over_path == "." until I start looping.. 
            #let's just check this for each file
            try: 
                os.makedirs(stagedir)
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    pass
                else:
                    raise

            print("making symlink src:", src_path, "dest:", link_path, "(dest is the link to create)")
            try: 
                os.symlink(src_path, link_path)
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    os.remove(link_path)
                    os.symlink(src_path, link_path)
                else:
                    raise

            #some file/dir are not required
            if os.path.exists(dataset["dir"]+"/"+over_path):
                files.append(path)
            elif file["required"] == True:
                print("required file is missing:"+dataset["dir"]+"/"+path)
                sys.exit(1)

        #dedupe the files
        files_dedupe = []
        for f in files:
          if f not in files_dedupe:
            files_dedupe.append(f)
        files = files_dedupe

    else:
        #new method is clean! just grab everything under "dir"
        files.append("-C")
        files.append(dataset["dir"])
        for file in os.listdir(dataset["dir"]):
            files.append(file)

    #archive!
    cmd.extend(files)
    subprocess.call(cmd)

    #get file size 
    product[dataset_id] = {"size":os.path.getsize(tarname)}

#####################################################################
# main loop..
#####################################################################
for dataset in config["datasets"]:
    #moved dataset["dataset"] is now deprecated
    if "dataset_id" in dataset:
        dataset_id = dataset["dataset_id"]
    else:
        dataset_id = dataset["dataset"]["_id"]

    storage=dataset["storage"]
    if storage == "xnat":
        handleXNAT(dataset)
    else:
        handleLocal(dataset, storage)

with open('product.json', 'w') as productjson:
    json.dump(product, productjson)



