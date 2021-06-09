 
from ckan.plugins.core import SingletonPlugin, implements
from ckan.lib.helpers import json
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters import HarvesterBase

from ckan import model
from ckan.model import Session
from ckan.logic import get_action

from string import Template

import os
import logging

log = logging.getLogger(__name__)
base_url = '/srv/app/data/harvest/'
base_download_url = "http://127.0.0.1:8080/"

class LocalFoldersHarvester(HarvesterBase):

  def info(self):
    return {
      'name': 'localfolders',
      'title': 'LocalFolders',
      'description': 'Custom harvester for local folders'
    }

  def get_original_url(self, harvest_object_id):
    raise NotImplementedError("Not implemented")

  def _get_dataset_notes(self, root, dataset_name, base_download_url):
    path = os.path.join(root, dataset_name, dataset_name)+".md"
    try:
      with open(path) as file:
        content = file.read()
      result = Template(content).substitute(base_url = os.path.join(base_download_url,dataset_name))
      return result
    except:
      return ""

  def _get_dataset_infos(self, root, dataset_name):
    path = os.path.join(root, dataset_name, dataset_name)+".json"
    try:
      with open(path) as file:
        data = json.load(file)

      tags = []
      for cur in data['tags']:
        tags.append({"name" : cur})
      
      data['tags'] = tags
      return data
    except:
      return {}

  def gather_stage(self, harvest_job):
    '''
    harvestJob.source content : {'_sa_instance_state': , 'frequency': 'ALWAYS', 'user_id': '', 'active': True, 'created': datetime.datetime(2021, 5, 15, 22, 43, 30, 458741), 'description': '', 'url': 'dataset_1', 'next_run': None, 'publisher_id': '', 'type': 'localfolders', 'config': '', 'title': 'dataset_1_title', 'id': 'e613a12e-e216-4f79-90ad-ec71b100f501'}
    :param harvest_job: HarvestJob object
    :returns: A list of HarvestObject ids
    '''
    objs_ids = []
    full_url = base_url+harvest_job.source.url
    log.info("In gather stage: %s" % full_url)

    for (root, dirs, files) in os.walk(full_url, topdown=True):
      dirs[:] = [d for d in dirs if d not in set(['data'])]
      log.info("Harvest folder : "+str(root))

      for cur_dir in dirs:

        if os.path.isdir(os.path.join(root, cur_dir, "data")):
          log.info("Found dataset in : "+str(os.path.join(root, cur_dir)))

          notes = self._get_dataset_notes(root, cur_dir, base_download_url)
          metadata = self._get_dataset_infos(root, cur_dir)

          for (sub_root, sub_dirs, sub_files) in os.walk( os.path.join(root,cur_dir,"data") ):

            resources = []
            relative_path = os.path.relpath(sub_root, root)
            download_path = os.path.join(base_download_url, relative_path)

            for sub_file in sub_files:

              resources.append({
                'name': sub_file,
                'url': os.path.join(download_path, sub_file)
              })

            if(len(resources) > 0):
              name = relative_path.replace('/data/', '/').replace('/', '_')
              log.info("New dataset : "+name)

              content = {
                "id" : harvest_job.source.id+name,
                "private" : False,
                "name" : name,
                "resources" : resources,
                "notes" : notes,
                "tags" : metadata['tags']
              }

              obj = HarvestObject(guid=harvest_job.source.id+name,
                                  job=harvest_job,
                                  content=json.dumps(content))
              obj.save()
              objs_ids.append(obj.id)

    log.info("Gather stage finished")
    return objs_ids

  def fetch_stage(self, harvest_object):
    '''
    :param harvest_object: HarvestObject object
    :returns: True if successful, 'unchanged' if nothing to import after
              all, False if not successful
    '''
    log.info("In fetch stage")
    return True

  def _get_owner(self, harvest_object):
    context = {
      'model': model,
      'session': Session,
      'user': 'sysadmin',
      'ignore_auth': True,
    }

    source_dataset = get_action('package_show')(
      context.copy(),
      {'id': harvest_object.source.id}
    )

    return source_dataset.get('owner_org')

  def import_stage(self, harvest_object):
    '''
    :param harvest_object: HarvestObject object
    :returns: True if the action was done, "unchanged" if the object didn't
              need harvesting after all or False if there were errors.
    '''
    log.info("In import stage")

    package_dict = json.loads(harvest_object.content)
    package_dict['owner_org'] = self._get_owner(harvest_object)
    result = self._create_or_update_package(package_dict, harvest_object, package_dict_form='package_show')

    return result

class NotImplementedError(Exception):
  pass 