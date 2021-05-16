 
from ckan.plugins.core import SingletonPlugin, implements
from ckan.lib.helpers import json
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.harvesters import HarvesterBase

from ckan import model
from ckan.model import Session
from ckan.logic import get_action

import os
import logging

log = logging.getLogger(__name__)
base_url = '/srv/app/data/harvest/'
base_download_url = "download_url/"

class LocalFoldersHarvester(HarvesterBase):

  def info(self):
    return {
      'name': 'localfolders',
      'title': 'LocalFolders',
      'description': 'Custom harvester for local folders'
    }

  def get_original_url(self, harvest_object_id):
    raise NotImplementedError("Not implemented")

  def _get_dataset_notes(self, root, dataset_name):
    path = os.path.join(root, dataset_name)+".md"
    try:
      with open(path) as file:
        content = file.read()
      return content
    except:
      return ""

  def _get_dataset_infos(self, root, dataset_name):
    path = os.path.join(root, dataset_name)+".json"
    log.info("Looking for infos in file  %s" % path)
    try:
      with open(path) as file:
        data = json.load(file)
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

    for (root, dirs, files) in os.walk(full_url):
      log.info("Harvest folder : "+str(root))

      for cur_dir in dirs:

        notes = self._get_dataset_notes(root, cur_dir)
        metadata = self._get_dataset_infos(root, cur_dir)
        log.info("DEBUG ")
        log.info(str(metadata))

        for (sub_root, sub_dirs, sub_files) in os.walk( os.path.join(full_url,cur_dir) ):

          resources = []

          for sub_file in sub_files:
            log.info("Added file : "+str(sub_file))

            resources.append({
              'name': sub_file,
              #'resource_type': 'HTML',
              #'format': 'HTML',
              'url': 'undefined'
            })

          if(len(resources) > 0):
            name = (os.path.relpath(sub_root, root)).replace('/', '_')
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

      break

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