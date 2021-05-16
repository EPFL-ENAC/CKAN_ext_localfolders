 
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

  def validate_config(self, config):
    '''
    [optional]

    Harvesters can provide this method to validate the configuration
    entered in the form. It should return a single string, which will be
    stored in the database.  Exceptions raised will be shown in the form's
    error messages.

    :param harvest_object_id: Config string coming from the form
    :returns: A string with the validated configuration options
    '''
    return ''

  def get_original_url(self, harvest_object_id):
    raise NotImplementedError("Not implemented")

  def _get_dataset_notes(self, root, dataset_name):
    path = os.path.join(root, dataset_name)+".desc"

    if(os.path.isfile(path)):
      file = open(path, mode='r')
      content = file.read()
      file.close()
      return content
    else:
      return ""

  def gather_stage(self, harvest_job):
    '''
    :param harvest_job: HarvestJob object
    :returns: A list of HarvestObject ids
    '''
    full_url = base_url+harvest_job.source.url

    #{'_sa_instance_state': , 'frequency': 'ALWAYS', 'user_id': '', 'active': True, 'created': datetime.datetime(2021, 5, 15, 22, 43, 30, 458741), 'description': '', 'url': 'dataset_1', 'next_run': None, 'publisher_id': '', 'type': 'localfolders', 'config': '', 'title': 'dataset_1_title', 'id': 'e613a12e-e216-4f79-90ad-ec71b100f501'}

    log.info("In gather stage: %s" % full_url)
    objs_ids = []

    for (root, dirs, files) in os.walk(full_url):
      log.info("Harvest folder : "+str(root))

      for cur_dir in dirs:
        log.info("New dataset : "+str(cur_dir))

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

            content = {
              "id" : harvest_job.source.id+str(cur_dir),
              "private" : False,
              "name" : (cur_dir+"/"+sub_root).replace('/', '_'),
              "resources" : resources,
              "notes" : self._get_dataset_notes(root, cur_dir)
            }

            obj = HarvestObject(guid=harvest_job.source.id+str(cur_dir),
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