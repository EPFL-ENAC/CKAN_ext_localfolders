 
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

  def gather_stage(self, harvest_job):
    '''
    :param harvest_job: HarvestJob object
    :returns: A list of HarvestObject ids
    '''
    full_url = base_url+harvest_job.source.url

    log.info("Infos:")
    log.info(harvest_job.source.__dict__)

    log.info("In gather stage: %s" % full_url)
    objs_ids = []
    counter = 0

    for (root, dirs, files) in os.walk(full_url):

      resources = []

      log.info("Into folder : "+str(root))

      for file in files:

        log.info("Added file : "+str(file))

        resources.append({
          'name': str(file),
          'resource_type': 'HTML',
          'format': 'HTML',
          'url': 'undefined'
        })


      if(len(files) > 0):

        content = {
          "id" : str(root),
          "private" : False,
          "name" : harvest_job.source.title,
          "resources" : resources
        }

        obj = HarvestObject(guid=full_url+str(counter),
                            job=harvest_job,
                            content=json.dumps(content))
        obj.save()
        objs_ids.append(obj.id)

        counter = counter + 1

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
    log.info("content" + str(harvest_object.content))

    package_dict = json.loads(harvest_object.content)
    package_dict['owner_org'] = self._get_owner(harvest_object)
    result = self._create_or_update_package(package_dict, harvest_object, package_dict_form='package_show')

    return result

class NotImplementedError(Exception):
  pass 