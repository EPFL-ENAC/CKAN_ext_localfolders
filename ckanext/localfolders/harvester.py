 
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


  def _get_owner(self):
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

  def gather_stage(self, harvest_job):
    '''
    The gather stage will receive a HarvestJob object and will be
    responsible for:
        - gathering all the necessary objects to fetch on a later.
          stage (e.g. for a CSW server, perform a GetRecords request)
        - creating the necessary HarvestObjects in the database, specifying
          the guid and a reference to its job. The HarvestObjects need a
          reference date with the last modified date for the resource, this
          may need to be set in a different stage depending on the type of
          source.
        - creating and storing any suitable HarvestGatherErrors that may
          occur.
        - returning a list with all the ids of the created HarvestObjects.
        - to abort the harvest, create a HarvestGatherError and raise an
          exception. Any created HarvestObjects will be deleted.

    :param harvest_job: HarvestJob object
    :returns: A list of HarvestObject ids
    '''
    log.info("In gather stage: %s" % harvest_job.source.url)
    objs_ids = []
    counter = 0

    for (root, dirs, files) in os.walk(harvest_job.source.url):

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
          "owner_org" : self._get_owner(),
          "private" : False,
          "name" : str(root),
          "resources" : resources
        }

        obj = HarvestObject(guid=harvest_job.source.url+str(counter),
                            job=harvest_job,
                            content=json.dumps(content))
        obj.save()
        objs_ids.append(obj.id)

        counter = counter + 1

    log.info("Gather stage finished")
    return objs_ids



  def fetch_stage(self, harvest_object):
    '''
    The fetch stage will receive a HarvestObject object and will be
    responsible for:
        - getting the contents of the remote object (e.g. for a CSW server,
          perform a GetRecordById request).
        - saving the content in the provided HarvestObject.
        - creating and storing any suitable HarvestObjectErrors that may
          occur.
        - returning True if everything is ok (ie the object should now be
          imported), "unchanged" if the object didn't need harvesting after
          all (ie no error, but don't continue to import stage) or False if
          there were errors.

    :param harvest_object: HarvestObject object
    :returns: True if successful, 'unchanged' if nothing to import after
              all, False if not successful
    '''
    log.info("In fetch stage")
    return True

  def import_stage(self, harvest_object):
    '''
    The import stage will receive a HarvestObject object and will be
    responsible for:
        - performing any necessary action with the fetched object (e.g.
          create, update or delete a CKAN package).
          Note: if this stage creates or updates a package, a reference
          to the package should be added to the HarvestObject.
        - setting the HarvestObject.package (if there is one)
        - setting the HarvestObject.current for this harvest:
          - True if successfully created/updated
          - False if successfully deleted
        - setting HarvestObject.current to False for previous harvest
          objects of this harvest source if the action was successful.
        - creating and storing any suitable HarvestObjectErrors that may
          occur.
        - creating the HarvestObject - Package relation (if necessary)
        - returning True if the action was done, "unchanged" if the object
          didn't need harvesting after all or False if there were errors.

    NB You can run this stage repeatedly using 'paster harvest import'.

    :param harvest_object: HarvestObject object
    :returns: True if the action was done, "unchanged" if the object didn't
              need harvesting after all or False if there were errors.
    '''
    log.info("In import stage")
    log.info("content" + str(harvest_object.content))

    package_dict = json.loads(harvest_object.content)
    result = self._create_or_update_package(package_dict, harvest_object, package_dict_form='package_show')
    return result

class NotImplementedError(Exception):
  pass 