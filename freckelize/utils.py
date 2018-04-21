# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import copy
import fnmatch
import logging
from collections import OrderedDict

import nsbl
from frkl import frkl
from luci import DictletFinder, TextFileDictletReader, JINJA_DELIMITER_PROFILES, replace_string, ordered_load, \
    readable_json

from freckles.freckles_base_cli import parse_tasks_dictlet
from freckles.freckles_defaults import *
from freckles.utils import DEFAULT_FRECKLES_CONFIG, freckles_jinja_extensions, RepoType

# from .freckle_detect import create_freckle_descs

log = logging.getLogger("freckles")

ADAPTER_CACHE = {}
DEFAULT_REPO_TYPE = RepoType()
DEFAULT_REPO_PRIORITY = 10000
METADATA_CONTENT_KEY = "freckle_metadata_file_content"

BLUEPRINT_CACHE = {}

def get_available_blueprints(config=None):
    """Find all available blueprints."""

    log.debug("Looking for blueprints...")
    if not config:
        config = DEFAULT_FRECKLES_CONFIG

    repos = nsbl.tasks.get_local_repos(config.trusted_repos, DEFAULT_LOCAL_REPO_PATH_BASE, DEFAULT_ROLE_REPOS, DEFAULT_ABBREVIATIONS)

    result = {}
    for repo in repos:

        blueprints = get_blueprints_from_repo(repo)
        for name, path in blueprints.items():
            result[name] = path

    log.debug("Found blueprints:")
    log.debug(readable_json(result, indent=2))

    return result

def get_blueprints_from_repo(blueprint_repo):
    """Find all blueprints under a folder.

    A blueprint is a folder that has a .blueprint.freckle marker file in it's root.
    """
    if not os.path.exists(blueprint_repo) or not os.path.isdir(os.path.realpath(blueprint_repo)):
        return {}

    if blueprint_repo in BLUEPRINT_CACHE.keys():
        return BLUEPRINT_CACHE[blueprint_repo]

    result = {}

    try:

        for root, dirnames, filenames in os.walk(os.path.realpath(blueprint_repo), topdown=True, followlinks=True):
            dirnames[:] = [d for d in dirnames if d not in DEFAULT_EXCLUDE_DIRS]
            for filename in fnmatch.filter(filenames, "*.{}".format(BLUEPRINT_MARKER_EXTENSION)):
                blueprint_metadata_file = os.path.realpath(os.path.join(root, filename))
                blueprint_folder = os.path.abspath(os.path.dirname(blueprint_metadata_file))

                #profile_name = ".".join(os.path.basename(blueprint_metadata_file).split(".")[1:2])
                profile_name = os.path.basename(blueprint_metadata_file).split(".")[0]

                result[profile_name] = blueprint_folder

    except (UnicodeDecodeError) as e:
        click.echo(" X one or more filenames in '{}' can't be decoded, ignoring. This can cause problems later. ".format(root))

    BLUEPRINT_CACHE[blueprint_repo] = result

    return result


def find_freckelize_adapters(path):
    """Helper method to find freckelize adapters.

    Adapter files are named in the format: <adapter_name>.adapter.freckle

    Args:
      path (str): the root path (usually the path to a 'trusted repo').
    Returns:
      list: a list of valid freckelize adapters under this path
    """

    log.debug("Finding adapters in: {}".format(path))
    if not os.path.exists(path) or not os.path.isdir(os.path.realpath(path)):
        return {}

    if path in ADAPTER_CACHE.keys():
        return ADAPTER_CACHE[path]

    result = {}
    try:
        for root, dirnames, filenames in os.walk(os.path.realpath(path), topdown=True, followlinks=True):

            dirnames[:] = [d for d in dirnames if d not in DEFAULT_EXCLUDE_DIRS]

            for filename in fnmatch.filter(filenames, "*.{}".format(ADAPTER_MARKER_EXTENSION)):
                adapter_metadata_file = os.path.realpath(os.path.join(root, filename))
                # adapter_folder = os.path.abspath(os.path.dirname(adapter_metadata_file))
                # profile_name = ".".join(os.path.basename(adapter_metadata_file).split(".")[1:2])

                profile_name = os.path.basename(adapter_metadata_file).split(".")[0]

                result[profile_name] = {"path": adapter_metadata_file, "type": "file"}

    except (UnicodeDecodeError) as e:
        click.echo(" X one or more filenames in '{}' can't be decoded, ignoring. This can cause problems later. ".format(root))

    ADAPTER_CACHE[path] = result

    return result



class FreckelizeAdapterFinder(DictletFinder):
    """Finder class for freckelize adapters.

    """

    def __init__(self, paths, **kwargs):

        super(FreckelizeAdapterFinder, self).__init__(**kwargs)
        self.paths = paths
        self.adapter_cache = None
        self.path_cache = {}

    def get_all_dictlet_names(self):

        return self.get_all_dictlets().keys()

    def get_all_dictlets(self):
        """Find all freckelize adapters."""

        log.debug("Retrieving all dictlets")

        if self.adapter_cache is None:
            self.adapter_cache = {}
        dictlet_names = OrderedDict()

        all_adapters = OrderedDict()

        for path in self.paths:
            if path not in self.path_cache.keys():

                adapters = find_freckelize_adapters(path)
                self.path_cache[path] = adapters
                frkl.dict_merge(all_adapters, adapters, copy_dct=False)
                frkl.dict_merge(self.adapter_cache, adapters, copy_dct=False)

        return all_adapters

    def get_dictlet(self, name):

        log.debug("Retrieving adapter: {}".format(name))
        if self.adapter_cache is None:
            self.get_all_dictlet_names()

        dictlet = self.adapter_cache.get(name, None)

        if dictlet is None:
            return None
        else:
            return dictlet

class FreckelizeAdapterReader(TextFileDictletReader):
    """Reads a text file and generates metadata for freckelize.

    The file needs to be in yaml format, if it contains a key 'args' the value of
    that is used to generate the freckelize command-line interface.
    The key 'defaults' is used for, well, default values.

    Read more about how the adapter file format: XXX

    Args:
      delimiter_profile (dict): a map describing the delimiter used for templating.
      **kwargs (dict): n/a
    """

    def __init__(self, delimiter_profile=JINJA_DELIMITER_PROFILES["luci"], **kwargs):

        super(FreckelizeAdapterReader, self).__init__(**kwargs)
        self.delimiter_profile = delimiter_profile
        self.tasks_keyword = FX_TASKS_KEY_NAME

    def process_lines(self, content, current_vars):

        log.debug("Processing content: {}".format(content))

        result = parse_tasks_dictlet(content, current_vars)
        return result


    def process_lines_old(self, content, current_vars):

        log.debug("Processing content: {}".format(content))

        # now, I know this isn't really the most
        # optimal way of doing this,
        # but I don't really care that much about execution speed yet,
        # plus I really want to be able to use variables used in previous
        # lines of the content
        last_whitespaces = 0
        current_lines = ""
        temp_vars = copy.deepcopy(current_vars)

        for line in content:

            if line.strip().startswith("#"):
                continue

            whitespaces = len(line) - len(line.lstrip(' '))
            current_lines = "{}{}\n".format(current_lines, line)
            if whitespaces <= last_whitespaces:

                temp = replace_string(current_lines, temp_vars, **self.delimiter_profile)

                if not temp.strip():
                    continue

                temp_dict = ordered_load(temp)
                if temp_dict:
                    temp_vars = frkl.dict_merge(temp_vars, temp_dict, copy_dct=False)

            last_whitespaces = whitespaces

        if current_lines:
            temp = replace_string(current_lines, temp_vars, additional_jinja_extensions=freckles_jinja_extensions, **self.delimiter_profile)
            temp_dict = ordered_load(temp)
            temp_vars = frkl.dict_merge(temp_vars, temp_dict, copy_dct=False)

        frkl.dict_merge(current_vars, temp_vars, copy_dct=False)
        log.debug("Vars after processing:\n{}".format(readable_json(current_vars, indent=2)))

        return current_vars
