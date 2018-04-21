# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function

import copy
import json
import logging
import operator
import sys
import tempfile
import uuid
from collections import OrderedDict

import yaml
from cookiecutter.main import cookiecutter
from frkl import frkl
from luci import output, ordered_load, readable_json, readable_raw, add_key_to_dict
from nsbl.output import print_title
from six import string_types

from freckles.freckles_base_cli import process_extra_task_lists, create_external_task_list_callback, \
    get_task_list_format
from freckles.freckles_defaults import *
from freckles.utils import DEFAULT_FRECKLES_CONFIG, expand_repos, create_and_run_nsbl_runner
# from .freckle_detect import create_freckle_descs
from .utils import get_available_blueprints, FreckelizeAdapterReader, FreckelizeAdapterFinder, DEFAULT_REPO_TYPE, \
    DEFAULT_REPO_PRIORITY, METADATA_CONTENT_KEY

log = logging.getLogger("freckles")

# for debug purposes, sometimes it's easier to read the output if list of files is not present in folder metadata. This will make some adapters not work though.
ADD_FILES = True


class FreckleRepo(object):
    """Model class containing all relevant freckle repo paramters.

    If providing a string as 'source' argument, it'll be converted into a dictionary using :meth:`freckles.utils.RepoType.convert`.

    The format of the source dictionary is::

        {"url": <freckle_repo_url_or_path>,
         "branch": <optional branch if git repo>}

    Args:
      source (str, dict): the source repo/path
      target_path (str): the local target path
      target_name (str): the local target name
      include (list): a list of strings that specify which sub-freckle folders to use (TODO: include link to relevant documetnation)
      exclude (list): a list of strings that specify which sub-freckle folders to exclude from runs (TODO: include link to relevant documetnation)
      non_recursive (bool): whether to only use the source base folder, not any containing sub-folders that contain a '.freckle' marker file
      priority (int): the priority of this repo (determines the order in which it gets processed)
      default_vars (dict): default values to be used for this repo (key: profile_name, value: vars)
      overlay_vars (dict): overlay values to be used for this repo (key: profile_name, value: vars), those will be overlayed after the checkout process
    """
    def __init__(self, source, target_folder=None, target_name=None, include=None, exclude=None, non_recursive=False, priority=DEFAULT_REPO_PRIORITY, default_vars=None, overlay_vars=None):

        if not source:
            raise Exception("No source provided")

        if isinstance(source, string_types):
            temp_source = DEFAULT_REPO_TYPE.convert(source, None, None)
        else:
            temp_source = source

        self.id = str(uuid.uuid4())

        # self.id = temp_source["url"]

        self.priority = DEFAULT_REPO_PRIORITY
        self.source = temp_source
        if target_folder is None:
            target_folder = DEFAULT_FRECKLE_TARGET_MARKER
        self.target_folder = target_folder
        self.target_name = target_name
        if include is None:
            include = []
        self.include = include
        if exclude is None:
            exculde = []
        self.exclude = exclude
        self.non_recursive = non_recursive

        if default_vars is None:
            default_vars = OrderedDict()
        self.default_vars = default_vars
        if overlay_vars is None:
            overlay_vars = OrderedDict()
        self.overlay_vars = overlay_vars

        self.target_become = False
        self.source_delete = False

        self.repo_desc = None

    def add_default_vars(self, vars_dict):

        frkl.dict_merge(self.default_vars, vars_dict, copy_dct=False)

    def add_overlay_vars(self, vars_dict):

        frkl.dict_merge(self.overlay_vars, vars_dict, copy_dct=False)

    def set_priority(self, priority):

        self.priority = priority

    def __repr__(self):

        return("{}: {}".format("FreckleRepo", readable_raw(self.__dict__)))

    def expand(self, config, default_target=None):
        """Expands or fills in details using the provided configuration.

        Args:
          config (FrecklesConfig): the freckles configuration object
        """
        log.debug("Expanding repo: {}".format(self.source))

        if default_target is None:
            default_target = config.default_freckelize_target

        if self.source["url"].startswith("{}:".format(BLUEPRINT_URL_PREFIX)) or self.source["url"].startswith("{}:".format(BLUEPRINT_DEFAULTS_URL_PREFIX)):

            blueprint_type = self.source["url"].split(":")[0]
            if blueprint_type == BLUEPRINT_DEFAULTS_URL_PREFIX:
                blueprint_defaults = True
            else:
                blueprint_defaults = False

            blueprint_name = ":".join(self.source["url"].split(":")[1:])
            blueprints = get_available_blueprints(config)
            match = blueprints.get(blueprint_name, False)

            if not match:
                raise Exception("No blueprint with name '{}' available.".format(blueprint_name))

            cookiecutter_file = os.path.join(match, "cookiecutter.json")

            if os.path.exists(cookiecutter_file):

                temp_path = tempfile.mkdtemp(prefix='frkl.')

                if blueprint_defaults:
                    log.debug("Found interactive blueprints, but using defaults...")
                    cookiecutter(match, output_dir=temp_path, no_input=True)
                else:
                    click.secho("\nFound interactive blueprint, please enter approriate values below:\n", bold=True)

                    cookiecutter(match, output_dir=temp_path)
                    click.echo()


                subdirs = [os.path.join(temp_path, f) for f in os.listdir(temp_path) if os.path.isdir(os.path.join(temp_path, f))]

                if len(subdirs) != 1:
                    raise Exception("More than one directories created by interactive template '{}'. Can't deal with that.".format(match))

                url = subdirs[0]
                self.source_delete = True

            else:
                url = match

            if self.target_folder == DEFAULT_FRECKLE_TARGET_MARKER:
                self.target_folder = default_target

        else:
            url = self.source["url"]


        repo_desc = {}
        repo_desc["blueprint"] = False

        if os.path.exists(os.path.realpath(os.path.expanduser(url))):

            p = os.path.realpath(os.path.expanduser(url))
            if os.path.isfile(p):
                # assuming archive
                repo_desc["type"] = "local_archive"
                repo_desc["remote_url"] = p
                repo_desc["source_delete"] = False
                if self.target_folder == DEFAULT_FRECKLE_TARGET_MARKER:
                    repo_desc["local_parent"] = default_target
                else:
                    repo_desc["local_parent"] = self.target_folder
                repo_desc["checkout_skip"] = False
                if self.target_name is None:
                    repo_desc["local_name"] = os.path.basename(p).split(".")[0]
                else:
                    repo_desc["local_name"] = self.target_name
            else:
                repo_desc["type"] = "local_folder"
                repo_desc["remote_url"] = p
                if self.target_folder == DEFAULT_FRECKLE_TARGET_MARKER:
                    if self.target_name is None:
                        repo_desc["local_parent"] = os.path.dirname(p)
                        repo_desc["checkout_skip"] = True
                    else:
                        repo_desc["local_parent"] = default_target
                        repo_desc["checkout_skip"] = False
                else:
                    repo_desc["local_parent"] = self.target_folder
                    repo_desc["checkout_skip"] = False
                if self.target_name is None:
                    repo_desc["local_name"] = os.path.basename(p)
                else:
                    repo_desc["local_name"] = self.target_name
                repo_desc["source_delete"] = self.source_delete

        elif url.endswith(".git"):

            repo_desc["type"] = "git"
            repo_desc["remote_url"] = url
            if self.target_name is None:
                repo_desc["local_name"] = os.path.basename(repo_desc["remote_url"])[:-4]
            else:
                repo_desc["local_name"] = self.target_name
            repo_desc["checkout_skip"] = False
            if self.target_folder == DEFAULT_FRECKLE_TARGET_MARKER:
                repo_desc["local_parent"] = DEFAULT_FRECKELIZE_TARGET_FOLDER
            else:
                repo_desc["local_parent"] = self.target_folder

            if self.source.get("branch", False):
                repo_desc["remote_branch"] = self.source["branch"]

        elif url.startswith("http://") or url.startswith("https://"):
            # TODO: check whether host is local
            repo_desc["type"] = "remote_archive"
            repo_desc["remote_url"] = url
            if self.local_name is None:
                repo_desc["local_name"] = os.path.basename(url).split('.')[0]
            else:
                repo_desc["local_name"] = self.local_name
            repo_desc["source_delete"] = False
            if self.target_folder == DEFAULT_FRECKLE_TARGET_MARKER:
                repo_desc["local_parent"] = default_target
            else:
                repo_desc["local_parent"] = self.target_folder

            repo_desc["checkout_skip"] = False

        else:
            raise Exception("freckle url format unknown, and no valid local path, don't know how to handle that: {}".format(url))


        if repo_desc["local_parent"] == DEFAULT_FRECKLE_TARGET_MARKER:
            raise Exception("default_target can't be set to '{}'".format(DEFAULT_FRECKLE_TARGET_MARKER))
        if not os.path.isabs(repo_desc["local_parent"]) and not repo_desc["local_parent"].startswith("~"):
            raise Exception("Relative path not supported for target folder option, please use an absolute one (or use '~' to indicate a home directory): {}".format(default_target))

        if repo_desc["local_parent"].startswith("~") or repo_desc["local_parent"].startswith("/home"):
            repo_desc["checkout_become"] = False
        else:
            repo_desc["checkout_become"] = True

        repo_desc["include"] = self.include
        repo_desc["exclude"] = self.exclude
        repo_desc["non_recursive"] = self.non_recursive
        repo_desc["id"] = self.id
        repo_desc["priority"] = self.priority

        repo_desc["add_file_list"] = ADD_FILES

        self.repo_desc = repo_desc

        return repo_desc


class FreckleDetails(object):
    """Model class containing all relevant freckle run parameters for a repo.

    If 'freckle_repos' is not a list, it'll be converted into one with itsel as the only item.
    If one of the itesm in the 'freckle_repos' list is a string, it'll be converted to a :class:`FreckleRepo` using default arguments.

    Args:
      freckle_repos (str, list): a list of :class:`FreckleRepo` objects
      profiles_to_run (OrderedDict): an ordered dict with the profile names to run as keys, and potential overlay vars as value
    """

    def __init__(self, freckle_repos, profiles_to_run=None, detail_priority=DEFAULT_REPO_PRIORITY):

        self.freckle_repos = []
        if not isinstance(freckle_repos, (list, tuple)):
            temp_freckle_repos = [freckle_repos]
        else:
            temp_freckle_repos = freckle_repos

        for r in temp_freckle_repos:
            if isinstance(r, string_types):
                r = FreckleRepo(r)
            elif isinstance(r, FreckleRepo):
                self.freckle_repos.append(r)
            else:
                raise Exception("Can't add object of type '{}' to FreckleDetails.".format(type(r)))

        if isinstance(profiles_to_run, string_types):
            profiles_to_run = [profiles_to_run]
        self.profiles_to_run = profiles_to_run
        self.set_priority(detail_priority)

    def set_priority(self, priority):

        self.priority = priority
        p = 0
        for fr in self.freckle_repos:
            fr.set_priority(self.priority+p)
            p = p + 1000

    def expand_repos(self, config, default_target=None):

        result = []
        for repo in self.freckle_repos:
            result.append(repo.expand(config, default_target))

        return result

    def __repr__(self):

        return("{}: {}".format("FreckleDetails", readable_raw(self.__dict__)))

class Freckelize(object):
    """Class to configure and execute a freckelize run.

    A freckelize run consists of two 'sub-runs': the checkout run, which (if necessary) checks-out/copies
    the repo/folder in question and reads it's metadata, and the actual processing run, which
    installs and configures the environment using that metadata as configuration.

    If the provided 'freckle_details' value is a single item, it gets converted into a list. If one of
    the items is a string, it gets converted into a :class:`FreckleDetails` object.

    Args:
      freckle_details (list): a list of :class:`FreckleDetails` objects
      config (FreckleConfig): the configuration to use for this run
      ask_become_pass (bool): whether Ansible should ask the user for a password if necessary
      password (str): the password to use
    """
    def __init__(self, freckle_details, config=None, ask_become_pass=False, password=None):

        if isinstance(freckle_details, string_types):
            temp_freckle_details = [FreckleDetails(freckle_details)]
        elif isinstance(freckle_details, FreckleRepo):
            temp_freckle_details = [FreckleDetails(freckle_details)]
        elif isinstance(freckle_details, FreckleDetails):
            temp_freckle_details = [freckle_details]
        else:
            temp_freckle_details = freckle_details

        self.ask_become_pass = ask_become_pass
        self.password = password

        self.freckle_details = []

        # to be populated after checkout
        self.freckles_metadata = None
        self.profiles = None
        self.freckle_profile = None
        self.repo_lookup = None

        base_priority = 0
        p = 0
        for d in temp_freckle_details:
            if isinstance(d, string_types):
                d = FreckleDetails(d)

            d.set_priority(base_priority+p)
            self.freckle_details.append(d)
            p = p + 10000

        if config is None:
            config = DEFAULT_FRECKLES_CONFIG
        self.config = config

        paths = [p['path'] for p in expand_repos(config.trusted_repos)]
        self.finder = FreckelizeAdapterFinder(paths)
        self.reader = FreckelizeAdapterReader()
        self.all_repos = OrderedDict()

        for f in self.freckle_details:
            f.expand_repos(self.config)
            for fr in f.freckle_repos:
                self.all_repos[fr.id] = fr

    def start_checkout_run(self, hosts=None, no_run=False, output_format="default"):

        if hosts is None:
            hosts = ["localhost"]

        if isinstance(hosts, (list, tuple)):
            if len(hosts) > 1:
                raise Exception("More than one host not supported (for now).")

        log.debug("Starting checkout run, using those repos:")

        for id, r in self.all_repos.items():
            log.debug(readable_json(r.repo_desc, indent=2))

        if not self.all_repos:
            log.info("No freckle repositories specified, doing nothing...")
            return

        print_title("starting freckelize run(s)...")
        click.echo()
        extra_profile_vars = {}

        repo_metadata_file = "repo_metadata"

        extra_profile_vars = {}
        # extra_profile_vars.setdefault("freckle", {})["no_run"] = bool(no_run)

        repos = []
        for id, r in self.all_repos.items():
            repos.append(r.repo_desc)

        task_config = [{"vars": {"freckles": repos, "user_vars": extra_profile_vars, "repo_metadata_file": repo_metadata_file}, "tasks": ["freckles_checkout"]}]

        result_checkout = create_and_run_nsbl_runner(task_config, output_format=output_format, ask_become_pass=self.ask_become_pass, password=self.password,
                                            no_run=no_run, run_box_basics=True, hosts_list=hosts)

        playbook_dir = result_checkout["playbook_dir"]

        repo_metadata_file_abs = os.path.join(playbook_dir, os.pardir, "logs", repo_metadata_file)

        return_code = result_checkout["return_code"]

        if return_code != 0:
            click.echo("Checkout phase failed, not continuing...")
            sys.exit(1)

        click.echo()

        all_repo_metadata = json.load(open(repo_metadata_file_abs))
        # TODO: delete file?
        folders_metadata = self.read_checkout_metadata(all_repo_metadata)
        (self.freckles_metadata, self.repo_lookup) = self.prepare_checkout_metadata(folders_metadata)

        # allow for multiple hosts in the future

        freckle_profile_folders = self.freckles_metadata.get("freckle")

        freckle_profile = {}  # this is just for easy lookup by path
        for folder in freckle_profile_folders:
            freckle_profile[folder["folder_metadata"]["full_path"]] = folder
            repo = self.all_repos[folder["folder_metadata"]["parent_repo_id"]]
            default_vars = repo.default_vars.get("freckle", {})
            overlay_vars = repo.overlay_vars.get("freckle", {})
            final_vars = self.process_folder_vars(folder["folder_vars"], default_vars, overlay_vars)
            folder["default_vars"] = default_vars
            folder["overlay_vars"] = overlay_vars
            folder["vars"] = final_vars

        profiles_map = self.calculate_profiles_to_run()
        for profile, folders in profiles_map.items():

            for folder in folders:
                repo = self.all_repos[folder["folder_metadata"]["parent_repo_id"]]
                default_vars = repo.default_vars.get(profile, {})
                overlay_vars = repo.overlay_vars.get(profile, {})
                path = folder["folder_metadata"]["full_path"]
                base_vars = freckle_profile[path]["vars"]
                folder["base_vars"] = base_vars
                folder["default_vars"] = default_vars
                folder["overlay_vars"] = overlay_vars

                final_vars = self.process_folder_vars(folder["folder_vars"], default_vars, overlay_vars, base_vars)
                folder["vars"] = final_vars

        self.profiles = [(hosts[0], profiles_map)]
        self.freckle_profile = [(hosts[0], freckle_profile)]

        log.debug("Using freckle details:")
        log.debug(readable_json(self.freckle_profile, indent=2))
        log.debug("Using profile details:")
        log.debug(readable_json(self.profiles, indent=2))

        return (self.freckle_profile, self.profiles)

    def process_folder_vars(self, folder_vars, default_vars, overlay_vars, base_vars={}):

        final_vars = frkl.dict_merge(default_vars, folder_vars, copy_dct=True)
        if base_vars:
            frkl.dict_merge(final_vars, base_vars, copy_dct=False)
        frkl.dict_merge(final_vars, overlay_vars, copy_dct=False)

        return final_vars

    def execute(self, hosts=["localhost"], no_run=False, output_format="default"):

        metadata = self.start_checkout_run(hosts=hosts, no_run=False, output_format=output_format)

        if metadata is None:
            return None

        self.start_freckelize_run(no_run=no_run, output_format=output_format)

    def start_freckelize_run(self, no_run=False, output_format="default"):

        if self.freckles_metadata is None:
            raise Exception("Checkout not run yet, can't continue.")

        log.debug("Starting freckelize run...")

        host = self.profiles[0][0]
        hosts_list = [host]
        freckelize_metadata = self.profiles[0][1]
        freckelize_freckle_metadata = self.freckle_profile[0][1]
        valid_adapters, adapters_files_map = self.create_adapters_files_map(freckelize_metadata.keys())

        task_list_aliases = {}
        for name, details in adapters_files_map.items():
            task_list_aliases[name] = details["play_target"]

        if not valid_adapters:
            click.echo("No valid adapters found, doing nothing...")
            return None

        # special case for 'ansible-tasks'
        if "ansible-tasks" in valid_adapters.keys():
            # it's still possible to add the confirmation via an extra var file,
            # but I think that's ok. Happy to hear suggestions if you think this is
            # too risky though.
            p_md = freckelize_metadata["ansible-tasks"]
            confirmation = False
            for md in p_md:
                if md["overlay_vars"].get("ansible_tasks_user_confirmation", False):
                    confirmation = True
                    break;
            if not confirmation:
                raise click.ClickException("As the ansible-tasks adapter can execute arbitrary code, user confirmation is necessary to  use this adatper. Consult the output of 'freckelize ansible-tasks --help' or XXX for more information.")

        tasks_for_callback = []
        for ad, details in valid_adapters.items():
            tasks_for_callback.append(details)

        callback = create_external_task_list_callback(adapters_files_map, tasks_for_callback)
        additional_roles = self.get_adapter_dependency_roles(valid_adapters.keys())

        sorted_adapters = self.sort_adapters_by_priority(valid_adapters.keys())

        click.echo()
        print_title("using adapters:", title_char="-")
        for a in sorted_adapters:
            click.echo()
            click.echo("  - ", nl=False)
            click.secho(a, bold=True, nl=False)
            click.echo(":")
            click.secho("      path", bold=True, nl=False)
            click.echo(": {}".format(valid_adapters[a]["path"]))
            click.secho("      folders", bold=True, nl=False)
            click.echo(":")
            for folder in freckelize_metadata[a]:
                full_path = folder["folder_metadata"]["full_path"]
                click.echo("         - {}".format(full_path))

        click.echo()

        task_config = [
            {"vars": {},
             "tasks": [{"freckles":
                        # {"user_vars": {},
                         {"freckelize_profiles_metadata": freckelize_metadata,
                         "freckelize_freckle_metadata": freckelize_freckle_metadata,
                         "profile_order": sorted_adapters,
                          "task_list_aliases": task_list_aliases}}]}]

        additional_repo_paths = []

        result = create_and_run_nsbl_runner(
            task_config, output_format=output_format, ask_become_pass=self.ask_become_pass, password=self.password,
            pre_run_callback=callback, no_run=no_run, additional_roles=additional_roles,
            run_box_basics=True, additional_repo_paths=additional_repo_paths, hosts_list=hosts_list)

        click.echo()
        if no_run:

            click.secho("========================================================", bold=True)
            click.echo()
            click.echo("'no-run' was specified, not executing freckelize run.")
            click.echo()
            click.echo("Variables that would have been used for an actual run:")
            click.echo()

            click.secho("Profiles:", bold=True)
            click.secho("--------", bold=True)
            for profile, folders in freckelize_metadata.items():
                click.echo()
                click.secho("profile: ", bold=True, nl=False)
                click.echo("{}".format(profile))
                click.echo()
                for folder in folders:
                    folder_metadata = folder["folder_metadata"]
                    click.secho("  path: ", bold=True, nl=False)
                    click.echo(folder_metadata["full_path"])
                    if folder["vars"]:
                        click.secho("  vars: ", bold=True, nl=True)
                        output(folder["vars"], output_type="yaml", indent=4, nl=False)
                        click.echo(u"\u001b[2K\r", nl=False)
                    else:
                        click.secho("  vars: ", bold=True, nl=False)
                        click.echo("none")
                    if folder["extra_vars"]:
                        click.secho("  extra vars:", bold=True)
                        output(folder["extra_vars"], output_type="yaml", indent=4)
                    else:
                        click.secho("  extra_vars: ", bold=True, nl=False)
                        click.echo("none")
            click.echo()


    def sort_adapters_by_priority(self, adapters):

        if not adapters:
            return []

        prios = []

        for adapter in adapters:

            metadata = self.get_adapter_metadata(adapter)
            priority = metadata.get("__freckles__", {}).get("adapter_priority", DEFAULT_FRECKELIZE_PROFILE_PRIORITY)
            prios.append([priority, adapter])

        profiles_sorted = sorted(prios, key=lambda tup: tup[0])
        return [item[1] for item in profiles_sorted]

    def get_adapter_dependency_roles(self, adapters):

        if not adapters:
            return []

        all_deps = set()
        for adapter in adapters:

            metadata = self.get_adapter_metadata(adapter)
            roles = metadata.get("__freckles__", {}).get("roles", [])
            all_deps |= set(roles)

        return list(all_deps)

    def get_adapter_details(self, adapter):

        adapter_details = self.finder.get_dictlet(adapter)
        return adapter_details

    def get_adapter_metadata(self, adapter):

        adapter_details = self.get_adapter_details(adapter)
        if adapter_details is None:
            return None
        adapter_metadata = self.reader.read_dictlet(adapter_details, {}, {})

        return adapter_metadata

    def create_adapters_files_map(self, adapters):

        files_map = {}
        valid_adapters = OrderedDict()

        for adapter in adapters:
            adapter_metadata = self.get_adapter_metadata(adapter)
            if adapter_metadata is None:
                log.warn("No adapter '{}' found: skipping".format(adapter))
                continue

            adapter_path = self.get_adapter_details(adapter)["path"]
            extra_task_lists_map = process_extra_task_lists(adapter_metadata, adapter_path)

            tasks = adapter_metadata.get("tasks", [])
            try:
                tasks_dict = yaml.safe_load(tasks)
            except (Exception) as e:
                raise Exception("Could not parse tasks string: {}".format(tasks))

            if not tasks_dict:
                log.warn("Adapter '{}' doesn't specify any tasks: skipping".format(adapter))
                continue

            task_list_format = get_task_list_format(tasks_dict)
            if task_list_format == "freckles":
                log.warning("Task list for adapter '{}' is 'freckles' format, this is not supported (for now). Ignoring...".format(adapter))
                continue

            intersection = set(files_map.keys()) & set(extra_task_lists_map.keys())
            if intersection:
                raise Exception("Can't execute frecklecute run, adapters {} share the same task_list keys: {}".format(adapters, intersection))

            files_map.update(extra_task_lists_map)

            valid_adapters[adapter] = {"path": adapter_path, "tasks": tasks_dict, "tasks_string": tasks, "tasks_format": "ansible", "target_name": "task_list_{}.yml".format(adapter)}

        return (valid_adapters, files_map)

    def calculate_profiles_to_run(self):

        if self.freckles_metadata is None:
            raise Exception("Checkout not run yet, can't calculate profiles to run.")

        all_profiles = OrderedDict()
        for fd in self.freckle_details:
            if fd.profiles_to_run is None:
                # run all __auto_run__ profiles
                run_map = OrderedDict()

                for repo in fd.freckle_repos:
                    fd_folders = self.get_freckle_folders_for_repo(repo.id)
                    for p, folders in fd_folders.items():
                        if p == "freckle":
                            continue
                        for folder in folders:
                            if not folder["folder_vars"].get("__auto_run__", True):
                                click.echo("  - auto-run disabled for profile '{}' in folder '{}', ignoring...".format(p, folder["folder_metadata"]["folder_name"]))
                            else:
                                all_profiles.setdefault(p, []).append(folder)
            else:
                for profile in fd.profiles_to_run:
                    for repo in fd.freckle_repos:
                        paths_to_get = copy.deepcopy(self.repo_lookup[repo.id])
                        profile_folders = self.get_freckle_folders_for_repo(repo.id)
                        # first check if there is a folder that has profile-specific vars
                        for f in profile_folders.get(profile, []):
                            full_path = f["folder_metadata"]["full_path"]
                            if full_path in paths_to_get:
                                log.debug("Using '{}' profile folder for path: {}".format(profile, full_path))
                                all_profiles.setdefault(profile, []).append(f)
                                paths_to_get.remove(full_path)

                        # if there are still folders left, we use the 'freckle' ones
                        if paths_to_get:
                            for f in profile_folders.get("freckle", []):
                                full_path = f["folder_metadata"]["full_path"]
                                if full_path in paths_to_get:
                                    log.debug("Using 'freckle' profile folder for path: {}".format(full_path))
                                    all_profiles.setdefault(profile, []).append(f)
                                    paths_to_get.remove(full_path)

                        if paths_to_get:
                            raise Exception("Could not find all folders for profile '{}'. Leftover: {}".format(profile, paths_to_get))

        return all_profiles

    def get_freckle_folders_for_repo(self, repo_id):

        if self.freckles_metadata is None:
            raise Exception("Checkout not run yet, can't calculate freckle folders.")

        repos = OrderedDict()

        for profile, details_list in self.freckles_metadata.items():

            for details in details_list:
                if details["folder_metadata"]["parent_repo_id"] == repo_id:
                    repos.setdefault(profile, []).append(details)

        return repos

    def prepare_checkout_metadata(self, folders_metadata):


        profiles_available = OrderedDict()
        all_folders = []

        repo_lookup = OrderedDict()

        for details in folders_metadata:
            extra_vars = details["extra_vars"]
            folder_metadata = details["folder_metadata"]
            folder_vars = details["vars"]

            repo_id = folder_metadata["parent_repo_id"]
            full_path = folder_metadata["full_path"]
            if full_path not in repo_lookup.setdefault(repo_id, []):
                repo_lookup[repo_id].append(full_path)

            profile_folder_vars = OrderedDict()
            for v in folder_vars:
                profile = v["profile"]["name"]
                if profile in profile_folder_vars.keys():
                    log.warn("Profile '{}' specified more than once in '{}', ignoring all but the last instance. Please check '{}' for details.".format(profile, os.path.join(folder_metadata["full_path"], ".freckle"), "https://XXX"))

                p_vars = v.get("vars", {})
                profile_folder_vars[profile] = p_vars


            for key, value in profile_folder_vars.items():
                profiles_available.setdefault(key, []).append({"folder_metadata": folder_metadata, "folder_vars": value, "extra_vars": extra_vars})

            if not "freckle" in profile_folder_vars.keys():
                profiles_available.setdefault("freckle", []).append({"folder_metadata": folder_metadata, "folder_vars": {}, "extra_vars": extra_vars})

        return (profiles_available, repo_lookup)

    def read_checkout_metadata(self, folders_metadata):

        temp_vars = OrderedDict()
        extra_vars = OrderedDict()
        folder_metadata_lookup = {}

        for metadata in folders_metadata:

            repo_id = metadata["parent_repo_id"]
            folder = metadata["full_path"]

            folder_metadata_lookup.setdefault(repo_id, {})[folder] = metadata

            raw_metadata = metadata.pop(METADATA_CONTENT_KEY, False)
            if raw_metadata:
                # md = yaml.safe_load(raw_metadata)
                md = ordered_load(raw_metadata)
                if not md:
                    md = []
                if isinstance(md, dict):
                    md_temp = []
                    for key, value in md.items():
                        md_temp.append({key: value})
                    md = md_temp
                    # if isinstance(md, (list, tuple)):
                    # md = {"vars": md}
            else:
                md = [{"profile": {"name": "freckle"}, "vars": {}}]

            temp_vars.setdefault(repo_id, {}).setdefault(folder, []).append(md)

            extra_vars_raw = metadata.pop("extra_vars", False)
            if extra_vars_raw:
                for rel_path, extra_metadata_raw in extra_vars_raw.items():
                    extra_metadata = ordered_load(extra_metadata_raw)
                    if not extra_metadata:
                        # this means there was an empty file. We interprete that as setting a flag to true
                        extra_metadata = True

                    #sub_path, filename = os.path.split(rel_path)
                    tokens = rel_path.split(os.path.sep)
                    last_token = tokens[-1]
                    if last_token.startswith("."):
                        last_token = last_token[1:]
                    else:
                        continue
                    if last_token.endswith(".freckle"):
                        last_token = last_token[0:-8]
                    else:
                        continue
                    tokens[-1] = last_token
                    add_key_to_dict(extra_vars.setdefault(repo_id, {}).setdefault(folder, {}), ".".join(tokens), extra_metadata)
                    # extra_vars.setdefault(folder, {}).setdefault(sub_path, {})[filename[1:-8]] = extra_metadata

        result = []
        for repo_id, folder_map in temp_vars.items():
            for freckle_folder, metadata_list in folder_map.items():
                chain = [frkl.FrklProcessor(DEFAULT_PROFILE_VAR_FORMAT)]
                try:
                    frkl_obj = frkl.Frkl(metadata_list, chain)
                    # mdrc_init = {"append_keys": "vars/packages"}
                    # frkl_callback = frkl.MergeDictResultCallback(mdrc_init)
                    frkl_callback = frkl.MergeResultCallback()
                    profile_vars_new = frkl_obj.process(frkl_callback)
                    item = {}
                    item["vars"] = profile_vars_new
                    item["extra_vars"] = extra_vars.get(repo_id, {}).get(freckle_folder, {})
                    item["folder_metadata"] = folder_metadata_lookup[repo_id][freckle_folder]
                    result.append(item)
                except (frkl.FrklConfigException) as e:
                    raise Exception(
                        "Can't read freckle metadata file '{}/.freckle': {}".format(freckle_folder, e.message))

        result.sort(key=lambda k: operator.itemgetter(k["folder_metadata"]["repo_priority"]))

        return result
