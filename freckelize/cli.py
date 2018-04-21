# -*- coding: utf-8 -*-

"""Console script for freckelize."""
from __future__ import absolute_import, division, print_function

import logging
import sys
from collections import OrderedDict

import click_completion
import click_log
from frkl import frkl
from luci import vars_file, readable_json

from freckles.freckles_base_cli import FrecklesBaseCommand
from freckles.freckles_defaults import *
from freckles.utils import DEFAULT_FRECKLES_CONFIG, RepoType
from . import print_version
from .freckelize import FreckleDetails, FreckleRepo, Freckelize
# from .freckle_detect import create_freckle_descs
from .utils import FreckelizeAdapterReader, FreckelizeAdapterFinder

log = logging.getLogger("freckles")
click_log.basic_config(log)

# optional shell completion
click_completion.init()

# TODO: this is a bit ugly, probably have refactor how role repos are used
# nsbl.defaults.DEFAULT_ROLES_PATH = os.path.join(os.path.dirname(__file__), "external", "default_role_repo")

VARS_ARG_HELP = "extra variables for this adapter, can be overridden by cli options if applicable"
VARS_ARG_METAVAR = "VARS"
DEFAULTS_HELP = "default variables, can be used instead (or in addition) to user input via command-line parameters"
KEEP_METADATA_HELP = "keep metadata in result directory, mostly useful for debugging"
FRECKELIZE_EPILOG_TEXT = "frecklecute is free and open source software and part of the 'freckles' project, for more information visit: https://docs.freckles.io"

FRECKLE_ARG_HELP = "the url or path to the freckle(s) to use, if specified here, before any commands, all profiles will be applied to it"
FRECKLE_ARG_METAVAR = "URL_OR_PATH"
TARGET_ARG_HELP = 'target folder for freckle checkouts (if remote url provided), defaults to folder \'freckles\' in users home'
TARGET_ARG_METAVAR = "PATH"
TARGET_NAME_ARG_HELP = "target name for freckle checkouts (if remote url provided), can not be used when multiple freckle folders are specified"
TARGET_NAME_ARG_METAVAR = "FOLDER_NAME"
INCLUDE_ARG_HELP = 'if specified, only process folders that end with one of the specified strings, only applicable for multi-freckle folders'
INCLUDE_ARG_METAVAR = 'FILTER_STRING'
EXCLUDE_ARG_HELP = 'if specified, omit process folders that end with one of the specified strings, takes precedence over the include option if in doubt, only applicable for multi-freckle folders'
EXCLUDE_ARG_METAVAR = 'FILTER_STRING'
ASK_PW_HELP = 'whether to force ask for a password, force ask not to, or let freckles decide (which might not always work)'
ASK_PW_CHOICES = click.Choice(["auto", "true", "false"])
NON_RECURSIVE_HELP = "whether to exclude all freckle child folders, default: false"

DEFAULT_FRECKELIZE_ROLES_PATH = os.path.join(os.path.dirname(__file__), "external", "roles")
DEFAULT_FRECKELIZE_ADAPTERS_PATH = os.path.join(os.path.dirname(__file__), "external", "adapters")
DEFAULT_FRECKELIZE_BLUEPRINTS_PATH = os.path.join(os.path.dirname(__file__), "external", "blueprints")
DEFAULT_USER_ADAPTERS_PATH = os.path.join(os.path.expanduser("~"), ".freckles", "frecklecutables")

class FreckelizeCommand(FrecklesBaseCommand):
    """Class to build the frecklecute command-line interface."""

    FRECKELIZE_ARGS = [(
        "freckle", {
            "required": False,
            "alias": "freckle",
            "doc": {
                "help": FRECKLE_ARG_HELP
            },
            "click": {
                "option": {
                    "multiple": True,
                    "param_decls": ["--freckle", "-f"],
                    "type": RepoType(),
                    "metavar": FRECKLE_ARG_METAVAR
                }
            }
        }),
        ("profile_extra_vars", {
            "alias": "vars",
            "required": False,
            "type": list,
            "doc": {
                "help": VARS_ARG_HELP
            },
            "click": {
                "option": {
                    "metavar": VARS_ARG_METAVAR,
                    "multiple": True,
                    "type": vars_file
                }
            }
        }),
        ("target_folder", {
            "alias": "target-folder",
            "required": False,
            # "default": "~/freckles",
            "type": str,
            "doc": {
                "help": TARGET_ARG_HELP
            },
            "click": {
                "option": {
                    "param_decls": ["--target-folder", "-t"],
                    "metavar": TARGET_ARG_METAVAR
                }
            }
        }),
        ("target_name", {
            "alias": "target-name",
            "required": False,
            "type": str,
            "doc": {
                "help": TARGET_NAME_ARG_HELP
            },
            "click": {
                "option": {
                    "metavar": TARGET_NAME_ARG_METAVAR
                }
            }
        }),
        ("include", {
            "alias": "include",
            "required": False,
            "doc": {
                "help": INCLUDE_ARG_HELP
            },
            "click": {
                "option": {
                    "param_decls": ["--include", "-i"],
                    "multiple": True,
                    "metavar": INCLUDE_ARG_METAVAR
                }
            }
        }),
        ("exclude", {
            "alias": "exclude",
            "required": False,
            "doc": {
                "help": EXCLUDE_ARG_HELP
            },
            "click": {
                "option": {
                    "param_decls": ["--exclude", "-e"],
                    "multiple": True,
                    "metavar": EXCLUDE_ARG_METAVAR
                }
            }
        }),
        # ("ask_become_pass", {
        #     "alias": "ask-become-pass",
        #     "doc": {
        #         "help": ASK_PW_HELP
        #     },
        #     "click": {
        #         "option": {
        #             "param_decls": ["--ask-become-pass", "-pw"],
        #             "type": ASK_PW_CHOICES
        #         }
        #     }
        # }),
        ("non_recursive", {
            "alias": "non-recursive",
            "type": bool,
            "required": False,
            "default": False,
            "doc": {
                "help": NON_RECURSIVE_HELP
            },
            "click": {
                "option": {
                    "is_flag": True
                }
            }
        })
    ]

    @staticmethod
    def freckelize_extra_params():

        freckle_option = click.Option(param_decls=["--freckle", "-f"], required=False, multiple=True, type=RepoType(),
                                  metavar=FRECKLE_ARG_METAVAR, help=FRECKLE_ARG_HELP)
        target_option = click.Option(param_decls=["--target-folder", "-t"], required=False, multiple=False, type=str,
                                     metavar=TARGET_ARG_METAVAR,
                                     help=TARGET_ARG_HELP)
        target_name_option = click.Option(param_decls=["--target-name"], required=False, multiple=False, type=str,
                                     metavar=TARGET_NAME_ARG_METAVAR,
                                     help=TARGET_NAME_ARG_HELP)
        include_option = click.Option(param_decls=["--include", "-i"],
                                      help=INCLUDE_ARG_HELP,
                                      type=str, metavar=INCLUDE_ARG_METAVAR, default=[], multiple=True)
        exclude_option = click.Option(param_decls=["--exclude", "-e"],
                                      help=EXCLUDE_ARG_HELP,
                                      type=str, metavar=EXCLUDE_ARG_METAVAR, default=[], multiple=True)
        parent_only_option = click.Option(param_decls=["--non-recursive"],
                                          help=NON_RECURSIVE_HELP,
                                          is_flag=True,
                                          default=False,
                                          required=False,
                                          type=bool
        )

        params = [freckle_option, target_option, target_name_option, include_option, exclude_option,
                           parent_only_option]

        return params


    def __init__(self, extra_params=None, print_version_callback=print_version, **kwargs):

        config = DEFAULT_FRECKLES_CONFIG
        config.add_repo(DEFAULT_FRECKELIZE_ROLES_PATH)
        config.add_repo(DEFAULT_FRECKELIZE_ADAPTERS_PATH)
        config.add_repo(DEFAULT_FRECKELIZE_BLUEPRINTS_PATH)
        config.add_user_repo(DEFAULT_USER_ADAPTERS_PATH)

        extra_params = FreckelizeCommand.freckelize_extra_params()
        super(FreckelizeCommand, self).__init__(config=config, extra_params=extra_params, print_version_callback=print_version_callback, **kwargs)
        self.config = DEFAULT_FRECKLES_CONFIG
        self.reader = FreckelizeAdapterReader()
        self.finder = None

    def get_dictlet_finder(self):

        if self.finder is None:
            # need to wait for paths to be initialized
            self.finder =  FreckelizeAdapterFinder(self.paths)

        return self.finder

    def get_dictlet_reader(self):

        return self.reader

    def get_additional_args(self):

        return OrderedDict(FreckelizeCommand.FRECKELIZE_ARGS)

    def freckles_process(self, command_name, default_vars, extra_vars, user_input, metadata, dictlet_details, config, parent_params, command_var_spec):

        result = {"name": command_name, "default_vars": default_vars, "extra_vars": extra_vars, "user_input": user_input, "adapter_metadata": metadata, "adapter_details": dictlet_details}

        return result


def assemble_freckelize_run(*args, **kwargs):

    no_run = kwargs.get("no_run")
    hosts = list(kwargs["host"])
    if not hosts:
        hosts = ["localhost"]

    default_target = kwargs.get("target_folder", None)
    default_target_name = kwargs.get("target_name", None)

    default_freckle_urls = list(kwargs.get("freckle", []))
    default_output_format = kwargs.get("output", "default")

    default_include = list(kwargs.get("include", []))
    default_exclude = list(kwargs.get("exclude", []))

    default_password = kwargs.get("password", None)
    default_non_recursive = kwargs.get("non_recursive", None)

    default_extra_vars_list = list(kwargs.get("vars", []))
    default_extra_vars = OrderedDict()
    for ev in default_extra_vars_list:
        frkl.dict_merge(default_extra_vars, ev, copy_dct=False)

    parent_command_vars = {}
    if default_target:
        parent_command_vars["target_folder"] = default_target
    if default_target_name:
        parent_command_vars["target_folder_name"] = default_target_name
    if default_include:
        parent_command_vars["includes"] = default_include
    if default_exclude:
        parent_command_vars["includes"] = default_include
    if default_non_recursive is not None:
        parent_command_vars["non_recursive"] = default_non_recursive

    freckle_details = []
    if not args[0]:

        # fill missing keys with default values
        if "target_folder" not in parent_command_vars.keys():
            parent_command_vars["target_folder"] = DEFAULT_FRECKLE_TARGET_MARKER
        if "target_folder_name" not in parent_command_vars.keys():
            parent_command_vars["target_folder_name"] = None
        if "include" not in parent_command_vars.keys():
            parent_command_vars["include"] = []
        if "exclude" not in parent_command_vars.keys():
            parent_command_vars["exclude"] = []
        if "non_recursive" not in parent_command_vars.keys():
            parent_command_vars["non_recursive"] = False

        prio = 1000
        freckle_repos = []
        # TODO: pre-fill with adapter-defaults?
        for freckle in default_freckle_urls:
            repo = FreckleRepo(freckle, target_folder=parent_command_vars["target_folder"], target_name=parent_command_vars["target_folder_name"], include=parent_command_vars["include"], exclude=parent_command_vars["exclude"], non_recursive=parent_command_vars["non_recursive"], priority=prio, default_vars={}, overlay_vars={"freckle": default_extra_vars})
            prio = prio + 100
            freckle_repos.append(repo)

        details = FreckleDetails(freckle_repos, profiles_to_run=None)
        freckle_details.append(details)
    else:

        multi_freckle_repos = OrderedDict()
        det_prio = 10000
        for p in args[0]:
            pn = p["name"]
            # if pn in profiles.keys():
                # raise Exception("Profile '{}' specified twice. I don't think that makes sense. Exiting...".format(pn))
            metadata = {}
            metadata = {}
            metadata["metadata"] = p["adapter_metadata"]
            metadata["details"] = p["adapter_details"]

            pvars_adapter_defaults = p["default_vars"]

            pvars_extra_vars = p["extra_vars"]
            pvars_user_input = p["user_input"]

            pvars_profile_extra_vars = pvars_user_input.pop("profile_extra_vars", ())

            freckle_default_vars = OrderedDict()
            for ev in pvars_extra_vars:
                frkl.dict_merge(freckle_default_vars, ev, copy_dct=False)

            pvars = OrderedDict()
            for ev in pvars_profile_extra_vars:
                frkl.dict_merge(pvars, ev, copy_dct=False)
            frkl.dict_merge(pvars, pvars_user_input, copy_dct=False)

            freckles = list(pvars.pop("freckle", []))
            include = list(set(pvars.pop("include", [])))
            exclude = list(set(pvars.pop("exclude", [])))
            target_folder = pvars.pop("target_folder", None)
            target_name = pvars.pop("target_name", None)

            # ask_become_pass = pvars.pop("ask_become_pass", None)
            # if ask_become_pass is None:
                # ask_become_pass = default_ask_become_pass

            non_recursive = pvars.pop("non_recursive", False)

            if non_recursive is None:
                non_recursive = default_non_recursive

            log.debug("Merged vars for profile: freckle".format(pn))
            log.debug(readable_json(freckle_default_vars, indent=2))
            log.debug("Merged vars for profile: {}".format(pn))
            log.debug(readable_json(pvars, indent=2))

            all_freckles_for_this_profile = freckles + default_freckle_urls
            if len(all_freckles_for_this_profile) > 1 and target_name is not None:
                raise Exception("Can't use 'target_name' if more than one folders are specified")

            prio = 1000
            freckle_repos = []
            for freckle in all_freckles_for_this_profile:

                repo = FreckleRepo(freckle, target_folder=target_folder, target_name=target_name, include=include, exclude=exclude, non_recursive=non_recursive, priority=prio, default_vars={pn: pvars_adapter_defaults}, overlay_vars={pn: pvars, "freckle": freckle_default_vars})
                prio = prio + 100
                freckle_repos.append(repo)

            details = FreckleDetails(freckle_repos, profiles_to_run=pn, detail_priority=det_prio)
            freckle_details.append(details)
            det_prio = det_prio + 1000

    if default_password is None:
        default_password = "no"

    if default_password == "ask":
        password = click.prompt("Please enter sudo password for this run", hide_input=True)
        click.echo()
        default_password = False
        # TODO: check password valid
    elif default_password == "ansible":
        default_password = True
        password = None
    elif default_password == "no":
        default_password = False
        password = None
    else:
        raise click.ClickException("Can't process password: {}".format(default_password))

    try:
        f = Freckelize(freckle_details, ask_become_pass=default_password, password=password)
        f.execute(hosts=hosts, no_run=no_run, output_format=default_output_format)
    except (Exception) as e:
        raise click.ClickException(str(e))

    sys.exit(0)


@click.command(name="freckelize", cls=FreckelizeCommand, epilog=FRECKELIZE_EPILOG_TEXT, subcommand_metavar="ADAPTER", invoke_without_command=True, result_callback=assemble_freckelize_run, chain=True)
@click_log.simple_verbosity_option(log, "--verbosity")
@click.pass_context
def cli(ctx, **kwargs):
    """Downloads a remote dataset or code (called a 'freckle') and sets up your local environment to be able to handle the data, according to the data's profile.

    Ideally the remote dataset includes all the metadata that is needed to setup the environment, but it's possible to provide some directives using commandline options globally (--target, --include, --exclude), or per adapter (use the --help function on each adapter to view those).

    Locally available adapters for supported profiles are listed below, each having their own configuration. You can specify a 'global' url by adding it's '--freckle' option before any of the subsequently specified adapters, prompting any of those adapters to apply their tasks to it. Or you can assign one (or multiple) freckle to an adapter by providing it after the adapter name.

    For more details, visit the online documentation: https://docs.freckles.io/en/latest/freckelize_command.html
    """

if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
