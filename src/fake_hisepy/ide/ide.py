import requests
import os

import utils as cu
from auth import get_from_metadata_server, get_bearer_token_header

_here = os.path.abspath(os.path.dirname(__file__))
CONFIG = cu.read_yaml('{}/config.yaml'.format(_here))


def stop_ide():
    ''' Stops/Terminates the active instance that is calling this function. '''
    # get IDE name
    this_ide_name = get_from_metadata_server(
        CONFIG['AUTHORIZE']['INSTANCE_NAME_PATH'])
    obj = cu.parse_hise_response(
        requests.request("POST",
                         "https://{s}/{tool}/{ide}/stop".format(
                             s=get_from_metadata_server(
                                 CONFIG['AUTHORIZE']['SERVER_ID_PATH']),
                             tool=CONFIG['TOOLCHAIN']['TOOLCHAIN_IDE'],
                             ide=this_ide_name),
                         headers=get_bearer_token_header()))
    if obj is None:
        raise SystemError('unable to find IDE: {}'.format(this_ide_name))
    else:
        print('{} has successfully been stopped'.format(this_ide_name))


def suspend_ide():
    ''' Suspends the active instance that is calling this function. '''
    # get IDE name
    this_ide_name = get_from_metadata_server(
        CONFIG['AUTHORIZE']['INSTANCE_NAME_PATH'])
    obj = cu.parse_hise_response(
        requests.request("POST",
                         "https://{s}/{tool}/{ide}/suspend".format(
                             s=get_from_metadata_server(
                                 CONFIG['AUTHORIZE']['SERVER_ID_PATH']),
                             tool=CONFIG['TOOLCHAIN']['TOOLCHAIN_IDE'],
                             ide=this_ide_name),
                         headers=get_bearer_token_header()))
    if obj is None:
        raise SystemError('unable to find IDE: {}'.format(this_ide_name))
    else:
        print('{} has successfully been suspended'.format(this_ide_name))
