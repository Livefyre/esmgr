#!/usr/bin/env python
# Buckets timestamps by some data property.

import pdb
from pprint import pformat, pprint
import os
from pyyacc.parser import build
from docopt import docopt
from functools import partial
from operator import ge, le
from requests import get
from json import loads as decode
from json import dumps as encode

def get_getter(args, config):
  cluster = args['<cluster>']
  conn_str = config['ConnectionStrings'][cluster]
  def getter(path):
    url = "http://{conn_str}/{path}".format(conn_str=conn_str, path=path)
    try:
      resp = get(url)
      if resp.status_code != 200:
        resp.raise_for_status()
      else:
        return decode(resp.content)
    except Exception as e:
      raise e
  return getter

def verb_list(args, config):
  if args['<cluster>'] is not None:
    getter = get_getter(args, config)
    es_settings = getter("_settings")
    print "\n".join(es_settings.keys())
  else:
    print "\n".join(config['ConnectionStrings'].keys())
  exit(0)

def verb_config(args, config):
    raise NotImplementedError()
    exit(0)

def verb_status(args, config):
    raise NotImplementedError()
    exit(0)

def check_wrapper(check, args, config):
    def check_logic(args, config, result, output):
        crit = int(args['<critical>'])
        warn = int(args['<warn>'])

        if crit >= warn:
          cmp = ge
        elif crit < warn:
          cmp = le

        if cmp(result, crit):
            print 'CRITICAL:', output
            exit(2)
        elif cmp(result,warn):
            print 'WARN:', output
            exit(1)
        else:
            print 'SUCCESS:', output
            exit(0)
    try:
        results = check(args, config)
    except Exception as e:
        print "UNKNOWN:", type(e), e
        exit(3)
    else:
      check_logic(args, config, *results)

usage = \
"""
esmgr. A Elastic Search cluster management tool.

Usage:
  esmgr [options] list
  esmgr [options] <cluster> (list|config|status)

Options:
  -h --help        Show this screen.
  --version        Show version.
  --config=<conf>  Comma separated config files.
"""

verb_map = {
 'list': verb_list,
 'config': verb_config,
 'status': verb_status,
}

_ROOT = os.path.abspath(os.path.dirname(__file__))

def main():
    args = docopt(usage)
    app_yaml = os.path.join(_ROOT, "app.yaml")
    if args['--config'] is not None:
      config_paths = args['--config'].split(",")
    else:
      config_paths = list()
    config_files = [app_yaml] + config_paths
    builder, settings_dict = build(*config_files)
    verbs = [function for (name, function) in verb_map.items() if args[name]]
    assert len(verbs) == 1
    verbs[0](args, settings_dict['search'])

#TODO REMOVE?
if __name__ == "__main__":
    main()
