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
from requests import put
from json import loads as decode
from json import dumps as encode

def chase(object, fields):
  try:
    for selection in fields:
      object = object[selection]
    return object
  except KeyError:
    return None
  except IndexError:
    return None

def get_getter(args, config):
  cluster = args['<cluster>']
  conn_str = config['ConnectionStrings'][cluster]
  def getter(path):
    url = "http://{conn_str}/{path}".format(conn_str=conn_str, path=path)
    try:
      resp = get(url)
      if not resp.ok:
        resp.raise_for_status()
      else:
        return decode(resp.content)
    except Exception as e:
      raise e
  return getter

def get_setter(args, config):
  cluster = args['<cluster>']
  conn_str = config['ConnectionStrings'][cluster]
  def setter(path, data):
    url = "http://{conn_str}/{path}".format(conn_str=conn_str, path=path)
    try:
      json_data = encode(data)
      resp = put(url, data=json_data)
      if not resp.ok:
        resp.raise_for_status()
    except Exception as e:
      raise e
  return setter

def verb_list(args, config):
  if args['<cluster>'] is not None:
    getter = get_getter(args, config)
    es_settings = getter("_settings")
    print "\n".join(es_settings.keys())
  else:
    print "\n".join(config['ConnectionStrings'].keys())
  exit(0)

def verb_replicas(args, config):
  index = args['<index>']
  path = "{index}/_settings".format(index=index)
  if args['<value>']:
    setter = get_setter(args, config)
    index_settings = {"number_of_replicas": int(args['<value>'])}
    setter(path, index_settings)
  else:
    getter = get_getter(args, config)
    index_settings = getter(path)
    print chase(index_settings, [index]+"settings.index.number_of_replicas".split("."))
  exit(0)

def verb_shards(args, config):
  getter = get_getter(args, config)
  index = args['<index>']
  index_settings = getter("{index}/_settings".format(index=index))
  print chase(index_settings, [index]+"settings.index.number_of_shards".split("."))
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
  esmgr [options] <cluster> list
  esmgr [options] <cluster> <index> (replicas|shards) [<value>]

Options:
  -h --help        Show this screen.
  --version        Show version.
  --config=<conf>  Comma separated config files.
"""

verb_map = {
 'list': verb_list,
 'replicas': verb_replicas,
 'shards': verb_shards
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
