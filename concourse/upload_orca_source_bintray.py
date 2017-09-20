#!/usr/bin/env python
import re
import subprocess
import os
import sys
import argparse

BASE_WD = os.getcwd()
ORCA_SRC_DIR=os.path.join(BASE_WD, 'orca_src')

GPORCA_VERSION_MAJOR_PATTERN=re.compile("^set\(GPORCA_VERSION_MAJOR+.\d+\)")
GPORCA_VERSION_MINOR_PATTERN=re.compile("^set\(GPORCA_VERSION_MINOR+.\d+\)")
GPORCA_VERSION_PATCH_PATTERN=re.compile("^set\(GPORCA_VERSION_PATCH+.\d+\)")

def exec_command(cmd):
  print "Executing command: {0}".format(' '.join(cmd))
  p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
  while True:
    nextline = p.stdout.readline()
    if nextline == '' and p.poll() is not None:
      break
    sys.stdout.write(nextline)
    sys.stdout.flush()

  output = p.communicate()[0]
  exitCode = p.returncode
  print "Exit Code: {0}\n".format(exitCode)
  return exitCode == 1

def _version_num(line):
  op = line.split(' ')[1].strip()
  return op.strip('\)')

def read_orca_version():
  # read orca version from CMakeLists.txt
  with open(os.path.join(ORCA_SRC_DIR, 'CMakeLists.txt'), 'r') as fh:
      cmakelist_content = fh.readlines();

  version = ['v'] # version starts with v
  for line in cmakelist_content:
      if GPORCA_VERSION_MAJOR_PATTERN.match(line):
          version.append(_version_num(line))
          version.append('.')
          continue
      if GPORCA_VERSION_MINOR_PATTERN.match(line):
          version.append(_version_num(line))
          version.append('.')
          continue
      if GPORCA_VERSION_PATCH_PATTERN.match(line):
          version.append(_version_num(line))
          break
  return ''.join(version)

def export_orca_src(orca_version):
  cmd = ["conan", "export", "orca/{0}@gpdb/stable".format(orca_version)]
  return exec_command(cmd)

def add_conan_remote(bintray_remote_name):
  # add conan remote
  cmd = ["conan", "remote",  "add", bintray_remote_name, "https://api.bintray.com/conan/greenplum-db/gpdb-oss"]
  return exec_command(cmd)

def set_bintray_user(bintray_user, bintray_user_key, bintray_remote):
  # set bintray account to be used to upload package
  cmd = ["conan", "user", "-p", bintray_user_key, "-r={0}".format(bintray_remote), bintray_user] 
  return exec_command(cmd)

def upload_orca_src(orca_version):
  # upload orca source to bintray
  cmd = ["conan", "upload", "orca/{0}@gpdb/stable".format(orca_version), "--all", "-r=conan-gpdb"]
  return exec_command(cmd)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Main drived to build and install ORCA using conan')
  required_arguments = parser.add_argument_group('required arguments')
  required_arguments.add_argument('--bintrayUser', help='Bintray user name to upload packages', type=str)
  required_arguments.add_argument('--bintrayUserKey', help='Bintray user key', type=str)
  required_arguments.add_argument('--bintrayRemote', help='Name of conan remote refering to bintray', type=str)

  args = parser.parse_args()
  if args.bintrayRemote is None or args.bintrayUser is None or args.bintrayUserKey is None:
    print "Values for --bintrayRemote, --bintrayUser and --bintrayUserKey argument values are required, some are missing, exiting!"
    sys.exit(1)

  os.chdir(ORCA_SRC_DIR)
  orca_version = read_orca_version()

  status = export_orca_src(orca_version)
  if status:
    sys.exit(1)

  status = add_conan_remote(args.bintrayRemote)
  if status:
    sys.exit(1)

  status = set_bintray_user(args.bintrayUser, args.bintrayUserKey, args.bintrayRemote)
  if status:
    sys.exit(1)

  status = upload_orca_src(orca_version)
  if status:
    sys.exit(1)
  os.chdir(BASE_WD)
