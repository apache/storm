#!/usr/bin/env python

"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

docker_to_squash.py is a tool to facilitate the process of converting
Docker images into squashFS layers, manifests, and configs.

Tool dependencies: skopeo, squashfs-tools, tar, setfattr
"""

import argparse
from collections import Iterable
import glob
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import time
from threading import Timer

LOG_LEVEL = None
HADOOP_BIN_DIR = None

def shell_command(command, print_stdout, print_stderr, raise_on_error,
                  timeout_sec=900):
  global LOG_LEVEL
  global ARG_MAX
  stdout_val = subprocess.PIPE
  stderr_val = subprocess.PIPE

  logging.debug("command: %s", command)

  for arg in command:
    if len(arg) > ARG_MAX:
      raise Exception("command length (" + str(len(arg)) + ")" +
                      " greater than ARG_MAX (" + str(ARG_MAX) + ")")

  if print_stdout:
    stdout_val = None

  if print_stderr or LOG_LEVEL == "DEBUG":
    stderr_val = None

  process = None
  try:
    process = subprocess.Popen(command, stdout=stdout_val,
                               stderr=stderr_val)
    timer = Timer(timeout_sec, process_timeout, [process])

    timer.start()
    out, err = process.communicate()

    if raise_on_error and process.returncode is not 0:
      exception_string = ("Commmand: " + str(command)
                          + " failed with returncode: "
                          + str(process.returncode))
      if out != None:
        exception_string = exception_string + "\nstdout: " + str(out)
      if err != None:
        exception_string = exception_string + "\nstderr: " + str(err)
      raise Exception(exception_string)

  except:
    if process and process.poll() is None:
      process.kill()
    raise Exception("Popen failure")
  finally:
    if timer:
      timer.cancel()

  return out, err, process.returncode

def process_timeout(process):
  process.kill()
  logging.error("Process killed due to timeout")

def does_hdfs_entry_exist(entry, raise_on_error=True):
  out, err, returncode = hdfs_ls(entry, raise_on_error=raise_on_error)
  if returncode is not 0:
    return False
  return True

def setup_hdfs_dirs(dirs):
  if does_hdfs_entry_exist(dirs, raise_on_error=False):
    return

  hdfs_mkdir(dirs, create_parents=True)
  chmod_dirs = []
  for dir_entry in dirs:
    directories = dir_entry.split("/")[1:]
    dir_path = ""
    for directory in directories:
      dir_path = dir_path + "/" +  directory
      logging.info("dir_path: %s", str(dir_path))
      chmod_dirs.append(dir_path)
  hdfs_chmod("755", chmod_dirs)

def append_or_extend_to_list(src, src_list):
  if isinstance(src, list):
    src_list.extend(src)
  else:
    src_list.append(src)

def hdfs_get(src, dest, print_stdout=False, print_stderr=False, raise_on_error=True):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR + "/hadoop", "fs", "-get"]
  append_or_extend_to_list(src, command)
  command.append(dest)
  out, err, returncode = shell_command(command, print_stdout, print_stderr, raise_on_error)
  return out, err, returncode

def hdfs_ls(file_path, options="", print_stdout=False, print_stderr=False,
            raise_on_error=True):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR + "/hadoop", "fs", "-ls"]
  if options:
    append_or_extend_to_list(options, command)
  append_or_extend_to_list(file_path, command)
  out, err, returncode = shell_command(command, print_stdout, print_stderr,
                                       raise_on_error)
  return out, err, returncode

def hdfs_cat(file_path, print_stdout=False, print_stderr=True, raise_on_error=True):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR + "/hadoop", "fs", "-cat"]
  append_or_extend_to_list(file_path, command)
  out, err, returncode = shell_command(command, print_stdout, print_stderr, raise_on_error)
  return out, err, returncode

def hdfs_mkdir(file_path, print_stdout=False, print_stderr=True, raise_on_error=True,
               create_parents=False):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR + "/hadoop", "fs", "-mkdir"]
  if create_parents:
    command.append("-p")
  append_or_extend_to_list(file_path, command)
  out, err, returncode = shell_command(command, print_stdout, print_stderr, raise_on_error)
  return out, err, returncode

def hdfs_rm(file_path, error_on_file_not_found=False, print_stdout=False, print_stderr=True, raise_on_error=True):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR + "/hadoop", "fs", "-rm"]
  if not error_on_file_not_found:
    command.append("-f")
  append_or_extend_to_list(file_path, command)
  out, err, returncode = shell_command(command, print_stdout, print_stderr, raise_on_error)
  return out, err, returncode

def hdfs_put(src, dest, force=False, print_stdout=False, print_stderr=True, raise_on_error=True):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR + "/hadoop", "fs", "-put"]
  if force:
    command.append("-f")
  append_or_extend_to_list(src, command)
  command.append(dest)
  out, err, returncode = shell_command(command, print_stdout, print_stderr,
                                       raise_on_error)
  return out, err, returncode

def hdfs_chmod(mode, file_path, print_stdout=False, print_stderr=True, raise_on_error=True,
               recursive=False):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR + "/hadoop", "fs", "-chmod"]
  if recursive:
    command.append("-R")
  command.append(mode)
  append_or_extend_to_list(file_path, command)
  out, err, returncode = shell_command(command, print_stdout, print_stderr, raise_on_error)
  return out, err, returncode

def hdfs_setrep(replication, file_path, print_stdout=False, print_stderr=True, raise_on_error=True):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR +  "/hadoop", "fs", "-setrep", str(replication)]
  append_or_extend_to_list(file_path, command)
  out, err, returncode = shell_command(command, print_stdout, print_stderr, raise_on_error)
  return out, err, returncode

def hdfs_cp(src, dest, force=False, print_stdout=False, print_stderr=True, raise_on_error=True):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR +  "/hadoop", "fs", "-cp"]
  if force:
    command.append("-f")
  append_or_extend_to_list(src, command)
  command.append(dest)
  out, err, returncode = shell_command(command, print_stdout, print_stderr,
                                       raise_on_error)
  return out, err, returncode

def hdfs_touchz(file_path, print_stdout=False, print_stderr=True,
                raise_on_error=True):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR + "/hadoop", "fs", "-touchz"]
  append_or_extend_to_list(file_path, command)
  out, err, returncode = shell_command(command, print_stdout, print_stderr, raise_on_error)
  return out, err, returncode

def hdfs_stat(file_path, fmt, print_stdout=False, print_stderr=True,
              raise_on_error=True):
  global HADOOP_BIN_DIR
  command = [HADOOP_BIN_DIR + "/hadoop", "fs", "-stat"]
  append_or_extend_to_list(fmt, command)
  command.append(file_path)
  out, err, returncode = shell_command(command, print_stdout, print_stderr, raise_on_error)
  return out, err, returncode

def get_working_dir(directory):
  try:
    if os.path.isdir(directory):
      working_dir = os.path.join(directory, "docker-to-squash")
    else:
      working_dir = directory
    os.makedirs(working_dir)
  except:
    raise Exception("Could not create working_dir: " + working_dir)
  return working_dir

def is_sha256_hash(string):
  if not re.findall(r"^[a-fA-F\d]{64,64}$", string):
    return False
  return True

def calculate_file_hash(filename):
  sha = hashlib.sha256()
  with open(filename, 'rb') as file_pointer:
    while True:
      data = file_pointer.read(65536)
      if not data:
        break
      sha.update(data)
  hexdigest = sha.hexdigest()
  if hexdigest == 0:
    raise Exception("Hex digest for file: " + hexdigest + "returned 0")
  return hexdigest

def calculate_string_hash(string):
  sha = hashlib.sha256()
  sha.update(string)
  return sha.hexdigest()

def get_local_manifest_from_path(manifest_path):
  with open(manifest_path, "rb") as file_pointer:
    out = file_pointer.read()
  manifest_hash = calculate_string_hash(str(out))
  manifest = json.loads(out)
  return manifest, manifest_hash

def get_hdfs_manifest_from_path(manifest_path):
  out, err, returncode = hdfs_cat(manifest_path)
  manifest_hash = calculate_string_hash(str(out))
  manifest = json.loads(out)
  return manifest, manifest_hash

def get_hdfs_manifests_from_paths(manifest_paths):
  out, err, returncode = hdfs_cat(manifest_paths)
  manifests_list = out.split("}{")
  manifests = []
  for manifest_str in manifests_list:
    if manifest_str[0] != "{":
      manifest_str = "{" + manifest_str
    if manifest_str[-1] != "}":
      manifest_str = manifest_str + "}"
    manifest_hash = calculate_string_hash(manifest_str)
    logging.debug("manifest for %s:\n%s", manifest_hash, manifest_str)
    manifest = json.loads(manifest_str)
    manifests.append((manifest, manifest_hash))
  return manifests

def get_config_hash_from_manifest(manifest):
  config_hash = manifest['config']['digest'].split(":", 1)[1]
  return config_hash

def check_total_layer_number(layers):
  global MAX_IMAGE_LAYERS
  if len(layers) > MAX_IMAGE_LAYERS:
    logging.error("layers: " + str(layers))
    raise Exception("Image has " + str(len(layers)) +
                    " layers, which is more than the maximum " + str(MAX_IMAGE_LAYERS) +
                    " layers. Failing out")

def check_total_layer_size(manifest, size):
  global MAX_IMAGE_SIZE
  if size > MAX_IMAGE_SIZE:
    for layer in manifest['layers']:
      logging.error("layer " + layer['digest'] + " has size " + str(layer['size']))
    raise Exception("Image has total size " + str(size) +
                    " B. which is more than the maximum size " + str(MAX_IMAGE_SIZE) + " B. Failing out")

def get_layer_hashes_from_manifest(manifest, error_on_size_check=True):
  layers = []
  size = 0;

  for layer in manifest['layers']:
    layers.append(layer['digest'].split(":", 1)[1])
    size += layer['size']

  if error_on_size_check:
    check_total_layer_number(layers)
    check_total_layer_size(manifest, size)

  return layers

def get_pull_fmt_string(pull_format):
  pull_fmt_string = pull_format + ":"
  if pull_format == "docker":
    pull_fmt_string = pull_fmt_string + "//"
  return pull_fmt_string

def get_manifest_from_docker_image(pull_format, image):
  pull_fmt_string = get_pull_fmt_string(pull_format)
  out, err, returncode = shell_command(["skopeo", "inspect", "--raw", pull_fmt_string + image],
                                       False, True, True)
  manifest = json.loads(out)
  if 'manifests' in manifest:
    logging.debug("skopeo inspect --raw returned a list of manifests")
    manifests_dict = manifest['manifests']
    sha = None
    for mfest in manifests_dict:
      if(mfest['platform']['architecture'] == "amd64"):
        sha = mfest['digest']
        break
    if not sha:
      raise Exception("Could not find amd64 manifest for" + image)

    image_without_tag = image.split("/", 1)[-1].split(":", 1)[0]
    image_and_sha = image_without_tag + "@" + sha

    logging.debug("amd64 manifest sha is: %s", sha)

    manifest, manifest_hash = get_manifest_from_docker_image(pull_format, image_and_sha)
  else:
    manifest_hash = calculate_string_hash(str(out))

  logging.debug("manifest: %s", str(manifest))
  return manifest, manifest_hash

def split_image_and_tag(image_and_tag):
  split = image_and_tag.split(",")
  image = split[0]
  tags = split[1:]
  return image, tags

def read_image_tag_to_hash(image_tag_to_hash):
  hash_to_tags = dict()
  tag_to_hash = dict()
  with open(image_tag_to_hash, 'rb') as file_pointer:
    while True:
      line = file_pointer.readline()
      if not line:
        break
      line = line.rstrip()

      if not line:
        continue

      comment_split_line = line.split("#", 1)
      line = comment_split_line[0]
      comment = comment_split_line[1:]

      split_line = line.rsplit(":", 1)
      manifest_hash = split_line[-1]
      tags_list = ' '.join(split_line[:-1]).split(",")

      if not is_sha256_hash(manifest_hash) or not tags_list:
        logging.warn("image-tag-to-hash file malformed. Skipping entry %s", line)
        continue

      tags_and_comments = hash_to_tags.get(manifest_hash, None)
      if tags_and_comments is None:
        known_tags = tags_list
        known_comment = comment
      else:
        known_tags = tags_and_comments[0]
        for tag in tags_list:
          if tag not in known_tags:
            known_tags.append(tag)
        known_comment = tags_and_comments[1]
        known_comment.extend(comment)

      hash_to_tags[manifest_hash] = (known_tags, known_comment)

      for tag in tags_list:
        cur_manifest = tag_to_hash.get(tag, None)
        if cur_manifest is not None:
          logging.warn("tag_to_hash already has manifest %s defined for tag %s."
                       + "This entry will be overwritten", cur_manifest, tag)
        tag_to_hash[tag] = manifest_hash
  return hash_to_tags, tag_to_hash

def remove_tag_from_dicts(hash_to_tags, tag_to_hash, tag):
  if not hash_to_tags:
    logging.debug("hash_to_tags is null. Not removing tag %s", tag)
    return

  prev_hash = tag_to_hash.get(tag, None)

  if prev_hash is not None:
    del tag_to_hash[tag]
    prev_tags, prev_comment = hash_to_tags.get(prev_hash, (None, None))
    prev_tags.remove(tag)
    if prev_tags == 0:
      del hash_to_tags[prev_hash]
    else:
      hash_to_tags[prev_hash] = (prev_tags, prev_comment)
  else:
    logging.debug("Tag not found. Not removing tag: %s", tag)

def remove_image_hash_from_dicts(hash_to_tags, tag_to_hash, image_hash):
  if not hash_to_tags:
    logging.debug("hash_to_tags is null. Not removing image_hash %s", image_hash)
    return
  logging.debug("hash_to_tags: %s", str(hash_to_tags))
  logging.debug("Removing image_hash from dicts: %s", image_hash)
  prev_tags, prev_comments = hash_to_tags.get(image_hash, None)

  if prev_tags is not None:
    hash_to_tags.pop(image_hash)
    for tag in prev_tags:
      del tag_to_hash[tag]

def add_tag_to_dicts(hash_to_tags, tag_to_hash, tag, manifest_hash, comment):
  tag_to_hash[tag] = manifest_hash
  new_tags_and_comments = hash_to_tags.get(manifest_hash, None)
  if new_tags_and_comments is None:
    new_tags = [tag]
    new_comment = [comment]
  else:
    new_tags = new_tags_and_comments[0]
    new_comment = new_tags_and_comments[1]
    if tag not in new_tags:
      new_tags.append(tag)
    if comment and comment not in new_comment:
      new_comment.append(comment)
  hash_to_tags[manifest_hash] = (new_tags, new_comment)

def write_local_image_tag_to_hash(image_tag_to_hash, hash_to_tags):
  file_contents = []
  for key, value in hash_to_tags.iteritems():
    manifest_hash = key
    # Sort tags list to preserve consistent order
    value[0].sort()
    tags = ','.join(map(str, value[0]))
    if tags:
      # Sort comments list to preserve consistent order
      value[1].sort()
      comment = ', '.join(map(str, value[1]))
      if comment > 0:
        comment = "#" + comment
      file_contents.append(tags + ":" + manifest_hash + comment + "\n")

  file_contents.sort()
  with open(image_tag_to_hash, 'w') as file_pointer:
    for val in file_contents:
      file_pointer.write(val)

def update_dicts_for_multiple_tags(hash_to_tags, tag_to_hash, tags,
                                   manifest_hash, comment):
  for tag in tags:
    update_dicts(hash_to_tags, tag_to_hash, tag, manifest_hash, comment)

def update_dicts(hash_to_tags, tag_to_hash, tag, manifest_hash, comment):
  remove_tag_from_dicts(hash_to_tags, tag_to_hash, tag)
  add_tag_to_dicts(hash_to_tags, tag_to_hash, tag, manifest_hash, comment)

def remove_from_dicts(hash_to_tags, tag_to_hash, tags):
  for tag in tags:
    logging.debug("removing tag: %s", tag)
    remove_tag_from_dicts(hash_to_tags, tag_to_hash, tag)

def populate_tag_dicts(image_tag_to_hash, local_image_tag_to_hash):
  #Setting hdfs_root to None will default it to using the global
  return populate_tag_dicts_set_root(image_tag_to_hash, local_image_tag_to_hash, None)

def populate_tag_dicts_set_root(image_tag_to_hash, local_image_tag_to_hash, hdfs_root):
  global HDFS_ROOT
  if hdfs_root is None:
    hdfs_root = HDFS_ROOT

  hdfs_get(hdfs_root + "/" + image_tag_to_hash, local_image_tag_to_hash,
           raise_on_error=True)
  image_tag_to_hash_hash = calculate_file_hash(local_image_tag_to_hash)

  if image_tag_to_hash_hash != 0:
    hash_to_tags, tag_to_hash = read_image_tag_to_hash(local_image_tag_to_hash)
  else:
    hash_to_tags = {}
    tag_to_hash = {}
  return hash_to_tags, tag_to_hash, image_tag_to_hash_hash


def setup_squashfs_hdfs_dirs(hdfs_dirs, image_tag_to_hash_path):
  logging.debug("Setting up squashfs hdfs_dirs: %s", str(hdfs_dirs))
  setup_hdfs_dirs(hdfs_dirs)
  if not does_hdfs_entry_exist(image_tag_to_hash_path, raise_on_error=False):
    hdfs_touchz(image_tag_to_hash_path)
    hdfs_chmod("755", image_tag_to_hash_path)

def skopeo_copy_image(pull_format, image, skopeo_format, skopeo_dir):
  logging.info("Pulling image: %s", image)
  if os.path.isdir(skopeo_dir):
    raise Exception("Skopeo output directory already exists. "
                    + "Please delete and try again "
                    + "Directory: " + skopeo_dir)
  pull_fmt_string = get_pull_fmt_string(pull_format)
  shell_command(["skopeo", "copy", pull_fmt_string + image,
                 skopeo_format + ":" + skopeo_dir], False, True, True)

def untar_layer(tmp_dir, layer_path):
  shell_command(["tar", "-C", tmp_dir, "--xattrs",
                 "--xattrs-include='*'", "-xf", layer_path],
                False, True, True)

def tar_file_search(archive, target):
  out, err, returncode = shell_command(["tar", "-xf", archive, target, "-O"],
                                       False, False, False)
  return out

def set_fattr(directory):
  shell_command(["setfattr", "-n", "trusted.overlay.opaque",
                 "-v", "y", directory], False, True, True)

def make_whiteout_block_device(file_path, whiteout):
  shell_command(["mknod", "-m", "000", file_path,
                 "c", "0", "0"], False, True, True)

  out, err, returncode = shell_command(["stat", "-c", "%U:%G", whiteout], False, True, True)
  perms = str(out).strip()

  shell_command(["chown", perms, file_path], False, True, True)

def convert_oci_whiteouts(tmp_dir):
  out, err, returncode = shell_command(["find", tmp_dir, "-name", ".wh.*"],
                                       False, False, True)
  whiteouts = str(out).splitlines()
  for whiteout in whiteouts:
    if whiteout == 0:
      continue
    basename = os.path.basename(whiteout)
    directory = os.path.dirname(whiteout)
    if basename == ".wh..wh..opq":
      set_fattr(directory)
    else:
      whiteout_string = ".wh."
      idx = basename.rfind(whiteout_string)
      bname = basename[idx+len(whiteout_string):]
      file_path = os.path.join(directory, bname)
      make_whiteout_block_device(file_path, whiteout)
    shell_command(["rm", whiteout], False, True, True)

def dir_to_squashfs(tmp_dir, squash_path):
  shell_command(["mksquashfs", tmp_dir, squash_path, "-write-queue", "4096",
                 "-read-queue", "4096", "-fragment-queue", "4096"],
                False, True, True)

def upload_to_hdfs(src, dest, replication, mode, force=False):
  if does_hdfs_entry_exist(dest, raise_on_error=False):
    if not force:
      logging.warn("Not uploading to HDFS. File already exists: %s", dest)
      return
    logging.info("File already exists, but overwriting due to force option: %s", dest)

  hdfs_put(src, dest, force)
  hdfs_setrep(replication, dest)
  hdfs_chmod(mode, dest)
  logging.info("Uploaded file %s with replication %d and permissions %s",
               dest, replication, mode)

def atomic_upload_mv_to_hdfs(src, dest, replication, image_tag_to_hash_file_hash):
  global HADOOP_PREFIX
  global HADOOP_BIN_DIR

  local_hash = calculate_file_hash(src)
  if local_hash == image_tag_to_hash_file_hash:
    logging.info("image_tag_to_hash file unchanged. Not uploading")
    return

  tmp_dest = dest + ".tmp"
  try:
    if does_hdfs_entry_exist(tmp_dest, raise_on_error=False):
      hdfs_rm(tmp_dest)
    hdfs_put(src, tmp_dest)
    hdfs_setrep(replication, tmp_dest)
    hdfs_chmod("444", tmp_dest)

    jar_path = HADOOP_PREFIX + "/share/hadoop/tools/lib/hadoop-extras-*.jar"
    for file in glob.glob(jar_path):
      jar_file = file

    if not jar_file:
      raise Exception("SymlinkTool Jar doesn't exist: %s" % (jar_path))

    logging.debug("jar_file: " + jar_file)

    shell_command([HADOOP_BIN_DIR + "/hadoop", "jar", jar_file, "org.apache.hadoop.tools.SymlinkTool",
                   "mvlink", "-f", tmp_dest, dest], False, False, True)

  except:
    if does_hdfs_entry_exist(tmp_dest, raise_on_error=False):
      hdfs_rm(tmp_dest)
    raise Exception("image tag to hash file upload failed")

def docker_to_squash(layer_dir, layer, working_dir):
  tmp_dir = os.path.join(working_dir, "expand_archive_" + layer)
  layer_path = os.path.join(layer_dir, layer)
  squash_path = layer_path + ".sqsh"

  if os.path.isdir(tmp_dir):
    raise Exception("tmp_dir already exists. Please delete and try again " +
                    "Directory: " + tmp_dir)
  os.makedirs(tmp_dir)

  try:
    untar_layer(tmp_dir, layer_path)
    convert_oci_whiteouts(tmp_dir)
    dir_to_squashfs(tmp_dir, squash_path)
  finally:
    os.remove(layer_path)
    shell_command(["rm", "-rf", tmp_dir],
                  False, True, True)


def check_image_for_magic_file(magic_file, skopeo_dir, layers):
  magic_file_absolute = magic_file.strip("/")
  logging.debug("Searching for magic file %s", magic_file_absolute)
  for layer in layers:
    ret = tar_file_search(os.path.join(skopeo_dir, layer), magic_file_absolute)
    if ret:
      logging.debug("Found magic file %s in layer %s", magic_file_absolute, layer)
      logging.debug("Magic file %s has contents:\n%s", magic_file_absolute, ret)
      return ret
  raise Exception("Magic file %s doesn't exist in any layer" %
                  (magic_file_absolute))

def pull_build_push_update(args):
  global HDFS_ROOT
  global HDFS_MANIFEST_DIR
  global HDFS_CONFIG_DIR
  global HDFS_LAYERS_DIR
  global HDFS_UNREF_DIR

  skopeo_format = args.skopeo_format
  pull_format = args.pull_format
  image_tag_to_hash = args.image_tag_to_hash
  replication = args.replication
  force = args.force
  images_and_tags = args.images_and_tags
  check_magic_file = args.check_magic_file
  magic_file = args.magic_file
  bootstrap = args.bootstrap

  working_dir = None

  try:
    working_dir = get_working_dir(args.working_dir)
    local_image_tag_to_hash = os.path.join(working_dir, os.path.basename(image_tag_to_hash))
    if bootstrap:
      hdfs_dirs = [HDFS_ROOT, HDFS_LAYERS_DIR, HDFS_CONFIG_DIR, HDFS_MANIFEST_DIR, HDFS_UNREF_DIR]
      image_tag_to_hash_path = HDFS_ROOT + "/" + image_tag_to_hash
      setup_squashfs_hdfs_dirs(hdfs_dirs, image_tag_to_hash_path)
    hash_to_tags, tag_to_hash, image_tag_to_hash_hash = populate_tag_dicts(image_tag_to_hash,
                                                                           local_image_tag_to_hash)

    for image_and_tag_arg in images_and_tags:
      image, tags = split_image_and_tag(image_and_tag_arg)
      if not image or not tags:
        raise Exception("Positional parameter requires an image and at least 1 tag: "
                        + image_and_tag_arg)

      logging.info("Working on image %s with tags %s", image, str(tags))
      manifest, manifest_hash = get_manifest_from_docker_image(pull_format, image)

      layers = get_layer_hashes_from_manifest(manifest)
      config_hash = get_config_hash_from_manifest(manifest)

      logging.debug("Layers: %s", str(layers))
      logging.debug("Config: %s", str(config_hash))

      update_dicts_for_multiple_tags(hash_to_tags, tag_to_hash, tags,
                                     manifest_hash, image)

      hdfs_files_to_check = []
      hdfs_files_to_check.append(HDFS_MANIFEST_DIR + "/" + manifest_hash)
      hdfs_files_to_check.append(HDFS_CONFIG_DIR + "/" + config_hash)

      for layer in layers:
        hdfs_files_to_check.append(HDFS_LAYERS_DIR + "/" + layer + ".sqsh")

      if does_hdfs_entry_exist(hdfs_files_to_check, raise_on_error=False):
        if not force:
          logging.info("All image files exist in HDFS, skipping this image")
          continue
        logging.info("All image files exist in HDFS, but force option set, so overwriting image")

      skopeo_dir = os.path.join(working_dir, image.split("/")[-1])
      logging.debug("skopeo_dir: %s", skopeo_dir)

      skopeo_copy_image(pull_format, image, skopeo_format, skopeo_dir)

      if check_magic_file:
        check_image_for_magic_file(magic_file, skopeo_dir, layers)

      for layer in layers:
        logging.info("Squashifying and uploading layer: %s", layer)
        hdfs_squash_path = HDFS_LAYERS_DIR + "/" + layer + ".sqsh"
        if does_hdfs_entry_exist(hdfs_squash_path, raise_on_error=False):
          if force:
            logging.info("Layer already exists, but overwriting due to force"
                         + "option: %s", layer)
          else:
            logging.info("Layer exists. Skipping and not squashifying or"
                         + "uploading: %s", layer)
            continue

        docker_to_squash(skopeo_dir, layer, working_dir)
        squash_path = os.path.join(skopeo_dir, layer + ".sqsh")
        squash_name = os.path.basename(squash_path)
        upload_to_hdfs(squash_path, HDFS_LAYERS_DIR + "/" + squash_name, replication, "444", force)


      config_local_path = os.path.join(skopeo_dir, config_hash)
      upload_to_hdfs(config_local_path,
                     HDFS_CONFIG_DIR + "/" + os.path.basename(config_local_path),
                     replication, "444", force)

      manifest_local_path = os.path.join(skopeo_dir, "manifest.json")
      upload_to_hdfs(manifest_local_path, HDFS_MANIFEST_DIR + "/" + manifest_hash,
                     replication, "444", force)

    write_local_image_tag_to_hash(local_image_tag_to_hash, hash_to_tags)
    atomic_upload_mv_to_hdfs(local_image_tag_to_hash, HDFS_ROOT + "/" + image_tag_to_hash,
                             replication, image_tag_to_hash_hash)
  finally:
    if working_dir:
      if os.path.isdir(working_dir):
        shell_command(["rm", "-rf", working_dir],
                      False, True, True)

def pull_build(args):
  skopeo_format = args.skopeo_format
  pull_format = args.pull_format
  images_and_tags = args.images_and_tags
  check_magic_file = args.check_magic_file
  magic_file = args.magic_file

  for image_and_tag_arg in images_and_tags:
    image, tags = split_image_and_tag(image_and_tag_arg)
    if not image or not tags:
      raise Exception("Positional parameter requires an image and at least 1 tag: "
                      + image_and_tag_arg)

    logging.info("Working on image %s with tags %s", image, str(tags))
    manifest, manifest_hash = get_manifest_from_docker_image(pull_format, image)

    layers = get_layer_hashes_from_manifest(manifest)
    config_hash = get_config_hash_from_manifest(manifest)

    logging.debug("Layers: %s", str(layers))
    logging.debug("Config: %s", str(config_hash))


    try:
      working_dir = get_working_dir(args.working_dir)
      skopeo_dir = os.path.join(working_dir, image.split("/")[-1])
      logging.debug("skopeo_dir: %s", skopeo_dir)
      skopeo_copy_image(pull_format, image, skopeo_format, skopeo_dir)

      if check_magic_file:
        check_image_for_magic_file(magic_file, skopeo_dir, layers)

      for layer in layers:
        logging.info("Squashifying layer: %s", layer)
        docker_to_squash(skopeo_dir, layer, working_dir)

    except:
      if os.path.isdir(skopeo_dir):
        shutil.rmtree(skopeo_dir)
      raise

def push_update(args):
  global HDFS_ROOT
  global HDFS_MANIFEST_DIR
  global HDFS_CONFIG_DIR
  global HDFS_LAYERS_DIR
  global HDFS_UNREF_DIR

  image_tag_to_hash = args.image_tag_to_hash
  replication = args.replication
  force = args.force
  images_and_tags = args.images_and_tags
  bootstrap = args.bootstrap

  local_image_tag_to_hash = None

  try:
    working_dir = args.working_dir
    local_image_tag_to_hash = os.path.join(working_dir, os.path.basename(image_tag_to_hash))
    if bootstrap:
      hdfs_dirs = [HDFS_ROOT, HDFS_LAYERS_DIR, HDFS_CONFIG_DIR, HDFS_MANIFEST_DIR, HDFS_UNREF_DIR]
      image_tag_to_hash_path = HDFS_ROOT + "/" + image_tag_to_hash
      setup_squashfs_hdfs_dirs(hdfs_dirs, image_tag_to_hash_path)
    hash_to_tags, tag_to_hash, image_tag_to_hash_hash = populate_tag_dicts(image_tag_to_hash,
                                                                           local_image_tag_to_hash)

    for image_and_tag_arg in images_and_tags:
      image, tags = split_image_and_tag(image_and_tag_arg)
      if not image or not tags:
        raise Exception("Positional parameter requires an image and at least 1 tag: "
                        + image_and_tag_arg)

      logging.info("Working on image %s with tags %s", image, str(tags))
      skopeo_dir = os.path.join(working_dir, image.split("/")[-1])
      if not os.path.exists(skopeo_dir):
        raise Exception("skopeo_dir doesn't exists: %s" % (skopeo_dir))
      manifest, manifest_hash = get_local_manifest_from_path(skopeo_dir + "/manifest.json")

      layers = get_layer_hashes_from_manifest(manifest)
      config_hash = get_config_hash_from_manifest(manifest)

      logging.debug("Layers: %s", str(layers))
      logging.debug("Config: %s", str(config_hash))

      update_dicts_for_multiple_tags(hash_to_tags, tag_to_hash, tags,
                                     manifest_hash, image)

      hdfs_files_to_check = []
      hdfs_files_to_check.append(HDFS_MANIFEST_DIR + "/" + manifest_hash)
      hdfs_files_to_check.append(HDFS_CONFIG_DIR + "/" + config_hash)

      for layer in layers:
        hdfs_files_to_check.append(HDFS_LAYERS_DIR + "/" + layer + ".sqsh")

      if does_hdfs_entry_exist(hdfs_files_to_check, raise_on_error=False):
        if not force:
          logging.info("All image files exist in HDFS, skipping this image")
          continue
        logging.info("All image files exist in HDFS, but force option set, so overwriting image")

      for layer in layers:
        hdfs_squash_path = HDFS_LAYERS_DIR + "/" + layer + ".sqsh"
        if does_hdfs_entry_exist(hdfs_squash_path, raise_on_error=False):
          if force:
            logging.info("Layer already exists, but overwriting due to force"
                         + "option: %s", layer)
          else:
            logging.info("Layer exists. Skipping and not squashifying or"
                         + "uploading: %s", layer)
            continue

        squash_path = os.path.join(skopeo_dir, layer + ".sqsh")
        squash_name = os.path.basename(squash_path)
        upload_to_hdfs(squash_path, HDFS_LAYERS_DIR + "/" + squash_name, replication, "444", force)


      config_local_path = os.path.join(skopeo_dir, config_hash)
      upload_to_hdfs(config_local_path,
                     HDFS_CONFIG_DIR + "/" + os.path.basename(config_local_path),
                     replication, "444", force)

      manifest_local_path = os.path.join(skopeo_dir, "manifest.json")
      upload_to_hdfs(manifest_local_path, HDFS_MANIFEST_DIR + "/" + manifest_hash,
                     replication, "444", force)

    write_local_image_tag_to_hash(local_image_tag_to_hash, hash_to_tags)
    atomic_upload_mv_to_hdfs(local_image_tag_to_hash, HDFS_ROOT + "/" + image_tag_to_hash,
                             replication, image_tag_to_hash_hash)
  finally:
    if local_image_tag_to_hash:
      if os.path.isfile(local_image_tag_to_hash):
        os.remove(local_image_tag_to_hash)


def remove_image(args):
  global HDFS_ROOT
  global HDFS_MANIFEST_DIR
  global HDFS_CONFIG_DIR
  global HDFS_LAYERS_DIR
  global HDFS_UNREF_DIR

  image_tag_to_hash = args.image_tag_to_hash
  replication = args.replication
  images_or_tags = args.images_or_tags
  working_dir = None

  try:
    working_dir = get_working_dir(args.working_dir)
    local_image_tag_to_hash = os.path.join(working_dir, os.path.basename(image_tag_to_hash))

    images_and_tags_to_remove = []
    for image_or_tag_arg in images_or_tags:
      images_and_tags_to_remove.extend(image_or_tag_arg.split(","))

    logging.debug("images_and_tags_to_remove:\n%s", images_and_tags_to_remove)

    hash_to_tags, tag_to_hash, image_tag_to_hash_hash = populate_tag_dicts(image_tag_to_hash,
                                                                           local_image_tag_to_hash)

    logging.debug("hash_to_tags: %s", str(hash_to_tags))
    logging.debug("tag_to_hash: %s", str(tag_to_hash))

    known_images = get_all_known_images()
    if not known_images:
      logging.warn("No known images\n")
      return

    images_to_remove = get_images_from_args(images_and_tags_to_remove, tag_to_hash, known_images)

    logging.debug("images_to_remove:\n%s", images_to_remove)
    if not images_to_remove:
      logging.warn("No images to remove")
      return

    delete_list = get_delete_list_from_images_to_remove(images_to_remove, known_images)

    for image_to_remove in images_to_remove:
      remove_image_hash_from_dicts(hash_to_tags, tag_to_hash, image_to_remove.manifest_stat.name)

    write_local_image_tag_to_hash(local_image_tag_to_hash, hash_to_tags)
    atomic_upload_mv_to_hdfs(local_image_tag_to_hash, HDFS_ROOT + "/" + image_tag_to_hash, replication,
                             image_tag_to_hash_hash)

    hdfs_rm(delete_list)

  finally:
    if working_dir:
      if os.path.isdir(working_dir):
        shutil.rmtree(working_dir)

def get_images_from_args(images_and_tags, tag_to_hash, known_images):
  images = []

  if isinstance(images_and_tags, Iterable):
    for image_arg in images_and_tags:
      image = get_image_hash_from_arg(image_arg, tag_to_hash, known_images)
      if image:
        images.append(image)
  else:
    image_arg = images_and_tags[0]
    image = get_image_hash_from_arg(image_arg, tag_to_hash, known_images)
    if image:
      images.append(image)

  return images

def get_image_hash_from_arg(image_str, tag_to_hash, known_images):
  if is_sha256_hash(image_str):
    image_hash = image_str
  else:
    image_hash = tag_to_hash.get(image_str, None)

  if image_hash:
    image = get_known_image_by_hash(image_hash, known_images)
  else:
    logging.warn("image tag unknown: %s", image_str)
    return None

  return image

def get_delete_list_from_images_to_remove(images_to_remove, known_images):
  global HDFS_ROOT
  global HDFS_MANIFEST_DIR
  global HDFS_CONFIG_DIR
  global HDFS_LAYERS_DIR
  global HDFS_UNREF_DIR

  layers_to_keep = []
  delete_list = []

  for image in known_images:
    if image not in images_to_remove:
      layers_to_keep.extend(image.layers)

  for image_to_remove in images_to_remove:
    delete_list.append(HDFS_MANIFEST_DIR + "/" + image_to_remove.manifest_stat.name)
    delete_list.append(HDFS_CONFIG_DIR + "/" + image_to_remove.config)
    if image_to_remove.unref_file_stat:
      delete_list.append(HDFS_UNREF_DIR + "/" + image_to_remove.unref_file_stat.name)

    layers = image_to_remove.layers
    for layer in layers:
      if layer not in layers_to_keep:
        layer_path = HDFS_LAYERS_DIR + "/" + layer + ".sqsh"
        if layer_path not in delete_list:
          delete_list.append(layer_path)

  logging.debug("delete_list:\n%s", delete_list)

  return delete_list

def add_remove_tag(args):
  global HDFS_ROOT
  global HDFS_MANIFEST_DIR

  pull_format = args.pull_format
  image_tag_to_hash = args.image_tag_to_hash
  replication = args.replication
  sub_command = args.sub_command
  images_and_tags = args.images_and_tags

  working_dir = None

  try:
    working_dir = get_working_dir(args.working_dir)
    local_image_tag_to_hash = os.path.join(working_dir, os.path.basename(image_tag_to_hash))
    hash_to_tags, tag_to_hash, image_tag_to_hash_hash = populate_tag_dicts(image_tag_to_hash,
                                                                           local_image_tag_to_hash)

    for image_and_tag_arg in images_and_tags:
      if sub_command == "add-tag":
        image, tags = split_image_and_tag(image_and_tag_arg)
        if is_sha256_hash(image):
          manifest_hash = image
        else:
          manifest_hash = tag_to_hash.get(image, None)

        if manifest_hash:
          manifest_path = HDFS_MANIFEST_DIR + "/" + manifest_hash
          out, err, returncode = hdfs_cat(manifest_path)
          manifest = json.loads(out)
          logging.debug("image tag exists for %s", image)
        else:
          manifest, manifest_hash = get_manifest_from_docker_image(pull_format, image)

        update_dicts_for_multiple_tags(hash_to_tags, tag_to_hash, tags,
                                       manifest_hash, image)

      elif sub_command == "remove-tag":
        tags = image_and_tag_arg.split(",")
        image = None
        manifest = None
        manifest_hash = 0
        remove_from_dicts(hash_to_tags, tag_to_hash, tags)
      else:
        raise Exception("Invalid sub_command: %s" % (sub_command))

    write_local_image_tag_to_hash(local_image_tag_to_hash, hash_to_tags)
    atomic_upload_mv_to_hdfs(local_image_tag_to_hash, HDFS_ROOT + "/" + image_tag_to_hash, replication,
                             image_tag_to_hash_hash)
  finally:
    if working_dir:
      if os.path.isdir(working_dir):
        shutil.rmtree(working_dir)

def copy_update(args):
  image_tag_to_hash = args.image_tag_to_hash
  replication = args.replication
  force = args.force
  src_root = args.src_root
  dest_root = args.dest_root
  images_and_tags = args.images_and_tags
  bootstrap = args.bootstrap

  src_layers_dir = src_root + "/layers"
  src_config_dir = src_root + "/config"
  src_manifest_dir = src_root  + "/manifests"
  dest_layers_dir = dest_root + "/layers"
  dest_config_dir = dest_root + "/config"
  dest_manifest_dir = dest_root  + "/manifests"
  dest_unref_dir = dest_root + "/unreferenced"

  if bootstrap:
    hdfs_dirs = [dest_root, dest_layers_dir, dest_config_dir, dest_manifest_dir, dest_unref_dir]
    image_tag_to_hash_path = dest_root + "/" + image_tag_to_hash
    setup_squashfs_hdfs_dirs(hdfs_dirs, image_tag_to_hash_path)

  working_dir = None

  try:
    working_dir = get_working_dir(args.working_dir)
    local_src_image_tag_to_hash = os.path.join(working_dir, "src-"
                                               + os.path.basename(image_tag_to_hash))
    local_dest_image_tag_to_hash = os.path.join(working_dir, "dest-"
                                                + os.path.basename(image_tag_to_hash))

    src_hash_to_tags, src_tag_to_hash, src_image_tag_to_hash_hash = populate_tag_dicts_set_root(image_tag_to_hash, local_src_image_tag_to_hash, src_root)
    dest_hash_to_tags, dest_tag_to_hash, dest_image_tag_to_hash_hash = populate_tag_dicts_set_root(image_tag_to_hash, local_dest_image_tag_to_hash, dest_root)

    for image_and_tag_arg in images_and_tags:
      image, tags = split_image_and_tag(image_and_tag_arg)
      if not image:
        raise Exception("Positional parameter requires an image: " + image_and_tag_arg)
      if not tags:
        logging.debug("Tag not given. Using image tag instead: %s", image)
        tags = [image]

      src_manifest_hash = src_tag_to_hash.get(image, None)
      if not src_manifest_hash:
        logging.info("Manifest not found for image %s. Skipping", image)
        continue

      src_manifest_path = src_manifest_dir + "/" + src_manifest_hash
      dest_manifest_path = dest_manifest_dir + "/" + src_manifest_hash
      src_manifest, src_manifest_hash = get_hdfs_manifest_from_path(src_manifest_path)

      src_config_hash = get_config_hash_from_manifest(src_manifest)
      src_config_path = src_config_dir + "/" + src_config_hash
      dest_config_path = dest_config_dir + "/" + src_config_hash

      src_layers = get_layer_hashes_from_manifest(src_manifest)
      src_layers_paths = [src_layers_dir + "/" + layer + ".sqsh" for layer in src_layers]
      dest_layers_paths = [dest_layers_dir + "/" + layer + ".sqsh" for layer in src_layers]

      logging.debug("Copying Manifest: %s", str(src_manifest_path))
      logging.debug("Copying Layers: %s", str(src_layers_paths))
      logging.debug("Copying Config: %s", str(src_config_hash))

      if not does_hdfs_entry_exist(dest_layers_paths, raise_on_error=False):
        dest_layers_paths = []
        for layer in src_layers:
          dest_layer_path = dest_layers_dir + "/" + layer + ".sqsh"
          src_layer_path = src_layers_dir + "/" + layer + ".sqsh"
          if not does_hdfs_entry_exist(dest_layer_path, raise_on_error=False):
            hdfs_cp(src_layer_path, dest_layer_path, force)
            dest_layers_paths.append(dest_layer_path)
        hdfs_setrep(replication, dest_layers_paths)
        hdfs_chmod("444", dest_layers_paths)

      if not does_hdfs_entry_exist(dest_config_path, raise_on_error=False):
        hdfs_cp(src_config_path, dest_config_dir, force)
        hdfs_setrep(replication, dest_config_path)
        hdfs_chmod("444", dest_config_path)

      if not does_hdfs_entry_exist(dest_manifest_path, raise_on_error=False):
        hdfs_cp(src_manifest_path, dest_manifest_dir, force)
        hdfs_setrep(replication, dest_manifest_path)
        hdfs_chmod("444", dest_manifest_path)

      for tag in tags:
        new_tags_and_comments = src_hash_to_tags.get(src_manifest_hash, None)
        if new_tags_and_comments:
          comment = ', '.join(map(str, new_tags_and_comments[1]))
        if comment is None:
          comment = image

        update_dicts(dest_hash_to_tags, dest_tag_to_hash, tag, src_manifest_hash, comment)

      write_local_image_tag_to_hash(local_dest_image_tag_to_hash, dest_hash_to_tags)
      atomic_upload_mv_to_hdfs(local_dest_image_tag_to_hash, dest_root + "/" + image_tag_to_hash,
                               replication,
                               dest_image_tag_to_hash_hash)

  finally:
    if working_dir:
      if os.path.isdir(working_dir):
        shutil.rmtree(working_dir)

def query_tag(args):
  global HDFS_ROOT
  global HDFS_MANIFEST_DIR
  global HDFS_CONFIG_DIR
  global HDFS_LAYERS_DIR

  image_tag_to_hash = args.image_tag_to_hash
  tags = args.tags
  working_dir = None

  try:
    working_dir = get_working_dir(args.working_dir)
    local_image_tag_to_hash = os.path.join(working_dir, os.path.basename(image_tag_to_hash))
    hash_to_tags, tag_to_hash, image_tag_to_hash_hash = populate_tag_dicts(image_tag_to_hash,
                                                                           local_image_tag_to_hash)

    logging.debug("hash_to_tags: %s", str(hash_to_tags))
    logging.debug("tag_to_hash: %s", str(tag_to_hash))


    for tag in tags:
      image_hash = tag_to_hash.get(tag, None)
      if not image_hash:
        logging.info("image hash mapping doesn't exist for tag %s", tag)
        continue

      manifest_path = HDFS_MANIFEST_DIR + "/" + image_hash
      if does_hdfs_entry_exist(manifest_path, raise_on_error=False):
        logging.debug("image manifest for %s exists: %s", tag, manifest_path)
      else:
        logging.info("Image manifest for %s doesn't exist: %s", tag, manifest_path)
        continue

      manifest, manifest_hash = get_hdfs_manifest_from_path(manifest_path)
      layers = get_layer_hashes_from_manifest(manifest, False)
      config_hash = get_config_hash_from_manifest(manifest)
      config_path = HDFS_CONFIG_DIR + "/" + config_hash

      layers_paths = [HDFS_LAYERS_DIR + "/" + layer + ".sqsh" for layer in layers]

      logging.info("Image info for '%s'", tag)
      logging.info(manifest_path)
      logging.info(config_path)
      for layer in layers_paths:
        logging.info(layer)

  finally:
    if working_dir:
      if os.path.isdir(working_dir):
        shutil.rmtree(working_dir)

def list_tags(args):
  global HDFS_ROOT

  image_tag_to_hash = args.image_tag_to_hash

  hdfs_image_tag_to_hash = HDFS_ROOT + "/" + image_tag_to_hash
  hdfs_cat(hdfs_image_tag_to_hash, True, True, True)

def bootstrap_setup(args):
  global HDFS_ROOT
  global HDFS_MANIFEST_DIR
  global HDFS_CONFIG_DIR
  global HDFS_LAYERS_DIR
  global HDFS_UNREF_DIR

  image_tag_to_hash = args.image_tag_to_hash

  hdfs_dirs = [HDFS_ROOT, HDFS_LAYERS_DIR, HDFS_CONFIG_DIR, HDFS_MANIFEST_DIR, HDFS_UNREF_DIR]
  image_tag_to_hash_path = HDFS_ROOT + "/" + image_tag_to_hash
  setup_squashfs_hdfs_dirs(hdfs_dirs, image_tag_to_hash_path)

def cleanup_untagged_images(args):
  global HDFS_ROOT
  global HDFS_MANIFEST_DIR
  global HDFS_CONFIG_DIR
  global HDFS_LAYERS_DIR
  global HDFS_UNREF_DIR
  global DEAD_PERMS

  image_tag_to_hash = args.image_tag_to_hash
  working_dir = get_working_dir(args.working_dir)

  local_image_tag_to_hash = os.path.join(working_dir, os.path.basename(image_tag_to_hash))

  try:
    hash_to_tags, tag_to_hash, image_tag_to_hash_hash = populate_tag_dicts(image_tag_to_hash,
                                                                           local_image_tag_to_hash)
    logging.debug("hash_to_tags: %s\n", hash_to_tags)
    logging.debug("tag_to_hash: %s\n", tag_to_hash)

    known_images = get_all_known_images()
    tagged_images = [image for image in known_images if image.manifest_stat.name in hash_to_tags.keys()]
    untagged_images = get_untagged_images(known_images, tagged_images)
    stale_images = get_stale_images(untagged_images)
    dead_images = get_dead_images(untagged_images)

    cleanup_handle_tagged_images(tagged_images)
    cleanup_handle_untagged_images(untagged_images)
    cleanup_handle_stale_images(stale_images)
    cleanup_handle_dead_images(dead_images, known_images)

  finally:
    if working_dir:
      if os.path.isdir(working_dir):
        shutil.rmtree(working_dir)

class Filestat:
  def __init__(self, perms, mod_time, name):
    self.perms = perms
    self.mod_time = mod_time
    self.name = name

  def __repr__(self):
    return "Perms: {}, Mod time: {}, Name: {}\n".format(self.perms, self.mod_time, self.name)

  def __str__(self):
    return "Perms: {}, Mod time: {}, Name: {}\n".format(self.perms, self.mod_time, self.name)

class Image:
  def __init__(self, manifest_stat, layers, config, unref_file_stat):
    self.manifest_stat = manifest_stat
    self.layers = layers
    self.config = config
    self.unref_file_stat = unref_file_stat

  def __repr__(self):
    return "Manifest: {}, Layers: {}, Config: {}, Unreferenced File: {}\n".format(self.manifest_stat, self.layers, self.config, self.unref_file_stat)

  def __str__(self):
    return "Manifest: {}, Layers: {}, Config: {}, Unreferenced File: {}\n".format(self.manifest_stat, self.layers, self.config, self.unref_file_stat)

def get_all_known_images():
  global HDFS_MANIFEST_DIR
  global HDFS_UNREF_DIR

  known_manifest_paths, err, returncode = hdfs_stat(HDFS_MANIFEST_DIR + "/*", "%a %Y %n", False, False, False)
  known_manifests  = [image for image in known_manifest_paths.split("\n") if image is not "" or None]

  unref_manifest_paths, err, returncode = hdfs_stat(HDFS_UNREF_DIR + "/*", "%a %Y %n", False, False, False)
  logging.debug("unref_manifest_paths:\n%s", unref_manifest_paths)
  unref_manifests = []
  if unref_manifest_paths:
    unref_manifests = [image for image in unref_manifest_paths.split("\n") if image is not "" or None]
  logging.debug("unref_manifests:\n%s", unref_manifests)

  unref_manifests_stats_dict = {}
  if unref_manifests:
    for unref_manifest in unref_manifests:
      unref_manifest_split = unref_manifest.split()
      unref_manifest_perms = unref_manifest_split[0]
      unref_manifest_mod_time = long(unref_manifest_split[1])
      unref_manifest_name = unref_manifest_split[2]
      unref_manifest_stat = Filestat(unref_manifest_perms, unref_manifest_mod_time, unref_manifest_name)
      unref_manifests_stats_dict[unref_manifest_name] = unref_manifest_stat

  logging.debug("unref_manifests_stats_dict:\n%s", unref_manifests_stats_dict)

  known_manifests_names = [known_manifest.split()[2] for known_manifest in known_manifests]
  layers_and_configs = get_all_layers_and_configs(known_manifests_names)

  known_images = []
  for manifest in known_manifests:
    manifest_split = manifest.split()
    manifest_perms = manifest_split[0]
    manifest_mod_time = long(manifest_split[1])
    manifest_name = manifest_split[2]
    manifest_stat = Filestat(manifest_perms, manifest_mod_time, manifest_name)

    unref_image_stat = unref_manifests_stats_dict.get(manifest_name, None)

    layers = layers_and_configs[manifest_name][0]
    config = layers_and_configs[manifest_name][1]
    known_image = Image(manifest_stat, layers, config, unref_image_stat)
    known_images.append(known_image)

  return known_images

def get_known_image_by_hash(image_hash, known_images):
  for image in known_images:
    if image_hash == image.manifest_stat.name:
      return image
  logging.debug("Couldn't find known image by hash:\n%s", image_hash)
  return None

def get_all_layers_and_configs(manifest_names):
  global HDFS_MANIFEST_DIR

  manifests_tuples = get_hdfs_manifests_from_paths([HDFS_MANIFEST_DIR + "/" + manifest_name for manifest_name in manifest_names])
  layers_and_configs = {}

  for manifest_tuple in manifests_tuples:
    manifest = manifest_tuple[0]
    manifest_hash = manifest_tuple[1]
    layers = []
    layers.extend(get_layer_hashes_from_manifest(manifest, False))
    config = get_config_hash_from_manifest(manifest)
    layers_and_configs[manifest_hash] = (layers, config)

    logging.debug("layers for %s:\n%s", manifest_hash, layers)
    logging.debug("config for %s:\n%s", manifest_hash, config)

  return layers_and_configs

def get_untagged_images(known_images, tagged_images):
  untagged_images = []
  for image in known_images:
    if is_image_untagged(image, tagged_images):
      untagged_images.append(image)

  logging.debug("known_images:\n%s", known_images)
  logging.debug("tagged_images:\n%s", tagged_images)
  logging.debug("untagged_images:\n%s", untagged_images)
  return untagged_images

def get_stale_images(untagged_images):
  stale_images = [image for image in untagged_images if is_image_stale(image)]
  logging.debug("stale_images:\n%s", stale_images)
  return stale_images

def get_dead_images(untagged_images):
  dead_images = [image for image in untagged_images if is_image_dead(image)]
  logging.debug("dead_images:\n%s", dead_images)
  return dead_images

def is_image_untagged(image, tagged_images):
  for tagged_image in tagged_images:
    if tagged_image.manifest_stat.name == image.manifest_stat.name:
      return False
  return True

def is_image_stale(image):
  return does_image_have_unref_file(image) and not does_image_have_dead_perms(image)

def is_image_dead(image):
  return does_image_have_unref_file(image) and does_image_have_dead_perms(image)

def does_image_have_unref_file(image):
  return image.unref_file_stat != None

def does_image_have_dead_perms(image):
  global DEAD_PERMS
  return image.manifest_stat.perms == DEAD_PERMS

def is_mod_time_old(mod_time):
  global UNTAGGED_TRANSITION_SEC

  cutoff_time = long(time.time() * 1000) - UNTAGGED_TRANSITION_SEC * 1000
  logging.debug("Mod time: %d, Cutoff time: %d", mod_time, cutoff_time)
  return mod_time < cutoff_time

def cleanup_handle_tagged_images(tagged_images):
  #Remove unreferenced files if they exist
  if not tagged_images:
    return

  unref_remove_list = []
  for image in tagged_images:
    if does_image_have_unref_file(image):
      unref_remove_list.append(image)

  remove_unref_files(unref_remove_list)

def cleanup_handle_untagged_images(untagged_images):
  #Create unreferenced file
  if not untagged_images:
    return

  touch_list = []
  for image in untagged_images:
    if not does_image_have_unref_file(image):
      touch_list.append(image)

  touch_unref_files(touch_list)

def cleanup_handle_stale_images(stale_images):
  #Set blob permissions to 400 for old stale images
  if not stale_images:
    return

  make_unreadable_list = []
  for image in stale_images:
    if is_mod_time_old(image.unref_file_stat.mod_time):
      make_unreadable_list.append(image)

  make_manifests_unreadable(make_unreadable_list)
  touch_unref_files(make_unreadable_list)

def cleanup_handle_dead_images(dead_images, known_images):
  #Remove old dead images
  if not dead_images:
    return

  images_to_remove  = []
  for image in dead_images:
    if is_mod_time_old(image.unref_file_stat.mod_time):
      images_to_remove.append(image)

  remove_dead_images(images_to_remove, known_images)

def make_manifests_unreadable(images):
  global HDFS_MANIFEST_DIR
  global DEAD_PERMS

  if not images:
    return

  manifest_file_paths = [HDFS_MANIFEST_DIR + "/" + image.unref_file_stat.name for image in images]
  logging.debug("Chmod %s manifest file:\n%s", DEAD_PERMS, manifest_file_paths)
  hdfs_chmod(DEAD_PERMS, manifest_file_paths)

def touch_unref_files(images):
  global HDFS_UNREF_DIR

  if not images:
    return

  unref_file_paths = [HDFS_UNREF_DIR + "/" + image.manifest_stat.name for image in images]
  logging.debug("Touching unref file:\n%s", unref_file_paths)
  hdfs_touchz(unref_file_paths)

def remove_unref_files(images):
  global HDFS_UNREF_DIR

  if not images:
    return

  unref_file_paths = [HDFS_UNREF_DIR + "/" + image.manifest_stat.name for image in images]
  logging.debug("Removing unref files:\n%s", unref_file_paths)
  hdfs_rm(unref_file_paths)

def remove_dead_images(images_to_remove, known_images):
  if not images_to_remove:
    return

  logging.debug("Removing dead images:\n%s", images_to_remove)
  delete_list = get_delete_list_from_images_to_remove(images_to_remove, known_images)
  if delete_list:
    hdfs_rm(delete_list)

def create_parsers():
  parser = argparse.ArgumentParser()
  add_parser_default_arguments(parser)

  subparsers = parser.add_subparsers(help='sub help', dest='sub_command')

  parse_pull_build_push_update = subparsers.add_parser('pull-build-push-update',
                                                       help='Pull an image, build its squashfs'
                                                       + ' layers, push it to hdfs, and'
                                                       + ' atomically update the'
                                                       + ' image-tag-to-hash file')
  parse_pull_build_push_update.set_defaults(func=pull_build_push_update)
  add_parser_default_arguments(parse_pull_build_push_update)
  parse_pull_build_push_update.add_argument("images_and_tags", nargs="+",
                                            help="Image and tag argument (can specify multiple)")

  parse_pull_build = subparsers.add_parser('pull-build',
                                           help='Pull an image and build its  squashfs layers')
  parse_pull_build .set_defaults(func=pull_build)
  add_parser_default_arguments(parse_pull_build)
  parse_pull_build.add_argument("images_and_tags", nargs="+",
                                help="Image and tag argument (can specify multiple)")

  parse_push_update = subparsers.add_parser('push-update',
                                            help='Push the squashfs layers to hdfs and update'
                                            + ' the image-tag-to-hash file')
  parse_push_update.set_defaults(func=push_update)
  add_parser_default_arguments(parse_push_update)
  parse_push_update.add_argument("images_and_tags", nargs="+",
                                 help="Image and tag argument (can specify multiple)")

  parse_remove_image = subparsers.add_parser('remove-image',
                                             help='Remove an image (manifest, config, layers)'
                                             + ' from hdfs based on its tag or manifest hash')
  parse_remove_image.set_defaults(func=remove_image)
  add_parser_default_arguments(parse_remove_image)
  parse_remove_image.add_argument("images_or_tags", nargs="+",
                                  help="Image or tag argument (can specify multiple)")

  parse_remove_tag = subparsers.add_parser('remove-tag',
                                           help='Remove an image to tag mapping in the'
                                           + ' image-tag-to-hash file')
  parse_remove_tag.set_defaults(func=add_remove_tag)
  add_parser_default_arguments(parse_remove_tag)
  parse_remove_tag.add_argument("images_and_tags", nargs="+",
                                help="Image and tag argument (can specify multiple)")

  parse_add_tag = subparsers.add_parser('add-tag',
                                        help='Add an image to tag mapping in the'
                                        + ' image-tag-to-hash file')
  parse_add_tag.set_defaults(func=add_remove_tag)
  add_parser_default_arguments(parse_add_tag)
  parse_add_tag.add_argument("images_and_tags", nargs="+",
                             help="Image and tag argument (can specify multiple)")

  parse_copy_update = subparsers.add_parser('copy-update',
                                            help='Copy an image from hdfs in one cluster to'
                                            + ' another and then update the'
                                            + ' image-tag-to-hash file')
  parse_copy_update.set_defaults(func=copy_update)
  add_parser_default_arguments(parse_copy_update)
  parse_copy_update.add_argument("src_root",
                                 help="HDFS path for source root directory")
  parse_copy_update.add_argument("dest_root",
                                 help="HDFS path for destination root directory")
  parse_copy_update.add_argument("images_and_tags", nargs="+",
                                 help="Image and tag argument (can specify multiple)")

  parse_query_tag = subparsers.add_parser('query-tag',
                                          help='Get the manifest, config, and layers'
                                          + ' associated with a tag')
  parse_query_tag.set_defaults(func=query_tag)
  add_parser_default_arguments(parse_query_tag)
  parse_query_tag.add_argument("tags", nargs="+",
                               help="Image or tag argument (can specify multiple)")

  parse_list_tags = subparsers.add_parser('list-tags',
                                          help='List all tags in image-tag-to-hash file')
  parse_list_tags.set_defaults(func=list_tags)
  add_parser_default_arguments(parse_list_tags)

  parse_bootstrap_setup = subparsers.add_parser('bootstrap',
                                          help='Bootstrap setup of required HDFS'
                                          + ' directories')
  parse_bootstrap_setup.set_defaults(func=bootstrap_setup)
  add_parser_default_arguments(parse_bootstrap_setup)

  parse_cleanup_untagged_images = subparsers.add_parser('cleanup',
                                                        help='Cleanup untagged images in HDFS')
  parse_cleanup_untagged_images.set_defaults(func=cleanup_untagged_images)
  add_parser_default_arguments(parse_cleanup_untagged_images)


  return parser

def add_parser_default_arguments(parser):
  parser.add_argument("--working-dir", type=str, dest='working_dir', default="dts-work-dir",
                      help="Name of working directory")
  parser.add_argument("--skopeo-format", type=str, dest='skopeo_format',
                      default='dir', help="Output format for skopeo copy")
  parser.add_argument("--pull-format", type=str, dest='pull_format',
                      default='docker', help="Pull format for skopeo")
  parser.add_argument("-l", "--log", type=str, dest='LOG_LEVEL',
                      default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
  parser.add_argument("--hdfs-root", type=str, dest='HDFS_ROOT',
                      default='/runc-root', help="The root directory in HDFS for all of the"
                      + "squashfs images")
  parser.add_argument("--image-tag-to-hash", type=str,
                      dest='image_tag_to_hash', default='image-tag-to-hash',
                      help="image-tag-to-hash filename in hdfs")
  parser.add_argument("-r", "--replication", type=int, dest='replication',
                      default=3, help="Replication factor for all files uploaded to HDFS")
  parser.add_argument("--hadoop-prefix", type=str, dest='hadoop_prefix',
                      default=os.environ.get('HADOOP_PREFIX'),
                      help="hadoop prefix value for environment")
  parser.add_argument("-f", "--force", dest='force',
                      action="store_true", default=False, help="Force overwrites in HDFS")
  parser.add_argument("--check-magic-file", dest='check_magic_file',
                      action="store_true", default=False, help="Check for a specific magic file"
                      + "in the image before uploading")
  parser.add_argument("--magic-file", type=str, dest='magic_file',
                      default='etc/dockerfile-version', help="The magic file to check for"
                      + "in the image")
  parser.add_argument("--max-layers", type=int, dest='MAX_IMAGE_LAYERS',
                      default=37, help="Maximum number of layers an image is allowed to have")
  parser.add_argument("--max-size", type=int, dest='MAX_IMAGE_SIZE',
                      default=10*1024*1024*1024, help="Maximum size an image is allowed to be")
  parser.add_argument("--untagged-transition-sec", type=long, dest='UNTAGGED_TRANSITION_SEC',
                      default=7*24*60*60, help="Time that untagged images will spend in each state"
                      + "before moving to the next one")
  parser.add_argument("--dead-perms", type=str, dest='DEAD_PERMS',
                      default="400", help="Permissions to set for manifests that are untagged "
                      + "before they are removed")
  parser.add_argument("-b", "--bootstrap", dest='bootstrap',
                      action="store_true", default=False, help="Bootstrap setup"
                      + " of required HDFS directories")
  return parser

def check_dependencies():
  global HADOOP_BIN_DIR

  try:
    command = [HADOOP_BIN_DIR + "/hadoop", "-h"]
    shell_command(command, False, False, True)
  except:
    logging.error("Could not find hadoop. Make sure HADOOP_PREFIX " +
                  "is set correctly either in your environment or on the command line " +
                  "via --hadoop-prefix")
    return 1

  try:
    command = ["skopeo", "-v"]
    shell_command(command, False, False, True)
  except:
    logging.error("Could not find skopeo. Make sure it is installed and present " +
                  "on the PATH")
    return 1

  try:
    command = ["mksquashfs", "-version"]
    shell_command(command, False, False, True)
  except:
    logging.error("Could not find mksquashfs. Make sure squashfs-tools is installed " +
                  "and mksquashfs is present on the the PATH")
    return 1

  try:
    command = ["tar", "--version"]
    shell_command(command, False, False, True)
  except:
    logging.error("Could not find tar. Make sure it is installed and present " +
                  "on the PATH")
    return 1

  try:
    command = ["setfattr", "--version"]
    shell_command(command, False, False, True)
  except:
    logging.error("Could not find setfattr . Make sure it is installed and present " +
                  "on the PATH")
    return 1

  return 0

def main():
  global LOG_LEVEL
  global HADOOP_PREFIX
  global HADOOP_BIN_DIR
  global HDFS_ROOT
  global HDFS_MANIFEST_DIR
  global HDFS_CONFIG_DIR
  global HDFS_LAYERS_DIR
  global HDFS_UNREF_DIR
  global MAX_IMAGE_LAYERS
  global MAX_IMAGE_SIZE
  global UNTAGGED_TRANSITION_SEC
  global ARG_MAX
  global DEAD_PERMS

  if os.geteuid() != 0:
    logging.error("Script must be run as root")
    return

  parser = create_parsers()
  args, extra = parser.parse_known_args()

  if extra:
    raise Exception("Extra unknown arguments given: %s" % (extra))

  ARG_MAX = os.sysconf("SC_ARG_MAX")
  HDFS_ROOT = args.HDFS_ROOT
  HDFS_MANIFEST_DIR = HDFS_ROOT + "/manifests"
  HDFS_CONFIG_DIR = HDFS_ROOT + "/config"
  HDFS_LAYERS_DIR = HDFS_ROOT + "/layers"
  HDFS_UNREF_DIR = HDFS_ROOT + "/unreferenced"
  MAX_IMAGE_LAYERS = args.MAX_IMAGE_LAYERS
  MAX_IMAGE_SIZE = args.MAX_IMAGE_SIZE
  UNTAGGED_TRANSITION_SEC = args.UNTAGGED_TRANSITION_SEC
  DEAD_PERMS = args.DEAD_PERMS
  LOG_LEVEL = args.LOG_LEVEL.upper()
  image_tag_to_hash = args.image_tag_to_hash

  numeric_level = getattr(logging, LOG_LEVEL, None)
  if not isinstance(numeric_level, int):
    logging.error("Invalid log level: %s", LOG_LEVEL)
    return
  logging.basicConfig(format="%(levelname)s: %(message)s", level=numeric_level)

  if args.hadoop_prefix is None:
    logging.error("Hadoop prefix is not set. You may set it either " +
                  "in your environment or via --hadoop-prefix")
    return

  HADOOP_PREFIX = args.hadoop_prefix
  HADOOP_BIN_DIR = HADOOP_PREFIX + "/bin"

  if check_dependencies():
    return

  if "/" in image_tag_to_hash:
    logging.error("image-tag-to-hash cannot contain a /")
    return

  logging.debug("args: %s", str(args))
  logging.debug("extra: %s", str(extra))
  logging.debug("image-tag-to-hash: %s", image_tag_to_hash)
  logging.debug("LOG_LEVEL: %s", LOG_LEVEL)
  logging.debug("HADOOP_BIN_DIR: %s", str(HADOOP_BIN_DIR))

  args.func(args)

if __name__ == "__main__":
  main()

