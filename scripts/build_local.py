# Requires protoc 3.19.04
# https://github.com/protocolbuffers/protobuf/releases/tag/v3.19.4

import os
import shutil
from os import walk

# Change to substrait folder
sub_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','third_party','substrait')
# Delete Current CPP files
shutil.rmtree(os.path.join(sub_folder,'substrait'))

git_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)),'..','substrait')

proto_folder = os.path.join(git_folder,'proto')
substrait_proto_folder = os.path.join(proto_folder,'substrait')
substrait_extensions_proto_folder = os.path.join(substrait_proto_folder,'extensions')

# Generate Proto Files on a specific git tag
os.chdir(sub_folder)

os.mkdir("substrait")
os.mkdir("substrait/extensions")

# Generate all files
proto_sub_list = next(walk(substrait_proto_folder), (None, None, []))[2]

proto_sub_extensions = next(walk(substrait_extensions_proto_folder), (None, None, []))[2]

# /usr/local/bin/protoc
print("Protoc version" + os.popen('protoc --version').read())

for proto in proto_sub_list:
    os.system("/usr/local/bin/protoc -I="+ proto_folder+ " --cpp_out="+sub_folder +" "+ os.path.join(substrait_proto_folder,proto))

for proto in proto_sub_extensions:
    os.system("/usr/local/bin/protoc -I="+ proto_folder+ " --cpp_out="+sub_folder +" "+ os.path.join(substrait_extensions_proto_folder,proto))