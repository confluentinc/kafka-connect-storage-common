# Purpose of this module

This module serves to replace the DOS-vulnerable Protobuf 3.7 contained within the package hadoop-shaded-protobuf_3_7 which is included by the package hadoop-common until such time that the the repo upgrades their protobuf version (at which time this package should be removable).

You can chack the current state of the protobuf package in question in its repo:
* https://github.com/apache/hadoop-thirdparty