rm -rf gen-javabean gen-py py java
rm -rf jvm/backtype/storm/generated
thrift --gen java:beans,hashcode,nocamel --gen py:utf8strings storm.thrift
mv gen-javabean/ java/
mv gen-py py
