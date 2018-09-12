# Shared volume
This directory is mounted as a shared volume between local disk and the hive server docker container. It's used to 
allow the container to access test data and the SerDe jar

**WARNING**: this folder should **only** be manipulated by the integration test suite. Do **not** add files here. The 
generated directories are added to `.gitignore` to avoid adding them to source control 
 