#!/bin/bash
#
# Deploy artifacts to Looker's maven repo.
#
# Depends upon a private repository; do not merge to Apache master.
#
# The '-DpomFile' line originally used a copy of pom.xml in /tmp; hopefully
# it still works when the pom.xml is edited in place.

for module in core babel; do
  mvn deploy:deploy-file \
    -Dfile=$HOME/.m2/repository/org/apache/calcite/calcite-${module}/1.20.0-SNAPSHOT/calcite-${module}-1.20.0-SNAPSHOT.jar \
    -DgeneratePom=false \
    -DpomFile=${module}/pom.xml \
    -DrepositoryId=nexus \
    -Durl=https://nexusrepo.looker.com/repository/vendored-artifacts/
done

# End deploy-looker.sh
