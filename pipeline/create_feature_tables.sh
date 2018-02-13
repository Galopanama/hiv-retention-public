#!/bin/bash
# TODO: need to change permissions on this

./create_acs_tables.sh

for f in `ls features/*sql`; do
    echo $f
    psql -f $f
done
