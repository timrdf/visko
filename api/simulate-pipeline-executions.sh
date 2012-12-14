#!/bin/bash
#
#3> <> prov:wasAttributedTo <http://tw.rpi.edu/instances/TimLebo> .

if [ -e "$1" ]; then
   vkq=$1
   name=`basename $vkq`
   echo $name 
   rm -rf output/$name/*; 
   mkdir output/$name &> /dev/null; 
   ant -Dp=`pwd`/output/$name -Dq=$vkq -Dm=3 execute-query
else
   # Run all of the queries.
   for vkq in queries/*.vkq; do name=`basename $vkq`; echo $name; rm -rf output/$name/*; mkdir output/$name &> /dev/null; ant -Dp=`pwd`/output/$name -Dq=$vkq -Dm=3 execute-query; done
fi

# Make them all Turtle.
for owl in `find output -name "*.owl"`; do rapper -q -g -o turtle $owl > $owl.ttl; rm $owl; done

# remove base filepaths
# e.g. file:///Users/lebot/afrl/phd/visko/github/visko/api/output/2dpoints-gravity.vkq
# from all turtle
# e.g. output/2dpoints-gravity.vkq/visko-pipeline-provenance-0226480254745262.owl.ttl
for rdf in `find output -name "*.ttl"`; do dir=`dirname $rdf`; base=file://`pwd`/$dir/; perl -pi -e "s|$base||g" $rdf; done

say done


# cat visko-* > pml.ttl; rapper -g -o rdfxml pml.ttl > pml.ttl.rdf; vsr2grf.sh rdf graffle -w pml.ttl.rdf
# rapper -g -o rdfxml prov.ttl > prov.ttl.rdf; vsr2grf.sh rdf graffle -w prov.ttl.rdf
