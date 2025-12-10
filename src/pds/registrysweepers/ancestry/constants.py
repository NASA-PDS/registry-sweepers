ANCESTRY_REFS_METADATA_KEY = "ops:Provenance/ops:ancestor_refs"

# The following is a minified painless script to deduplicate ancestry elements at update-time
# Because AOSS does not support named/stored scripts, it is necessary to inline the script within each update
# The script is equivalent to the following unminified version:
#
# """
# boolean changed = false;
# if (ctx._source['ancestry'] == null) {
#     ctx._source['ancestry'] = [];
#     changed = true;
# }
#
# def existing = new HashSet();
# for (item in ctx._source['ancestry']) {
#     existing.add(item.lid + '::' + item.vid);
# }
#
# for (item in params.new_items) {
# def key = item.lid + '::' + item.vid;
#     if (!existing.contains(key)) {
#       ctx._source['ancestry'].add(item);
#       changed = true;
#     }
# }
#
# if (!changed) {
#     ctx.op = 'none';  // <— Prevents reindexing if nothing changed
# }"""
ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED = "boolean c=false;if(ctx._source[\'ancestry\']==null){ctx._source[\'ancestry\']=[];c=true;}def e=new HashSet();for(i in ctx._source[\'ancestry\']){e.add(i.lid+\'::\'+i.vid);}for(i in params.new_items){def k=i.lid+\'::\'+i.vid;if(!e.contains(k)){ctx._source[\'ancestry\'].add(i);c=true;}}if(!c){ctx.op=\'none\';}"
