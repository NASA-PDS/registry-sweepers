ANCESTRY_REFS_METADATA_KEY = "ops:Provenance/ops:ancestor_refs"

# The following is a minified painless script to deduplicate ancestry elements at update-time
# Because AOSS does not support named/stored scripts, it is necessary to inline the script within each update
# The script is equivalent to the following unminified version:
#
# """
# boolean changed = false;
# if (ctx._source['ops:Provenance/ops:ancestor_refs'] == null) {
#     ctx._source['ops:Provenance/ops:ancestor_refs'] = [];
#     changed = true;
# }
#
# def existing = new HashSet();
# for (item in ctx._source['ops:Provenance/ops:ancestor_refs']) {
#     existing.add(item);
# }
#
# for (item in params.new_items) {
#     if (!existing.contains(item)) {
#       ctx._source['ops:Provenance/ops:ancestor_refs'].add(item);
#       changed = true;
#     }
# }
#
# if (!changed) {
#     ctx.op = 'none';  // <— Prevents reindexing if nothing changed
# }

ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED = "boolean c=false;if(ctx._source[\'ops:Provenance/ops:ancestor_refs\']==null){ctx._source[\'ops:Provenance/ops:ancestor_refs\']=[];c=true;}def e=new HashSet();for(i in ctx._source[\'ops:Provenance/ops:ancestor_refs\']){e.add(i);}for(i in params.new_items){if(!e.contains(i)){ctx._source[\'ops:Provenance/ops:ancestor_refs\'].add(i);c=true;}}if(!c){ctx.op=\'none\';}"
