ANCESTRY_REFS_METADATA_KEY = "ops:Provenance/ops:ancestor_refs"

# this placeholder exists to provide interpolation of the key without needing to escape special characters in the painless script
KEY_PLACEHOLDER = "ANCESTRY_REFS_METADATA_KEY_PLACEHOLDER"

# The following is a minified painless script to deduplicate ancestry elements at update-time
# Because AOSS does not support named/stored scripts, it is necessary to inline the script within each update
# The script is equivalent to the following unminified version:
#
# """
# boolean changed = false;
# if (ctx._source['ANCESTRY_REFS_METADATA_KEY_PLACEHOLDER'] == null) {
#     ctx._source['ANCESTRY_REFS_METADATA_KEY_PLACEHOLDER'] = [];
#     changed = true;
# }
#
# def existing = new HashSet();
# for (item in ctx._source['ANCESTRY_REFS_METADATA_KEY_PLACEHOLDER']) {
#     existing.add(item);
# }
#
# for (item in params.new_items) {
#     if (!existing.contains(item)) {
#       ctx._source['ANCESTRY_REFS_METADATA_KEY_PLACEHOLDER'].add(item);
#       changed = true;
#     }
# }
#
# if (!changed) {
#     ctx.op = 'none';  // <— Prevents reindexing if nothing changed
# }

ANCESTRY_DEDUPLICATION_SCRIPT_MINIFIED = "boolean c=false;if(ctx._source[\'ANCESTRY_REFS_METADATA_KEY_PLACEHOLDER\']==null){ctx._source[\'ANCESTRY_REFS_METADATA_KEY_PLACEHOLDER\']=[];c=true;}def e=new HashSet();for(i in ctx._source[\'ANCESTRY_REFS_METADATA_KEY_PLACEHOLDER\']){e.add(i);}for(i in params.new_items){if(!e.contains(i)){ctx._source[\'ANCESTRY_REFS_METADATA_KEY_PLACEHOLDER\'].add(i);c=true;}}if(!c){ctx.op=\'none\';}" \
    .replace(KEY_PLACEHOLDER, ANCESTRY_REFS_METADATA_KEY)
