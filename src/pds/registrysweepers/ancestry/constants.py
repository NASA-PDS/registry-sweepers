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
#     existing.add(item);
# }
#
# for (item in params.new_items) {
#     if (!existing.contains(item)) {
#       ctx._source['ancestry'].add(item);
#       changed = true;
#     }
# }
#
# if (!changed) {
#     ctx.op = 'none';  // <— Prevents reindexing if nothing changed
# }

for (item in params.new_items) {
    if (!existing.contains(item)) {
      ctx._source['ancestry'].add(item);
      changed = true;
    }
}

if (!changed) {
    ctx.op = 'none';  // <— Prevents reindexing if nothing changed
}"""