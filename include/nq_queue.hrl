
-record(state, {tref, qname, size = 0, meta_dirty = false, rfrag_idx = 0, rfrag_recno = 0, rfrag_cache_size = 0, rfrag_cache = [], rfrag_dirty = false, wfrag_cache = [], wfrag_dirty = false, wfrag_cache_size = 0, wfrag_idx = 0, last_sync = now(), subs_dict = dict:new(), storage_mod = nq_file, storage_mod_params = "./nqdata/", max_frag_size = 64000, auto_sync = true, last_broadcast = now()}).

