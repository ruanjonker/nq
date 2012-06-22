-define(info(X), error_logger:info_report({?MODULE, X})).
-define(warn(X), error_logger:warning_report({?MODULE, X})).

-define(dbset(D,K,V), nq_util:db_set(D,K,V)).
-define(dbget(D,K),   nq_util:db_get(D,K)).
-define(dbdel(D,K), nq_util:db_del(D,K)).

%EOF
